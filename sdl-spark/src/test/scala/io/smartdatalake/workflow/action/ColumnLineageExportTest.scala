/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package io.smartdatalake.workflow.action

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.ColumnLineageExportBehaviour
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.workflow.action.generic.transformer.{SQLDfTransformer, SQLDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfTransformer
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.ColumnTransformation.{Identity, Transformation}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Test the column level lineage collected in the init phase of a "--test dry-run-with-lineage-export" run,
 * and its export, see issue #867.
 *
 * The engine independent cases are covered by [[ColumnLineageExportBehaviour]], which the plain-Scala engine
 * runs as well. Tested here are the transformers only available for Spark, and the effect of Sparks analyzer
 * on an Action reading the same columns twice.
 */
class ColumnLineageExportTest extends AnyFunSuite with ColumnLineageExportBehaviour {

  override def subFeedType: Type = typeOf[SparkSubFeed]

  protected implicit val session: SparkSession = SparkTestUtil.session

  // the DataFrames of the tests are built engine independently, as the mock DataObjects take a GenericDataFrame
  import helper.implicits._

  override def defaultEngineConnection: Connection with EngineConnection = SparkTestUtil.defaultSparkConnection

  override def createDataObject(name: String)(implicit instanceRegistry: InstanceRegistry): DataObject with CanCreateDataFrame with CanWriteDataFrame =
    MockSparkDataObject(name).register

  override def defaultActionPipelineContext(implicit instanceRegistry: InstanceRegistry): ActionPipelineContext =
    SparkTestUtil.getDefaultActionPipelineContext

  override def inputSubFeed(dataObjectId: DataObjectId): DataFrameSubFeed = SparkSubFeed(None, dataObjectId, Seq())

  test("the column lineage of an Action is collected in the init phase") {
    testTheColumnLineageOfAnActionIsCollectedInTheInitPhase()
  }

  test("the column lineage of an Action with two inputs keeps both inputs apart") {
    testTheColumnLineageOfAnActionWithTwoInputsKeepsBothInputsApart()
  }

  test("the collected column lineage is exported as OpenLineage column lineage facet") {
    testTheCollectedColumnLineageIsExportedAsOpenLineageColumnLineageFacet()
  }

  test("the lineage of chained Actions stops at the intermediate DataObject although the DataFrame is cached") {
    testTheLineageOfChainedActionsStopsAtTheIntermediateDataObject()
  }

  test("no column lineage is collected without the lineage export test mode") {
    testNoColumnLineageIsCollectedWithoutTheLineageExportTestMode()
  }

  test("the column lineage of an Action using a SQL transformer is collected") {
    withLineageExport { (instanceRegistry, contextExec, contextInitExport, _) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val srcDO = createDataObject("src1")
      srcDO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val tgtDO = createDataObject("tgt1")
      val action = CopyAction("copyCities", srcDO.id, tgtDO.id,
        transformers = Seq(SQLDfTransformer(code = Some("select name, upper(country) as country, 'x' as constant from src1")))
      )
      instanceRegistry.register(action)

      action.init(Seq(inputSubFeed(srcDO.id)))(contextInitExport)

      val lineage = contextInitExport.columnLineageExportRegistry.getColumnLineages(tgtDO.id).lineage
      assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
      assert(inputsOf(lineage, "country") == Seq(("src1", "country", Transformation)))
      // the SQL expression creating the column is exported as description of the transformation. Reading
      // through a temporary view, the column of the expression is qualified with the name of the view.
      val description = lineage.get("country").get.inputFields.head.transformation.description
      assert(description.exists(d => d.toLowerCase.startsWith("upper(") && d.contains("country")), description)
      assert(lineage.get("constant").exists(_.inputFields.isEmpty))
      assert(lineage.unresolvedColumns.isEmpty)
    }
  }

  test("reading a cached output and its own input in one Action reports shared columns for both inputs") {
    withLineageExport { (instanceRegistry, contextExec, contextInitExport, _) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val srcDO = createDataObject("src1")
      srcDO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val interDO = createDataObject("inter1")
      val tgtDO = createDataObject("tgt1")
      // action1 passes its input through unchanged and caches it, so inter1 has the columns of src1
      val action1 = CopyAction("a1", srcDO.id, interDO.id, cacheOutput = true)
      val action2 = CustomDataFrameAction("a2", Seq(interDO.id, srcDO.id), Seq(tgtDO.id),
        transformers = Seq(SQLDfsTransformer(code = Map(tgtDO.id.id ->
          "select i.name as fromInter, s.country as fromSrc from inter1 i join src1 s on i.name = s.name"
        )))
      )
      instanceRegistry.register(action1)
      instanceRegistry.register(action2)

      val interSubFeeds = action1.init(Seq(inputSubFeed(srcDO.id)))(contextInitExport)
      action2.init(interSubFeeds :+ inputSubFeed(srcDO.id))(contextInitExport)

      val lineage = contextInitExport.columnLineageExportRegistry.getColumnLineages(tgtDO.id).lineage
      // the column belongs to both inputs, as inter1 is a copy of src1 which was not materialized in between
      assert(inputsOf(lineage, "fromInter") == Seq(("inter1", "name", Identity), ("src1", "name", Identity)))
      // Spark's analyzer replaces the expression ids of one side of a join reading the same columns twice, so
      // the columns of that side can not be traced back. They are reported as unresolved and not as a wrong source.
      assert(lineage.unresolvedColumns == Seq("fromSrc"))
    }
  }

  test("the column lineage of an Action using a user defined function is collected") {
    withLineageExport { (instanceRegistry, contextExec, contextInitExport, _) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val srcDO = createDataObject("src1")
      srcDO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val tgtDO = createDataObject("tgt1")
      val action = CopyAction("udfLineage", srcDO.id, tgtDO.id,
        transformers = Seq(ScalaClassSparkDfTransformer(className = classOf[ColumnLineageTestGeoUdfTransformer].getName))
      )
      instanceRegistry.register(action)

      action.init(Seq(inputSubFeed(srcDO.id)))(contextInitExport)

      val lineage = contextInitExport.columnLineageExportRegistry.getColumnLineages(tgtDO.id).lineage
      // the Action adds the column comments from the ScalaDoc of the case class returned by the UDF before the
      // lineage is collected, see issue #765. This rewrites the output attributes of the plan.
      assert(inputsOf(lineage, "geo") == Seq(("src1", "name", Transformation)))
      assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
      assert(lineage.unresolvedColumns.isEmpty)
    }
  }
}

/**
 * A geo location enriched from a city name.
 *
 * @param lat Latitude in decimal degrees, WGS84.
 * @param lon Longitude in decimal degrees, WGS84.
 */
case class ColumnLineageTestGeo(lat: Double, lon: Double)

/**
 * Adds a geo location computed by a user defined function returning a case class.
 */
class ColumnLineageTestGeoUdfTransformer extends CustomDfTransformer {
  override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    val geoUdf = udf((city: String) => ColumnLineageTestGeo(1.0, 2.0))
    df.withColumn("geo", geoUdf(col("name")))
  }
}
