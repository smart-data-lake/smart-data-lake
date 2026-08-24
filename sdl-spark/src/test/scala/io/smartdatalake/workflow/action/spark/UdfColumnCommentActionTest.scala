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
package io.smartdatalake.workflow.action.spark

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.util.spark.{SparkSchemaUtil, TestUdfGeo, TestUdfTag}
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfTransformer
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test that column comments are derived from the ScalaDoc of a case class returned by a user defined function.
 */
class UdfColumnCommentActionTest extends AnyFunSuite with BeforeAndAfter {

  implicit val session: SparkSession = SparkTestUtil.session
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  import session.implicits._

  before {
    instanceRegistry.clear()
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
  }

  private def comments(df: DataFrame): Map[String, String] =
    SparkSchemaUtil.columnsComments(df.schema).map { case (path, comment) => path.mkString(".") -> comment }

  test("action output gets column comments from the ScalaDoc of the case class returned by a udf") {
    val srcDO = MockSparkDataObject("src1").register
    val tgtDO = MockSparkDataObject("tgt1").register
    srcDO.writeSparkDataFrame(Seq("Bern", "Zurich").toDF("city"), Seq(), isRecursiveInput = false, None)(contextExec)

    val transformer = ScalaClassSparkDfTransformer(className = classOf[TestGeoUdfTransformer].getName)
    val action = CopyAction("udfComments", srcDO.id, tgtDO.id, transformers = Seq(transformer))

    // the init phase propagates the schema of the output, including the column comments, to the output DataObject
    action.init(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextInit)

    val tgtDf = tgtDO.getSparkDataFrame()(contextInit)
    assert(comments(tgtDf) == Map(
      "geo" -> "A geo location enriched from an address.",
      "geo.lat" -> "Latitude in decimal degrees, WGS84.",
      "geo.lon" -> "Longitude in decimal degrees, WGS84.",
      "geo.tags" -> "Free-form tags attached to the location.",
      "geo.tags.key" -> "The tag key.",
      "geo.tags.value" -> "The tag value."
    ))

    // the data written in the exec phase must be unchanged
    action.exec(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextExec)
    assert(tgtDO.getSparkDataFrame()(contextExec).select("city").as[String].collect().toSet == Set("Bern", "Zurich"))
  }

  test("action output gets column comments from the ScalaDoc of a typed Dataset return value") {
    val srcDO = MockSparkDataObject("src1").register
    val tgtDO = MockSparkDataObject("tgt1").register
    srcDO.writeSparkDataFrame(Seq(("Bern", 1)).toDF("name", "population"), Seq(), isRecursiveInput = false, None)(contextExec)

    val transformer = ScalaClassSparkDfTransformer(className = classOf[TestTypedDatasetTransformer].getName)
    val action = CopyAction("dsComments", srcDO.id, tgtDO.id, transformers = Seq(transformer))
    action.init(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextInit)

    // the columns are documented from the ScalaDoc of the Dataset type in the transform method signature
    val tgtDf = tgtDO.getSparkDataFrame()(contextInit)
    assert(comments(tgtDf) == Map(
      "name" -> "Name of the city.",
      "population" -> "Number of inhabitants."
    ))

    action.exec(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextExec)
    assert(tgtDO.getSparkDataFrame()(contextExec).select("name").as[String].collect().toSet == Set("Bern"))
  }
}

/**
 * A city.
 *
 * @param name       Name of the city.
 * @param population Number of inhabitants.
 */
case class TestCity(name: String, population: Int)

/**
 * Declares a typed Dataset return value, so its case class ScalaDoc documents the output columns.
 */
class TestTypedDatasetTransformer extends CustomDfTransformer {
  def transform(ds: Dataset[TestCity]): Dataset[TestCity] = {
    import ds.sparkSession.implicits._
    ds.map(city => city.copy(population = city.population + 1))
  }
}

/**
 * Adds a geo location computed by a user defined function returning a case class.
 */
class TestGeoUdfTransformer extends CustomDfTransformer {
  override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    val geoUdf = udf((city: String) => TestUdfGeo(1.0, 2.0, Seq(TestUdfTag("key1", "value1"))))
    df.withColumn("geo", geoUdf(col("city")))
  }
}
