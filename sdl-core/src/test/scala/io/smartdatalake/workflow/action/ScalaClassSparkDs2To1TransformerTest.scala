/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.testutils.TestUtil._
import io.smartdatalake.workflow.action.spark.customlogic.CustomDs2to1Transformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDs2To1Transformer
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.CsvFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files

case class AnotherInputDataSet(name: String, rating: Int)

case class AnotherOutputDataSet(concatenated_name: String, added_rating: Int)

class TestDS2To1Transformer extends CustomDs2to1Transformer[AnotherInputDataSet, AnotherInputDataSet, AnotherOutputDataSet] {

  override def transform(session: SparkSession, options: Map[String, String], ds1: Dataset[AnotherInputDataSet], ds2: Dataset[AnotherInputDataSet]): Dataset[AnotherOutputDataSet] = {
    import session.implicits._

    val crossJoined = ds1.as("ds1").crossJoin(ds2.as("ds2"))
    val result = crossJoined.withColumn("added_rating", $"ds1.rating" + $"ds2.rating")
      .withColumn("concatenated_name", concat($"ds1.name", $"ds2.name"))
      .select("concatenated_name", "added_rating")
      .as[AnotherOutputDataSet]
    result
  }
}

class ScalaClassSparkDs2To1TransformerTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog

  import sessionHiveCatalog.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextExec: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("One DS2To1 Transformation (direct call to exec)") {
    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1", tempPath + "/src1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO1)
    val srcDO2 = CsvFileDataObject("src2", tempPath + "/src2", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO2)
    val tgt1DO = CsvFileDataObject("tgt1", tempPath + "/tgt1", partitions = Seq()
      , schema = Some(SparkSchema(StructType.fromDDL("concatenated_name string, added_rating int"))))
    instanceRegistry.register(tgt1DO)

    // fill src with first files
    val dfSrc1 = Seq(("john", 5))
      .toDF("name", "rating")
    srcDO1.writeSparkDataFrame(dfSrc1, Seq())
    val dfSrc2 = Seq(("doe", 10))
      .toDF("name", "rating")
    srcDO2.writeSparkDataFrame(dfSrc2, Seq())


    // prepare & start load
    val customTransformerConfig = ScalaClassSparkDs2To1Transformer(className = classOf[TestDS2To1Transformer].getName)
    val testAction = CustomDataFrameAction("action", List(srcDO1.id, srcDO2.id), List(tgt1DO.id), transformers = Seq(customTransformerConfig))

    instanceRegistry.register(srcDO1)
    instanceRegistry.register(srcDO2)
    instanceRegistry.register(tgt1DO)
    instanceRegistry.register(testAction)

    val srcSubFeed1 = SparkSubFeed(None, "src1", partitionValues = Seq())
    val srcSubFeed2 = SparkSubFeed(None, "src2", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed1, srcSubFeed2))

    val actual = tgt1DO.getSparkDataFrame().as[AnotherOutputDataSet].head()
    assert(actual.added_rating == 15)
    assert(actual.concatenated_name == "johndoe")
  }

  test("One DS2To1 Transformation using config file") {

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1", tempPath + "/src1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO1)
    val srcDO2 = CsvFileDataObject("src2", tempPath + "/src2", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO2)

    // fill src with first files
    val dfSrc1 = Seq(("john", 5))
      .toDF("name", "rating")
    srcDO1.writeSparkDataFrame(dfSrc1, Seq())
    val dfSrc2 = Seq(("doe", 10))
      .toDF("name", "rating")
    srcDO2.writeSparkDataFrame(dfSrc2, Seq())

    val sdlb = new DefaultSmartDataLakeBuilder()
    // setup input data
    val srcDO = CsvFileDataObject("src1DS", "target/src1DS", partitions = Seq(),
      schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "test_feed_name", configuration = Some(Seq(
      getClass.getResource("/configScalaClassSparkDs2to1Transformer/application.conf").getPath))
    )
    //Run SDLB
    sdlb.run(sdlConfig)

    val tgt1DO = CsvFileDataObject("tgt1", "target/tgt1", partitions = Seq()
      , schema = Some(SparkSchema(StructType.fromDDL("concatenated_name string, added_rating int"))))
    instanceRegistry.register(tgt1DO)
    val actual = tgt1DO.getSparkDataFrame().as[AnotherOutputDataSet].head()
    assert(actual.added_rating == 15)
    assert(actual.concatenated_name == "johndoe")
  }
}