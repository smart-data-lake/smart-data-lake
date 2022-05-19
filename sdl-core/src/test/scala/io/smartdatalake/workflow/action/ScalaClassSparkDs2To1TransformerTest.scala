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
import io.smartdatalake.workflow.action.spark.customlogic.CustomDsNto1Transformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDs2To1Transformer
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.CsvFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import java.nio.file.Files
import scala.reflect.io.Directory

case class NameRating(name: String, rating: Int)

case class RatingName(rating: Int, name: String)

case class AnotherOutputDataSet(concatenated_name: String, added_rating: Int)

class TestResolutionByIdDS2To1Transformer extends CustomDsNto1Transformer{

  def transform(session: SparkSession, options: Map[String, String], src1DS: Dataset[NameRating], src2DS: Dataset[NameRating]): Dataset[AnotherOutputDataSet] = {
    import session.implicits._

    val crossJoined = src1DS.as("ds1").crossJoin(src2DS.as("ds2"))
    val result = crossJoined.withColumn("added_rating", $"ds1.rating" + $"ds2.rating")
      .withColumn("concatenated_name", concat($"ds1.name", $"ds2.name"))
      .select("concatenated_name", "added_rating")
      .as[AnotherOutputDataSet]
    result
  }

  //This method is only here to demonstrate that it still works even if the user defined another method that is called transform (this method is ignored by SDLB)
  def transformTest(session: SparkSession, src1DS: Dataset[NameRating], src2DS: Dataset[NameRating]): Dataset[AnotherOutputDataSet] = {
    throw new IllegalArgumentException
  }
}

class TestResolutionByOrderingDS2To1Transformer extends CustomDsNto1Transformer {

  def transform(session: SparkSession, options: Map[String, String], firstDataset: Dataset[RatingName], secondDataset: Dataset[NameRating]): Dataset[AnotherOutputDataSet] = {
    import session.implicits._

    val crossJoined = firstDataset.as("ds1").crossJoin(secondDataset.as("ds2"))
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

  private val tempDir = Files.createTempDirectory("testScalaClassSparkDs2To1TransformerTest")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextExec: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("One DS2To1 Transformation (direct call to exec)") {
    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1DS", tempPath + "/src1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO1)
    val srcDO2 = CsvFileDataObject("src2DS", tempPath + "/src2", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO2)
    val tgt1DO = CsvFileDataObject("tgt1DS", tempPath + "/tgt1", partitions = Seq()
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
    val customTransformerConfig = ScalaClassSparkDs2To1Transformer(className = classOf[TestResolutionByIdDS2To1Transformer].getName)
    val testAction = CustomDataFrameAction("action", List(srcDO1.id, srcDO2.id), List(tgt1DO.id), transformers = Seq(customTransformerConfig))

    instanceRegistry.register(srcDO1)
    instanceRegistry.register(srcDO2)
    instanceRegistry.register(tgt1DO)
    instanceRegistry.register(testAction)

    val srcSubFeed1 = SparkSubFeed(None, "src1DS", partitionValues = Seq())
    val srcSubFeed2 = SparkSubFeed(None, "src2DS", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed1, srcSubFeed2))

    val actual = tgt1DO.getSparkDataFrame().as[AnotherOutputDataSet].head()
    assert(actual.added_rating == 15)
    assert(actual.concatenated_name == "johndoe")
  }

  test("One DS2To1 Transformation using config file: two identical input types using dataObjectId") {

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1", "target/src1DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO1)
    val srcDO2 = CsvFileDataObject("src2", "target/src2DS2to1", partitions = Seq("name")
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

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "test_feed_name", configuration = Some(Seq(
      getClass.getResource("/configScalaClassSparkDs2to1Transformer/usingDataObjectId.conf").getPath))
    )
    //Run SDLB
    sdlb.run(sdlConfig)

    val tgt1DO = CsvFileDataObject("tgt1", "target/tgt1DS2to1", partitions = Seq()
      , schema = Some(SparkSchema(StructType.fromDDL("concatenated_name string, added_rating int"))))
    instanceRegistry.register(tgt1DO)
    val actual = tgt1DO.getSparkDataFrame().as[AnotherOutputDataSet].head()
    assert(actual.added_rating == 15)
    assert(actual.concatenated_name == "johndoe")

    //cleanup
    val directoriesToDelete = {
      List(
        new Directory(new File("target/src1DS2to1")),
        new Directory(new File("target/src2DS2to1")),
        new Directory(new File("target/tgt1DS2to1")),
      )
    }
    directoriesToDelete.foreach(dir => dir.deleteRecursively())
  }

    test("One DS2To1 Transformation using config file: two different input types using dataObjectOrdering") {

      // setup DataObjects
      // source has partition columns dt and type
      val srcDO1 = CsvFileDataObject("src1", "target/src1DS2to1", partitions = Seq("name")
        , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
      instanceRegistry.register(srcDO1)
      val srcDO2 = CsvFileDataObject("src2", "target/src2DS2to1", partitions = Seq("name")
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

      val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "test_feed_name", configuration = Some(Seq(
        getClass.getResource("/configScalaClassSparkDs2to1Transformer/usingDataObjectOrdering.conf").getPath))
      )
      //Run SDLB
      sdlb.run(sdlConfig)

      val tgt1DO = CsvFileDataObject("tgt1", "target/tgt1DS2to1", partitions = Seq()
        , schema = Some(SparkSchema(StructType.fromDDL("concatenated_name string, added_rating int"))))
      instanceRegistry.register(tgt1DO)
      val actual = tgt1DO.getSparkDataFrame().as[AnotherOutputDataSet].head()
      assert(actual.added_rating == 15)
      assert(actual.concatenated_name == "johndoe")

      //cleanup
      val directoriesToDelete = {
        List(
          new Directory(new File("target/src1DS2to1")),
          new Directory(new File("target/src2DS2to1")),
          new Directory(new File("target/tgt1DS2to1")),
        )
      }
      directoriesToDelete.foreach(dir => dir.deleteRecursively())
    }
}