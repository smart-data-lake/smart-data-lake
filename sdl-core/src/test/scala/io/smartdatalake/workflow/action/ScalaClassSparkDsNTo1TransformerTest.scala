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
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.testutils.TestUtil._
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.workflow.action.spark.customlogic.CustomDsNto1Transformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDsNTo1Transformer
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.CsvFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, SubFeed}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionLayout, PartitionValues}
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.Matchers.{a, thrownBy}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import java.nio.file.Files
import scala.reflect.io.Directory

case class NameRating(name: String, rating: Int)

case class RatingName(rating: Int, name: String)

case class AnotherOutputDataSet(concatenated_name: String, added_rating: Int)

case class AnotherOutputDataSetPartitioned(concatenated_name: String, added_rating: Int, year: String, month: String, day: String)

case class AddedRating(added_rating: Int)

class TestResolutionByIdDs2To1Transformer extends CustomDsNto1Transformer {

  def transform(session: SparkSession, options: Map[String, String], src1Ds: Dataset[NameRating], src2Ds: Dataset[NameRating]): Dataset[AnotherOutputDataSet] = {
    import session.implicits._

    val crossJoined = src1Ds.as("ds1").crossJoin(src2Ds.as("ds2"))
    val result = crossJoined.withColumn("added_rating", $"ds1.rating" + $"ds2.rating")
      .withColumn("concatenated_name", concat($"ds1.name", $"ds2.name"))
      .select("concatenated_name", "added_rating")
      .as[AnotherOutputDataSet]
    result
  }
}

class TestResolutionByOrderingDs2To1Transformer extends CustomDsNto1Transformer {

  //The params are in strange order to illustrate that only Datasets are taken into account when resolving DOs by config order
  def transform(options: Map[String, String], firstDataset: Dataset[RatingName], session: SparkSession, secondDataset: Dataset[NameRating]): Dataset[AnotherOutputDataSet] = {
    import session.implicits._

    val crossJoined = firstDataset.as("ds1").crossJoin(secondDataset.as("ds2"))
    val result = crossJoined.withColumn("added_rating", $"ds1.rating" + $"ds2.rating")
      .withColumn("concatenated_name", concat($"ds1.name", $"ds2.name"))
      .select("concatenated_name", "added_rating")
      .as[AnotherOutputDataSet]
    result
  }
}

class Ds9To1Transformer extends CustomDsNto1Transformer {

  //The params are in strange order to illustrate that only Datasets are taken into account when resolving DOs by config order
  def transform(session: SparkSession, options: Map[String, String], src1: Dataset[NameRating], src2: Dataset[NameRating], src3: Dataset[NameRating], src4: Dataset[NameRating], src5: Dataset[NameRating], src6: Dataset[NameRating], src7: Dataset[NameRating], src8: Dataset[NameRating], src9: Dataset[NameRating]): Dataset[AddedRating] = {
    import session.implicits._

    val unioned = src1.unionByName(src2).unionByName(src3).unionByName(src4).unionByName(src5).unionByName(src6).unionByName(src7).unionByName(src8).unionByName(src9)
    unioned.withColumn("added_rating", sum($"rating").over(Window.partitionBy()).cast("int"))
      .select("added_rating")
      .as[AddedRating]
  }
}

class NoTransformDsNTo1Transformer extends CustomDsNto1Transformer {
}

class BadSignatureTransformDsNTo1Transformer extends CustomDsNto1Transformer {
  def transform(gugus: String, options: Map[String, String], firstDataset: Dataset[RatingName], session: SparkSession, secondDataset: Dataset[NameRating]): Dataset[AnotherOutputDataSet] = {
    throw new IllegalStateException("Code should not be reached")
  }
}

class ScalaClassSparkDsNTo1TransformerTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  private val tempDir = Files.createTempDirectory("testScalaClassSparkDs2To1TransformerTest")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextExec: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("One Ds2To1 Transformation (direct call to exec)") {
    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1Ds", tempPath + "/src1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO1)
    val srcDO2 = CsvFileDataObject("src2Ds", tempPath + "/src2", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO2)
    val tgt1DO = CsvFileDataObject("tgt1Ds", tempPath + "/tgt1", partitions = Seq()
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
    val customTransformerConfig = ScalaClassSparkDsNTo1Transformer(className = classOf[TestResolutionByIdDs2To1Transformer].getName)
    val testAction = CustomDataFrameAction("action", List(srcDO1.id, srcDO2.id), List(tgt1DO.id), transformers = Seq(customTransformerConfig))

    instanceRegistry.register(testAction)

    val srcSubFeed1 = SparkSubFeed(None, "src1Ds", partitionValues = Seq())
    val srcSubFeed2 = SparkSubFeed(None, "src2Ds", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed1, srcSubFeed2))

    val actual = tgt1DO.getSparkDataFrame().as[AnotherOutputDataSet].head()
    assert(actual.added_rating == 15)
    assert(actual.concatenated_name == "johndoe")
  }

  test("No transform method defined (direct call to exec)") {
    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1Ds", tempPath + "/src1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO1)
    val srcDO2 = CsvFileDataObject("src2Ds", tempPath + "/src2", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO2)
    val tgt1DO = CsvFileDataObject("tgt1Ds", tempPath + "/tgt1", partitions = Seq()
      , schema = Some(SparkSchema(StructType.fromDDL("concatenated_name string, added_rating int"))))
    instanceRegistry.register(tgt1DO)


    // prepare & start load
    val customTransformerConfig = ScalaClassSparkDsNTo1Transformer(className = classOf[NoTransformDsNTo1Transformer].getName)
    val testAction = CustomDataFrameAction("action", List(srcDO1.id, srcDO2.id), List(tgt1DO.id), transformers = Seq(customTransformerConfig))

    instanceRegistry.register(testAction)

    val srcSubFeed1 = SparkSubFeed(None, "src1Ds", partitionValues = Seq())
    val srcSubFeed2 = SparkSubFeed(None, "src2Ds", partitionValues = Seq())
    a[AssertionError] shouldBe thrownBy(testAction.exec(Seq(srcSubFeed1, srcSubFeed2)))
  }

  test("Transform method with bad signature defined (direct call to exec)") {
    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1Ds", tempPath + "/src1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO1)
    val srcDO2 = CsvFileDataObject("src2Ds", tempPath + "/src2", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO2)
    val tgt1DO = CsvFileDataObject("tgt1Ds", tempPath + "/tgt1", partitions = Seq()
      , schema = Some(SparkSchema(StructType.fromDDL("concatenated_name string, added_rating int"))))
    instanceRegistry.register(tgt1DO)


    // prepare & start load
    val customTransformerConfig = ScalaClassSparkDsNTo1Transformer(className = classOf[BadSignatureTransformDsNTo1Transformer].getName)
    val testAction = CustomDataFrameAction("action", List(srcDO1.id, srcDO2.id), List(tgt1DO.id), transformers = Seq(customTransformerConfig))

    instanceRegistry.register(testAction)
    val srcSubFeed1 = SparkSubFeed(None, "src1Ds", partitionValues = Seq())
    val srcSubFeed2 = SparkSubFeed(None, "src2Ds", partitionValues = Seq())
    a[java.lang.IllegalStateException] shouldBe thrownBy(testAction.exec(Seq(srcSubFeed1, srcSubFeed2)))

  }

  test("One Ds2To1 Transformation using config file: two identical input types using dataObjectId") {

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1", "target/src1Ds2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    val srcDO2 = CsvFileDataObject("src2", "target/src2Ds2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))

    // fill src with first files
    val dfSrc1 = Seq(("john", 5))
      .toDF("name", "rating")
    srcDO1.writeSparkDataFrame(dfSrc1, Seq())
    val dfSrc2 = Seq(("doe", 10))
      .toDF("name", "rating")
    srcDO2.writeSparkDataFrame(dfSrc2, Seq())

    val sdlb = new DefaultSmartDataLakeBuilder()

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "test_feed_name", configuration = Some(Seq(
      getClass.getResource("/configScalaClassSparkDsNto1Transformer/usingDataObjectId.conf").getPath))
    )
    //Run SDLB
    sdlb.run(sdlConfig)

    val tgt1DO = CsvFileDataObject("tgt1", "target/tgt1Ds2to1", partitions = Seq()
      , schema = Some(SparkSchema(StructType.fromDDL("concatenated_name string, added_rating int"))))
    instanceRegistry.register(tgt1DO)
    val actual = tgt1DO.getSparkDataFrame().as[AnotherOutputDataSet].head()
    assert(actual.added_rating == 15)
    assert(actual.concatenated_name == "johndoe")

    //cleanup
    val directoriesToDelete = {
      List(
        new Directory(new File("target/src1Ds2to1")),
        new Directory(new File("target/src2Ds2to1")),
        new Directory(new File("target/tgt1Ds2to1")),
      )
    }
    directoriesToDelete.foreach(dir => dir.deleteRecursively())
  }

  test("One Ds2To1 Transformation using config file: two identical input types using dataObjectOrdering") {

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1", "target/src1Ds2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    val srcDO2 = CsvFileDataObject("src2", "target/src2Ds2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))

    // fill src with first files
    val dfSrc1 = Seq(("john", 5))
      .toDF("name", "rating")
    srcDO1.writeSparkDataFrame(dfSrc1, Seq())
    val dfSrc2 = Seq(("doe", 10))
      .toDF("name", "rating")
    srcDO2.writeSparkDataFrame(dfSrc2, Seq())

    val sdlb = new DefaultSmartDataLakeBuilder()

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "test_feed_name", configuration = Some(Seq(
      getClass.getResource("/configScalaClassSparkDsNto1Transformer/usingDataObjectOrdering.conf").getPath))
    )
    //Run SDLB
    sdlb.run(sdlConfig)

    val tgt1DO = CsvFileDataObject("tgt1", "target/tgt1Ds2to1", partitions = Seq()
      , schema = Some(SparkSchema(StructType.fromDDL("concatenated_name string, added_rating int"))))
    instanceRegistry.register(tgt1DO)
    val actual = tgt1DO.getSparkDataFrame().as[AnotherOutputDataSet].head()
    assert(actual.added_rating == 15)
    assert(actual.concatenated_name == "johndoe")

    //cleanup
    val directoriesToDelete = {
      List(
        new Directory(new File("target/src1Ds2to1")),
        new Directory(new File("target/src2Ds2to1")),
        new Directory(new File("target/tgt1Ds2to1")),
      )
    }
    directoriesToDelete.foreach(dir => dir.deleteRecursively())
  }

  test("One Ds2To1 Transformation using config file: one input type that is partioned, using addPartitionValuesToOutput = true") {

    // setup DataObjects
    val partitionValues = Seq(PartitionValues(
      Map(("year" -> "1992"),
        ("month" -> "04"),
        ("day" -> "25"))))
    // fill src with first files
    val srcDO1 = SparkSubFeed(SparkDataFrame(
      Seq(("john", 5, "1992", "04", "25"))
        .toDF("name", "rating", "year", "month", "day")
    ), DataObjectId("src1Ds"), partitionValues)

    val srcDO2 = SparkSubFeed(SparkDataFrame(
      Seq(("doe", 10, "1992", "04", "25"))
        .toDF("name", "rating", "year", "month", "day")
    ), DataObjectId("src2Ds"), partitionValues)

    val sdlb = new DefaultSmartDataLakeBuilder()

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "test_feed_name", configuration = Some(Seq(
      getClass.getResource("/configScalaClassSparkDsNto1Transformer/usingDataObjectIdWithPartitionAutoSelect.conf").getPath)),
      partitionValues =
        Some(Seq(PartitionValues(
          Map(("year" -> "1992"),
            ("month" -> "04"),
            ("day" -> "25")
          ))))
    )
    //Run SDLB
    val (subFeeds, _): (Seq[SubFeed], Map[RuntimeEventState, Int]) = sdlb.startSimulationWithConfigFile(sdlConfig, Seq(srcDO1, srcDO2))(session)

    val tgt1DO: SparkSubFeed = subFeeds.head.asInstanceOf[SparkSubFeed]
    val actual = tgt1DO.dataFrame.get.inner.as[AnotherOutputDataSetPartitioned].collect().head
    assert(actual.added_rating == 15)
    assert(actual.concatenated_name == "johndoe")
    assert(actual.year == "1992")
    assert(actual.month == "04")
    assert(actual.day == "25")
  }

  test("One Ds9To1 Transformation using config file: 9 input dataObjects using dataObjectOrdering") {

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1", "target/src1DsNto1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))

    val srcDO2 = CsvFileDataObject("src2", "target/src2DsNto1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))

    // fill src with first files
    val dfSrc1 = Seq(("john", 5))
      .toDF("name", "rating")
    srcDO1.writeSparkDataFrame(dfSrc1, Seq())
    val dfSrc2 = Seq(("doe", 10))
      .toDF("name", "rating")
    srcDO2.writeSparkDataFrame(dfSrc2, Seq())

    val srcDO3 = CsvFileDataObject("src3", "target/src3DsNto1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO3.writeSparkDataFrame(dfSrc1, Seq())

    val srcDO4 = CsvFileDataObject("src4", "target/src4DsNto1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO4.writeSparkDataFrame(dfSrc2, Seq())

    // fill src with first files
    val srcDO5 = CsvFileDataObject("src5", "target/src5DsNto1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO5.writeSparkDataFrame(dfSrc1, Seq())

    val srcDO6 = CsvFileDataObject("src6", "target/src6DsNto1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO6.writeSparkDataFrame(dfSrc2, Seq())

    val srcDO7 = CsvFileDataObject("src7", "target/src7DsNto1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO7.writeSparkDataFrame(dfSrc1, Seq())

    val srcDO8 = CsvFileDataObject("src8", "target/src8DsNto1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO8.writeSparkDataFrame(dfSrc2, Seq())

    val srcDO9 = CsvFileDataObject("src9", "target/src9DsNto1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO9.writeSparkDataFrame(dfSrc2, Seq())

    val sdlb = new DefaultSmartDataLakeBuilder()

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "test_feed_name", configuration = Some(Seq(
      getClass.getResource("/configScalaClassSparkDsNto1Transformer/usingDataObjectOrdering9Inputs.conf").getPath))
    )
    //Run SDLB
    sdlb.run(sdlConfig)

    val tgt1DO = CsvFileDataObject("tgt1", "target/tgt1Ds2to1", partitions = Seq()
      , schema = Some(SparkSchema(StructType.fromDDL("added_rating int"))))
    instanceRegistry.register(tgt1DO)
    val actual = tgt1DO.getSparkDataFrame().as[AddedRating].head()
    assert(actual.added_rating == 70)

    //cleanup
    val directoriesToDelete = {
      List(
        new Directory(new File("target/src1DsNto1")),
        new Directory(new File("target/src2DsNto1")),
        new Directory(new File("target/src3DsNto1")),
        new Directory(new File("target/src4DsNto1")),
        new Directory(new File("target/src5DsNto1")),
        new Directory(new File("target/src6DsNto1")),
        new Directory(new File("target/src7DsNto1")),
        new Directory(new File("target/src8DsNto1")),
        new Directory(new File("target/src9DsNto1")),
        new Directory(new File("target/tgt1DsNto1")),
      )
    }
    directoriesToDelete.foreach(dir => dir.deleteRecursively())
  }

  test("One Ds9To1 Transformation using config file: wrong number of dataset params using dataObjectOrdering") {

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1", "target/src1Ds2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO1)
    val srcDO2 = CsvFileDataObject("src2", "target/src2Ds2to1", partitions = Seq("name")
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
      getClass.getResource("/configScalaClassSparkDsNto1Transformer/usingDataObjectOrdering9InputsWrongTransformer.conf").getPath))
    )
    //Run SDLB

    a[TaskFailedException] shouldBe thrownBy(sdlb.run(sdlConfig))
  }
}