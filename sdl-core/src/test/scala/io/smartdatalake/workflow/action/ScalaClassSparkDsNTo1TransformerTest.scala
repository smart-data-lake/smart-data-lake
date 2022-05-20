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
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.workflow.action.spark.customlogic.CustomDsNto1Transformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDsNTo1Transformer
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.CsvFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
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

class TestResolutionByIdDS2To1Transformer extends CustomDsNto1Transformer {

  def transform(session: SparkSession, options: Map[String, String], src1DS: Dataset[NameRating], src2DS: Dataset[NameRating]): Dataset[AnotherOutputDataSet] = {
    import session.implicits._

    val crossJoined = src1DS.as("ds1").crossJoin(src2DS.as("ds2"))
    val result = crossJoined.withColumn("added_rating", $"ds1.rating" + $"ds2.rating")
      .withColumn("concatenated_name", concat($"ds1.name", $"ds2.name"))
      .select("concatenated_name", "added_rating")
      .as[AnotherOutputDataSet]
    result
  }
}

class TestResolutionByOrderingDS2To1Transformer extends CustomDsNto1Transformer {

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

class DS9To1Transformer extends CustomDsNto1Transformer {

  //The params are in strange order to illustrate that only Datasets are taken into account when resolving DOs by config order
  def transform(session: SparkSession, options: Map[String, String], d1: Dataset[NameRating], d2: Dataset[NameRating], d3: Dataset[NameRating], d4: Dataset[NameRating], d5: Dataset[NameRating], d6: Dataset[NameRating], d7: Dataset[NameRating], d8: Dataset[NameRating], d9: Dataset[NameRating]): Dataset[AnotherOutputDataSet] = {
    import session.implicits._

    val unioned = d1.as("ds1").union(d2.as("ds2")).union(d3.as("ds3")).union(d4.as("ds4")).union(d5.as("ds5")).union(d6.as("ds6")).union(d7.as("ds7")).union(d8.as("ds8")).union(d9.as("ds9"))
    d1.show()
    d2.show()
    d3.show()
    d4.show()
    d5.show()
    d6.show()
    d7.show()
    d8.show()
    d9.show()

    unioned.show(false)
    val result = unioned.withColumn("added_rating", $"ds1.rating" + $"ds2.rating" + $"ds3.rating" + $"ds4.rating" + $"ds5.rating" + $"ds6.rating" + $"ds7.rating" + $"ds8.rating" + $"ds9.rating")
      .withColumn("concatenated_name", concat($"ds1.name", $"ds2.name", $"ds3.name", $"ds4.name", $"ds5.name", $"ds6.name", $"ds7.name", $"ds8.name", $"ds9.name"))
      .select("concatenated_name", "added_rating")

    result.as[AnotherOutputDataSet]
  }
}

class NoTransformDSNTo1Transformer extends CustomDsNto1Transformer {
}

class BadSignatureTransformDSNTo1Transformer extends CustomDsNto1Transformer {
  def transform(gugus: String, options: Map[String, String], firstDataset: Dataset[RatingName], session: SparkSession, secondDataset: Dataset[NameRating]): Dataset[AnotherOutputDataSet] = {
    ???
  }
}

class ScalaClassSparkDsNTo1TransformerTest extends FunSuite with BeforeAndAfter {

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
    val customTransformerConfig = ScalaClassSparkDsNTo1Transformer(className = classOf[TestResolutionByIdDS2To1Transformer].getName)
    val testAction = CustomDataFrameAction("action", List(srcDO1.id, srcDO2.id), List(tgt1DO.id), transformers = Seq(customTransformerConfig))

    instanceRegistry.register(testAction)

    val srcSubFeed1 = SparkSubFeed(None, "src1DS", partitionValues = Seq())
    val srcSubFeed2 = SparkSubFeed(None, "src2DS", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed1, srcSubFeed2))

    val actual = tgt1DO.getSparkDataFrame().as[AnotherOutputDataSet].head()
    assert(actual.added_rating == 15)
    assert(actual.concatenated_name == "johndoe")
  }

  test("No transform method defined (direct call to exec)") {
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


    // prepare & start load
    val customTransformerConfig = ScalaClassSparkDsNTo1Transformer(className = classOf[NoTransformDSNTo1Transformer].getName)
    val testAction = CustomDataFrameAction("action", List(srcDO1.id, srcDO2.id), List(tgt1DO.id), transformers = Seq(customTransformerConfig))

    instanceRegistry.register(testAction)

    val srcSubFeed1 = SparkSubFeed(None, "src1DS", partitionValues = Seq())
    val srcSubFeed2 = SparkSubFeed(None, "src2DS", partitionValues = Seq())
    a[AssertionError] shouldBe thrownBy(testAction.exec(Seq(srcSubFeed1, srcSubFeed2)))
  }

  test("Transform method with bad signature defined (direct call to exec)") {
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


    // prepare & start load
    val customTransformerConfig = ScalaClassSparkDsNTo1Transformer(className = classOf[BadSignatureTransformDSNTo1Transformer].getName)
    val testAction = CustomDataFrameAction("action", List(srcDO1.id, srcDO2.id), List(tgt1DO.id), transformers = Seq(customTransformerConfig))

    instanceRegistry.register(testAction)
    val srcSubFeed1 = SparkSubFeed(None, "src1DS", partitionValues = Seq())
    val srcSubFeed2 = SparkSubFeed(None, "src2DS", partitionValues = Seq())
    a[java.lang.IllegalStateException] shouldBe thrownBy(testAction.exec(Seq(srcSubFeed1, srcSubFeed2)))

  }

  test("One DS2To1 Transformation using config file: two identical input types using dataObjectId") {

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1", "target/src1DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    val srcDO2 = CsvFileDataObject("src2", "target/src2DS2to1", partitions = Seq("name")
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
      val srcDO2 = CsvFileDataObject("src2", "target/src2DS2to1", partitions = Seq("name")
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

  test("One DS9To1 Transformation using config file: two different input types using dataObjectOrdering") {

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO1 = CsvFileDataObject("src1", "target/src1DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))

    val srcDO2 = CsvFileDataObject("src2", "target/src2DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))

    // fill src with first files
    val dfSrc1 = Seq(("john", 5))
      .toDF("name", "rating")
    srcDO1.writeSparkDataFrame(dfSrc1, Seq())
    val dfSrc2 = Seq(("doe", 10))
      .toDF("name", "rating")
    srcDO2.writeSparkDataFrame(dfSrc2, Seq())

    val srcDO3 = CsvFileDataObject("src3", "target/src3DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO3.writeSparkDataFrame(dfSrc1, Seq())

    val srcDO4 = CsvFileDataObject("src4", "target/src4DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO4.writeSparkDataFrame(dfSrc2, Seq())

    // fill src with first files
    val srcDO5 = CsvFileDataObject("src5", "target/src5DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO5.writeSparkDataFrame(dfSrc1, Seq())

    val srcDO6 = CsvFileDataObject("src6", "target/src6DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO6.writeSparkDataFrame(dfSrc1, Seq())

    val srcDO7 = CsvFileDataObject("src7", "target/src7DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO7.writeSparkDataFrame(dfSrc1, Seq())

    val srcDO8 = CsvFileDataObject("src8", "target/src8DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO8.writeSparkDataFrame(dfSrc2, Seq())

    val srcDO9 = CsvFileDataObject("src9", "target/src9DS2to1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    srcDO9.writeSparkDataFrame(dfSrc1, Seq())

    val sdlb = new DefaultSmartDataLakeBuilder()

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "test_feed_name", configuration = Some(Seq(
      getClass.getResource("/configScalaClassSparkDsNto1Transformer/usingDataObjectOrdering9Inputs.conf").getPath))
    )
    //Run SDLB
    sdlb.run(sdlConfig)

    val tgt1DO = CsvFileDataObject("tgt1", "target/tgt1DS2to1", partitions = Seq()
      , schema = Some(SparkSchema(StructType.fromDDL("concatenated_name string, added_rating int"))))
    instanceRegistry.register(tgt1DO)
    val actual = tgt1DO.getSparkDataFrame().as[AnotherOutputDataSet].head()
    assert(actual.added_rating == 70)
    assert(actual.concatenated_name == "johndoejohndoejohndoejohndoedoe")

    //cleanup
    val directoriesToDelete = {
      List(
        new Directory(new File("target/src1DS2to1")),
        new Directory(new File("target/src2DS2to1")),
        new Directory(new File("target/src3DS2to1")),
        new Directory(new File("target/src4DS2to1")),
        new Directory(new File("target/src5DS2to1")),
        new Directory(new File("target/src6DS2to1")),
        new Directory(new File("target/src7DS2to1")),
        new Directory(new File("target/src8DS2to1")),
        new Directory(new File("target/src9DS2to1")),
        new Directory(new File("target/tgt1DS2to1")),
      )
    }
    directoriesToDelete.foreach(dir => dir.deleteRecursively())
  }

  test("One DS9To1 Transformation using config file: wrong number of dataset params using dataObjectOrdering") {

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
      getClass.getResource("/configScalaClassSparkDsNto1Transformer/usingDataObjectOrdering9InputsWrongTransformer.conf").getPath))
    )
    //Run SDLB

    a[TaskFailedException] shouldBe thrownBy(sdlb.run(sdlConfig))
  }
}