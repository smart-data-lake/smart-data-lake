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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.definitions._
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.executionMode.{CustomPartitionMode, CustomPartitionModeLogic, DataFrameIncrementalMode, ExecutionModeFailedException, FileIncrementalMoveMode, PartitionDiffMode}
import io.smartdatalake.workflow.action.spark.customlogic.{SparkUDFCreator, SparkUDFCreatorConfig}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.{ActionPipelineContext, FileRefMapping, FileSubFeed}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.custom.ExpressionEvaluator
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import java.nio.file.Files

class ExecutionModeTest extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  // setup some data objects
  val srcTable = Table(Some("default"), "src1")
  val srcDO = HiveTableDataObject("src1", Some(tempPath + s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("lastname"), numInitialHdfsPartitions = 1)
  instanceRegistry.register(srcDO)

  val tgt1Table = Table(Some("default"), "tgt1", None, Some(Seq("lastname", "firstname")))
  val tgt1DO = TickTockHiveTableDataObject("tgt1", Some(tempPath + s"/${tgt1Table.fullName}"), table = tgt1Table, partitions = Seq("lastname"), numInitialHdfsPartitions = 1)
  instanceRegistry.register(tgt1DO)

  val tgt2Table = Table(Some("default"), "tgt2", None, Some(Seq("lastname", "firstname")))
  val tgt2DO = TickTockHiveTableDataObject("tgt2", Some(tempPath + s"/${tgt2Table.fullName}"), table = tgt2Table, partitions = Seq("lastname"), numInitialHdfsPartitions = 1)
  instanceRegistry.register(tgt2DO)

  val fileSrcDO = CsvFileDataObject("fileSrcDO", tempPath + s"/fileTestSrc", partitions = Seq("lastname"))
  instanceRegistry.register(fileSrcDO)

  val fileEmptyDO = CsvFileDataObject("fileEmptyDO", tempPath + s"/fileTestEmpty", partitions = Seq("lastname"))
  instanceRegistry.register(fileEmptyDO)

  override def beforeAll(): Unit = {
    srcDO.dropTable
    val l1 = Seq(("doe", "john", 5), ("einstein", "albert", 2)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    tgt1DO.dropTable
    tgt2DO.dropTable
    tgt2DO.writeSparkDataFrame(l1.where($"rating" <= 2), Seq())
    fileSrcDO.writeSparkDataFrame(l1, Seq())
  }

  test("PartitionDiffMode default") {
    val executionMode = PartitionDiffMode()
    executionMode.prepare(ActionId("test"))
    val subFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping).get
    assert(result.inputPartitionValues == Seq(PartitionValues(Map("lastname" -> "doe")), PartitionValues(Map("lastname" -> "einstein"))))
  }

  test("PartitionDiffMode nbOfPartitionValuesPerRun=1 and positive applyCondition") {
    val executionMode = PartitionDiffMode(nbOfPartitionValuesPerRun = Some(1), applyCondition = Some("feed = 'feedTest'"))
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping).get
    assert(result.inputPartitionValues == Seq(PartitionValues(Map("lastname" -> "doe"))))
    assert(result.outputPartitionValues == Seq(PartitionValues(Map("lastname" -> "doe"))))
  }

  test("PartitionDiffMode negative applyCondition") {
    val executionMode = PartitionDiffMode(applyCondition = Some("feed = 'failtest'"))
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val executionModeResult = executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping)
    assert(executionModeResult.isEmpty)
  }

  test("PartitionDiffMode failCondition") {
    val executionMode = PartitionDiffMode(nbOfPartitionValuesPerRun = Some(1), failCondition = Some("array_contains(transform(selectedOutputPartitionValues, p -> p.lastname), 'doe')"))
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    intercept[ExecutionModeFailedException](executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping))
  }

  test("PartitionDiffMode failConditions with description") {
    val executionMode = PartitionDiffMode(nbOfPartitionValuesPerRun = Some(1), failConditions = Seq(Condition(description = Some("fail on lastname=doe"), expression = "array_contains(transform(selectedOutputPartitionValues, p -> p.lastname), 'doe')")))
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val ex = intercept[ExecutionModeFailedException](executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping))
    assert(ex.msg.contains("fail on lastname=doe"))
  }

  test("PartitionDiffMode selectExpression") {
    val executionMode = PartitionDiffMode(selectExpression = Some("slice(selectedOutputPartitionValues,-1,1)")) // select last value only
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping).get
    assert(result.inputPartitionValues == Seq(PartitionValues(Map("lastname" -> "einstein"))))
  }

  test("PartitionDiffMode selectExpression should be applied before nbOfPartitionValuesPerRun=1 ") {
    val executionMode = PartitionDiffMode(selectExpression = Some("slice(selectedOutputPartitionValues,-1,1)"), nbOfPartitionValuesPerRun = Some(1)) // select last value only
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping).get
    // 'einstein' is the last partition value. If selectExpression is applied after nbOfPartitionValuesPerRun, this would result in 'doe' (the first partition value).
    assert(result.inputPartitionValues == Seq(PartitionValues(Map("lastname" -> "einstein"))))
  }

  test("PartitionDiffMode selectAdditionalInputExpression with udf") {
    val udfConfig = SparkUDFCreatorConfig(classOf[TestUdfAddLastnameEinstein].getName)
    ExpressionEvaluator.registerUdf("testUdfAddLastNameEinstein", udfConfig.getUDF)
    val executionMode = PartitionDiffMode(selectAdditionalInputExpression = Some("testUdfAddLastNameEinstein(selectedInputPartitionValues,inputPartitionValues)"))
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDO, tgt2DO, subFeed, PartitionValues.oneToOneMapping).get
    assert(result.outputPartitionValues == Seq(PartitionValues(Map("lastname" -> "doe")))) // Einstein already exists in tgt2
    assert(result.inputPartitionValues.toSet == Set(PartitionValues(Map("lastname" -> "einstein")), PartitionValues(Map("lastname" -> "doe")))) // but Einstein is added as additional input partition
  }

  test("PartitionDiffMode alternativeOutputId") {
    val executionMode = PartitionDiffMode(alternativeOutputId = Some("tgt2"))
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping).get
    assert(result.inputPartitionValues == Seq(PartitionValues(Map("lastname" -> "doe")))) // partition lastname=einstein is already loaded into tgt2
  }

  test("PartitionDiffMode no data to process") {
    val executionMode = PartitionDiffMode()
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    intercept[NoDataToProcessWarning](executionMode.apply(ActionId("test"), srcDO, srcDO, subFeed, PartitionValues.oneToOneMapping))
  }

  test("PartitionDiffMode no data to process after selectExpression") {
    val executionMode = PartitionDiffMode(selectExpression = Some("filter(givenPartitionValues, pv -> false)"))
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    intercept[NoDataToProcessWarning](executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping))
  }

  test("DataFrameIncrementalMode empty source") {
    val executionMode = DataFrameIncrementalMode(compareCol = "rating")
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, tgt1DO.id, partitionValues = Seq())
    intercept[NoDataToProcessWarning](executionMode.apply(ActionId("test"), tgt1DO, tgt2DO, subFeed, PartitionValues.oneToOneMapping))
  }

  test("DataFrameIncrementalMode empty target") {
    val executionMode = DataFrameIncrementalMode(compareCol = "rating")
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping).get
    assert(result.filter.isEmpty) // no filter if target is empty as everything needs to be copied
  }

  test("DataFrameIncrementalMode partially filled target") {
    val executionMode = DataFrameIncrementalMode(compareCol = "rating")
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDO, tgt2DO, subFeed, PartitionValues.oneToOneMapping).get
    assert(result.filter.nonEmpty)
  }

  test("DataFrameIncrementalMode no data to process") {
    val executionMode = DataFrameIncrementalMode(compareCol = "rating")
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, tgt2DO.id, partitionValues = Seq())
    intercept[NoDataToProcessWarning](executionMode.apply(ActionId("test"), tgt2DO, tgt2DO, subFeed, PartitionValues.oneToOneMapping))
  }

  test("CustomPartitionMode alternativeOutputId") {
    val executionMode = CustomPartitionMode(className = classOf[TestCustomPartitionMode].getName, alternativeOutputId = Some("tgt2"))
    executionMode.prepare(ActionId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDO, tgt1DO, subFeed, PartitionValues.oneToOneMapping).get
    assert(result.inputPartitionValues == Seq(PartitionValues(Map("lastname" -> "doe")))) // partition lastname=einstein is already loaded into tgt2
  }

  test("FileIncrementalMoveMode select file refs") {
    val executionMode = FileIncrementalMoveMode()
    executionMode.prepare(ActionId("test"))
    val subFeed: FileSubFeed = FileSubFeed(fileRefs = None, dataObjectId = fileSrcDO.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), fileSrcDO, fileSrcDO, subFeed, PartitionValues.oneToOneMapping).get
    assert(result.fileRefs.get.nonEmpty)
  }

  test("FileIncrementalMoveMode no data to process") {
    val executionMode = FileIncrementalMoveMode()
    executionMode.prepare(ActionId("test"))
    val subFeed: FileSubFeed = FileSubFeed(fileRefs = None, dataObjectId = fileEmptyDO.id, partitionValues = Seq())
    intercept[NoDataToProcessWarning](executionMode.apply(ActionId("test"), fileEmptyDO, fileEmptyDO, subFeed, PartitionValues.oneToOneMapping))
  }

  test("FileIncrementalMoveMode archive relative path") {
    val srcDOArchive = ParquetFileDataObject("srcArchive", tempPath + s"/srcArchive")
    srcDOArchive.deleteAll
    instanceRegistry.register(srcDOArchive)
    val l1 = Seq(("doe", "john", 5), ("einstein", "albert", 2)).toDF("lastname", "firstname", "rating")
    srcDOArchive.writeSparkDataFrame(l1, Seq())

    val executionMode = FileIncrementalMoveMode(archivePath = Some("archive"))
    executionMode.prepare(ActionId("test"))
    val subFeed: FileSubFeed = FileSubFeed(fileRefs = None, dataObjectId = srcDOArchive.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDOArchive, srcDOArchive, subFeed, PartitionValues.oneToOneMapping).get
    val subFeedWithFileRefs = subFeed.copy(fileRefs = result.fileRefs, fileRefMapping = result.fileRefs.map(_.map(fileRef => FileRefMapping(fileRef, fileRef))))
    assert(srcDOArchive.getFileRefs(Seq()).nonEmpty)
    executionMode.postExec(ActionId("test"), srcDOArchive, srcDOArchive, subFeedWithFileRefs, subFeedWithFileRefs)
    assert(srcDOArchive.getFileRefs(Seq()).isEmpty)
    assert(srcDOArchive.filesystem.listStatus(new Path(srcDOArchive.path + s"/archive")).toSeq.nonEmpty)
  }

  test("FileIncrementalMoveMode archive absolute path") {
    val srcDOArchive = ParquetFileDataObject("srcArchive", tempPath + "/srcArchive")
    srcDOArchive.deleteAll
    instanceRegistry.register(srcDOArchive)
    val l1 = Seq(("doe", "john", 5), ("einstein", "albert", 2)).toDF("lastname", "firstname", "rating")
    srcDOArchive.writeSparkDataFrame(l1, Seq())

    val executionMode = FileIncrementalMoveMode(archivePath = Some(tempPath + "/absoluteArchive"))
    executionMode.prepare(ActionId("test"))
    val subFeed: FileSubFeed = FileSubFeed(fileRefs = None, dataObjectId = srcDOArchive.id, partitionValues = Seq())
    val result = executionMode.apply(ActionId("test"), srcDOArchive, srcDOArchive, subFeed, PartitionValues.oneToOneMapping).get
    val subFeedWithFileRefs = subFeed.copy(fileRefs = result.fileRefs, fileRefMapping = result.fileRefs.map(_.map(fileRef => FileRefMapping(fileRef, fileRef))))
    assert(srcDOArchive.getFileRefs(Seq()).nonEmpty)
    executionMode.postExec(ActionId("test"), srcDOArchive, srcDOArchive, subFeedWithFileRefs, subFeedWithFileRefs)
    assert(srcDOArchive.getFileRefs(Seq()).isEmpty)
    assert(srcDOArchive.filesystem.listStatus(new Path(executionMode.archivePath.get)).toSeq.nonEmpty)
  }
}

class TestCustomPartitionMode() extends CustomPartitionModeLogic {
  override def apply(options: Map[String, String], actionId: ActionId, input: DataObject with CanHandlePartitions, output: DataObject with CanHandlePartitions, givenPartitionValues: Seq[Map[String, String]], context: ActionPipelineContext): Option[Seq[Map[String, String]]] = {
    val partitionValuesToProcess = input.listPartitions(context).diff(output.listPartitions(context))
    Some(partitionValuesToProcess.map(_.getMapString))
  }
}

class TestUdfAddLastnameEinstein() extends SparkUDFCreator {
  val partitionValueToAdd = Map("lastname" -> "einstein")

  def addLastnameEinstein(selectedInputPartitionValues: Seq[Map[String, String]], inputPartitionValues: Seq[Map[String, String]]): Seq[Map[String, String]] = {
    if (selectedInputPartitionValues.isEmpty) return Seq()
    if (!selectedInputPartitionValues.contains(partitionValueToAdd) && inputPartitionValues.contains(partitionValueToAdd)) selectedInputPartitionValues :+ partitionValueToAdd
    else selectedInputPartitionValues
  }

  override def get(options: Map[String, String]): UserDefinedFunction = udf(addLastnameEinstein _)
}