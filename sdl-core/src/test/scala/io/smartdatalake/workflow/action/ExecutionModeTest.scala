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

import java.nio.file.Files
import java.time.LocalDateTime

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ActionObjectId
import io.smartdatalake.definitions.{Condition, ExecutionModeFailedException, PartitionDiffMode, SparkIncrementalMode}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, Table, TickTockHiveTableDataObject}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class ExecutionModeTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  // setup some data objects
  val srcTable = Table(Some("default"), "src1")
  val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
  srcDO.dropTable
  instanceRegistry.register(srcDO)
  val l1 = Seq(("doe","john",5),("einstein","albert",2)).toDF("lastname", "firstname", "rating")
  srcDO.writeDataFrame(l1, Seq())

  val tgt1Table = Table(Some("default"), "tgt1", None, Some(Seq("lastname","firstname")))
  val tgt1DO = TickTockHiveTableDataObject("tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), table = tgt1Table, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
  tgt1DO.dropTable
  instanceRegistry.register(tgt1DO)

  val tgt2Table = Table(Some("default"), "tgt2", None, Some(Seq("lastname","firstname")))
  val tgt2DO = TickTockHiveTableDataObject("tgt2", Some(tempPath+s"/${tgt2Table.fullName}"), table = tgt2Table, partitions=Seq("lastname"), numInitialHdfsPartitions = 1)
  tgt2DO.dropTable
  tgt2DO.writeDataFrame(l1.where($"rating"<=2), Seq())
  instanceRegistry.register(tgt2DO)

  implicit val context: ActionPipelineContext = ActionPipelineContext("test", "test", 1, 1, instanceRegistry, Some(LocalDateTime.now()), SmartDataLakeBuilderConfig())

  test("PartitionDiffMode default") {
    val executionMode = PartitionDiffMode()
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val (partitionValues, filter) = executionMode.apply(ActionObjectId("test"), srcDO, tgt1DO, subFeed).get
    assert(partitionValues == Seq(PartitionValues(Map("lastname" -> "doe")), PartitionValues(Map("lastname" -> "einstein"))))
  }

  test("PartitionDiffMode nbOfPartitionValuesPerRun=1 and positive applyCondition") {
    val executionMode = PartitionDiffMode(nbOfPartitionValuesPerRun=Some(1), applyCondition=Some("feed = 'test'"))
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val (partitionValues, filter) = executionMode.apply(ActionObjectId("test"), srcDO, tgt1DO, subFeed).get
    assert(partitionValues == Seq(PartitionValues(Map("lastname" -> "doe"))))
  }

  test("PartitionDiffMode negative applyCondition") {
    val executionMode = PartitionDiffMode(applyCondition=Some("feed = 'failtest'"))
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val executionModeResult = executionMode.apply(ActionObjectId("test"), srcDO, tgt1DO, subFeed)
    assert(executionModeResult.isEmpty)
  }

  test("PartitionDiffMode failCondition") {
    val executionMode = PartitionDiffMode(nbOfPartitionValuesPerRun=Some(1), failCondition=Some("array_contains(transform(selectedPartitionValues, p -> p.lastname), 'doe')"))
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    intercept[ExecutionModeFailedException](executionMode.apply(ActionObjectId("test"), srcDO, tgt1DO, subFeed))
  }

  test("PartitionDiffMode failConditions with description") {
    val executionMode = PartitionDiffMode(nbOfPartitionValuesPerRun=Some(1), failConditions=Seq(Condition(description=Some("fail on lastname=doe"), expression="array_contains(transform(selectedPartitionValues, p -> p.lastname), 'doe')")))
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val ex = intercept[ExecutionModeFailedException](executionMode.apply(ActionObjectId("test"), srcDO, tgt1DO, subFeed))
    assert(ex.msg.contains("fail on lastname=doe"))
  }

  test("PartitionDiffMode selectExpression") {
    val executionMode = PartitionDiffMode(selectExpression = Some("slice(selectedPartitionValues,-1,1)")) // select last value only
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val (partitionValues, filter) = executionMode.apply(ActionObjectId("test"), srcDO, tgt1DO, subFeed).get
    assert(partitionValues == Seq(PartitionValues(Map("lastname" -> "einstein"))))
  }

  test("PartitionDiffMode alternativeOutputId") {
    val executionMode = PartitionDiffMode(alternativeOutputId = Some("tgt2"))
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val (partitionValues, filter) = executionMode.apply(ActionObjectId("test"), srcDO, tgt1DO, subFeed).get
    assert(partitionValues == Seq(PartitionValues(Map("lastname" -> "doe")))) // partition lastname=einstein is already loaded into tgt2
  }

  test("PartitionDiffMode no data to process") {
    val executionMode = PartitionDiffMode()
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    intercept[NoDataToProcessWarning](executionMode.apply(ActionObjectId("test"), srcDO, srcDO, subFeed))
  }

  test("SparkIncrementalMode empty source") {
    val executionMode = SparkIncrementalMode(compareCol = "rating")
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, tgt1DO.id, partitionValues = Seq())
    intercept[NoDataToProcessWarning](executionMode.apply(ActionObjectId("test"), tgt1DO, tgt2DO, subFeed))
  }

  test("SparkIncrementalMode empty target") {
    val executionMode = SparkIncrementalMode(compareCol = "rating")
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val (partitionValues, filter) = executionMode.apply(ActionObjectId("test"), srcDO, tgt1DO, subFeed).get
    assert(filter.isEmpty) // no filter if target is empty as everything needs to be copied
  }

  test("SparkIncrementalMode partially filled target") {
    val executionMode = SparkIncrementalMode(compareCol = "rating")
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, srcDO.id, partitionValues = Seq())
    val (partitionValues, filter) = executionMode.apply(ActionObjectId("test"), srcDO, tgt2DO, subFeed).get
    assert(filter.nonEmpty)
  }

  test("SparkIncrementalMode no data to process") {
    val executionMode = SparkIncrementalMode(compareCol = "rating")
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, tgt2DO.id, partitionValues = Seq())
    intercept[NoDataToProcessWarning](executionMode.apply(ActionObjectId("test"), tgt2DO, tgt2DO, subFeed))
  }

  test("SparkIncrementalMode no data to process dont stop") {
    val executionMode = SparkIncrementalMode(compareCol = "rating", stopIfNoData = false)
    executionMode.prepare(ActionObjectId("test"))
    val subFeed: SparkSubFeed = SparkSubFeed(dataFrame = None, tgt2DO.id, partitionValues = Seq())
    intercept[NoDataToProcessDontStopWarning](executionMode.apply(ActionObjectId("test"), tgt2DO, tgt2DO, subFeed))
  }

}