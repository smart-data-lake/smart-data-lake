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
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.{DeltaLakeModulePlugin, DeltaLakeTableDataObject, HiveTableDataObject, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.LocalDateTime

class DeltaLakeDeduplicateWithMergeActionTest extends FunSuite with BeforeAndAfter {

  // set additional spark options for delta lake
  protected implicit val session : SparkSession = new DeltaLakeModulePlugin().additionalSparkProperties()
    .foldLeft(TestUtil.sparkSessionBuilder(withHive = true)) {
      case (builder, config) => builder.config(config._1, config._2)
    }.getOrCreate()
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  before {
    instanceRegistry.clear()
  }

  test("deduplicate 1st 2nd load mergeModeEnable") {

    // setup DataObjects
    val feed = "deduplicate"
    val srcTable = Table(Some("default"), "deduplicate_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"),  table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "deduplicate_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = DeltaLakeTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = context.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = DeduplicateAction("dda", srcDO.id, tgtDO.id, mergeModeEnable = true)
    val l1 = Seq(("doe","john",5),("pan","peter",5),("hans","muster",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())(context1)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(context1).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    {
      val expected = Seq(("doe", "john", 5, Timestamp.valueOf(refTimestamp1)), ("pan", "peter", 5, Timestamp.valueOf(refTimestamp1)), ("hans", "muster", 5, Timestamp.valueOf(refTimestamp1)))
        .toDF("lastname", "firstname", "rating", "dl_ts_captured")
      val actual = tgtDO.getSparkDataFrame()
      val resultat = expected.isEqual(actual)
      if (!resultat) TestUtil.printFailedTestResult("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = context.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val l2 = Seq(("doe","john",10),("pan","peter",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l2, Seq())(context1)
    action1.exec(Seq(SparkSubFeed(None, "src1", Seq())))(context2)

    {
      // note that we expect pan/peter/5 with updated refTimestamp even though all attributes stay the same
      val expected = Seq(("doe", "john", 10, Timestamp.valueOf(refTimestamp2)), ("pan", "peter", 5, Timestamp.valueOf(refTimestamp2)), ("hans", "muster", 5, Timestamp.valueOf(refTimestamp1)))
        .toDF("lastname", "firstname", "rating", "dl_ts_captured")
      val actual = tgtDO.getSparkDataFrame()
      val resultat = expected.isEqual(actual)
      if (!resultat) TestUtil.printFailedTestResult("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  test("deduplicate 1st 2nd load mergeModeEnable updateCapturedColumnOnlyWhenChanged") {

    // setup DataObjects
    val feed = "deduplicate"
    val srcTable = Table(Some("default"), "deduplicate_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"),  table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "deduplicate_output", None, Some(Seq("lastname","firstname")))
    val tgtDO = DeltaLakeTableDataObject( "tgt1", Some(tempPath+s"/${tgtTable.fullName}"), table = tgtTable)
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = context.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = DeduplicateAction("dda", srcDO.id, tgtDO.id, mergeModeEnable = true, updateCapturedColumnOnlyWhenChanged = true)
    val l1 = Seq(("doe","john",5),("pan","peter",5),("hans","muster",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())(context1)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(context1).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    {
      val expected = Seq(("doe", "john", 5, Timestamp.valueOf(refTimestamp1)), ("pan", "peter", 5, Timestamp.valueOf(refTimestamp1)), ("hans", "muster", 5, Timestamp.valueOf(refTimestamp1)))
        .toDF("lastname", "firstname", "rating", "dl_ts_captured")
      val actual = tgtDO.getSparkDataFrame()
      val resultat = expected.isEqual(actual)
      if (!resultat) TestUtil.printFailedTestResult("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = context.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val l2 = Seq(("doe","john",10),("pan","peter",5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l2, Seq())(context1)
    action1.exec(Seq(SparkSubFeed(None, "src1", Seq())))(context2)

    {
      // note that we expect pan/peter/5 with updated refTimestamp even though all attributes stay the same
      val expected = Seq(("doe", "john", 10, Timestamp.valueOf(refTimestamp2)), ("pan", "peter", 5, Timestamp.valueOf(refTimestamp1)), ("hans", "muster", 5, Timestamp.valueOf(refTimestamp1)))
        .toDF("lastname", "firstname", "rating", "dl_ts_captured")
      val actual = tgtDO.getSparkDataFrame()
      val resultat = expected.isEqual(actual)
      if (!resultat) TestUtil.printFailedTestResult("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }
  }
}
