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
import io.smartdatalake.definitions
import io.smartdatalake.testutils.spark.dataset.TestToolDataset
import io.smartdatalake.testutils.{MockSparkDataObject, TestUtil}
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.spark.dataset.Equality
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, IcebergTableDataObject, IcebergTestUtils, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.LocalDateTime

class IcebergHistorizeWithMergeActionTest extends AnyFunSuite with BeforeAndAfter
  with TestToolDataset with Equality {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  // set additional spark options for delta lake
  protected implicit val session: SparkSession = IcebergTestUtils.session

  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  session.sql("CREATE DATABASE IF NOT EXISTS iceberg1.default")

  before {
    instanceRegistry.clear()
  }

  test("historize load mergeModeEnable") {

    val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcDO = MockSparkDataObject("src1").register
    val tgtTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "historize_output", primaryKey = Some(Seq("lastname", "firstname")))
    val tgtDO = IcebergTableDataObject("tgt1", Some(tempPath + s"/${tgtTable.fullName}"), table = tgtTable)
    tgtDO.dropTable(context)
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id, mergeModeEnable = true)
    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())(context1)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
    action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
    action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(srcSubFeed))(context1)

    {
      val expected = Seq(("doe", "john", 5, Timestamp.valueOf(refTimestamp1), definitions.Environment.historizationUpperHorizonTimestamp))
        .toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getSparkDataFrame()(context1)
        .drop(Historization.historizeHashColName)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("historize 1st load mergeModeEnable", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val action2 = HistorizeAction("ha2", srcDO.id, tgtDO.id, mergeModeEnable = true)
    val l2 = Seq(("doe", "john", 10)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l2, Seq())(context1)
    val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
    action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
    action2.preInit(Seq(srcSubFeed), Seq())(context2.copy(phase = ExecutionPhase.Init))
    action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
    action2.exec(Seq(srcSubFeed2))(context2)

    {
      val expected = Seq(
        ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
        ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), definitions.Environment.historizationUpperHorizonTimestamp)
      ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getSparkDataFrame()(context2)
        .drop(Historization.historizeHashColName)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("historize 2nd load mergeModeEnable", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 3rd load with schema evolution
    // Note that this is not possible with DeltaLake 1.x, as schema evolution with mergeStmt.insertExpr is not properly supported.
    // See also last two tests in IcebergTableDataObjectTest.
    val refTimestamp3 = LocalDateTime.now()
    val context3 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec)
    val tgtDOwithSchemaEvolution = tgtDO.copy(id = "tgt3", allowSchemaEvolution = true) // table remains the same...
    instanceRegistry.register(tgtDOwithSchemaEvolution)
    val action3 = HistorizeAction("ha3", srcDO.id, tgtDOwithSchemaEvolution.id, mergeModeEnable = true)
    val l3 = Seq(("doe", "john", 10, "test")).toDF("lastname", "firstname", "rating", "test")
    srcDO.writeSparkDataFrame(l3, Seq())(context3)
    val srcSubFeed3 = SparkSubFeed(None, "src1", Seq())
    action3.prepare(context3.copy(phase = ExecutionPhase.Prepare))
    action3.preInit(Seq(srcSubFeed), Seq())(context3.copy(phase = ExecutionPhase.Init))
    action3.init(Seq(srcSubFeed3))(context3.copy(phase = ExecutionPhase.Init))
    action3.exec(Seq(srcSubFeed3))(context3)

    {
      val expected = Seq(
        ("doe", "john", 5, null, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
        ("doe", "john", 10, null, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(refTimestamp3.minusNanos(1000000L))),
        ("doe", "john", 10, "test", Timestamp.valueOf(refTimestamp3), definitions.Environment.historizationUpperHorizonTimestamp)
      ).toDF("lastname", "firstname", "rating", "test", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getSparkDataFrame()(context3)
        .drop(Historization.historizeHashColName)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("historize 3rd load mergeModeEnable with schema evolution", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  test("historize load mergeModeEnable CDC") {

    val context = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcDO = MockSparkDataObject("src1").register
    val tgtTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "historize_output", primaryKey = Some(Seq("lastname", "firstname")))
    val tgtDO = IcebergTableDataObject("tgt1", Some(tempPath + s"/${tgtTable.fullName}"), table = tgtTable)
    tgtDO.dropTable(context)
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id, mergeModeEnable = true, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
    val l1 = Seq(("doe", "john", 5, "new"), ("pan", "peter", 5, "new")).toDF("lastname", "firstname", "rating", "operation")
    srcDO.writeSparkDataFrame(l1, Seq())(context1)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
    action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
    action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(srcSubFeed))(context1)

    {
      val expected = Seq(
        ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), definitions.Environment.historizationUpperHorizonTimestamp),
        ("pan", "peter", 5, Timestamp.valueOf(refTimestamp1), definitions.Environment.historizationUpperHorizonTimestamp)
      ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getSparkDataFrame()(context1)
        .drop(Historization.historizeDummyColName)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("historize 1st load mergeModeEnable", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val action2 = HistorizeAction("ha2", srcDO.id, tgtDO.id, mergeModeEnable = true, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
    val l2 = Seq(("doe", "john", 10, "updated"), ("pan", "peter", 5, "deleted")).toDF("lastname", "firstname", "rating", "operation")
    srcDO.writeSparkDataFrame(l2, Seq())(context1)
    val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
    action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
    action2.preInit(Seq(srcSubFeed), Seq())(context2.copy(phase = ExecutionPhase.Init))
    action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
    action2.exec(Seq(srcSubFeed2))(context2)

    {
      val expected = Seq(
        ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
        ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), definitions.Environment.historizationUpperHorizonTimestamp),
        ("pan", "peter", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
      ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getSparkDataFrame()(context1)
        .drop(Historization.historizeDummyColName)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("historize 2nd load mergeModeEnable", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 3rd load with schema evolution
    // Note that this is not possible with DeltaLake 1.x, as schema evolution with mergeStmt.insertExpr is not properly supported.
    // See also last two tests in IcebergTableDataObjectTest.
    val refTimestamp3 = LocalDateTime.now()
    val context3 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec)
    val tgtDOwithSchemaEvolution = tgtDO.copy(id = "tgt3", allowSchemaEvolution = true) // table remains the same...
    instanceRegistry.register(tgtDOwithSchemaEvolution)
    val action3 = HistorizeAction("ha3", srcDO.id, tgtDOwithSchemaEvolution.id, mergeModeEnable = true, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
    val l3 = Seq(("doe", "john", 10, "test", "updated")).toDF("lastname", "firstname", "rating", "test", "operation")
    srcDO.writeSparkDataFrame(l3, Seq())(context3)
    val srcSubFeed3 = SparkSubFeed(None, "src1", Seq())
    action3.prepare(context3.copy(phase = ExecutionPhase.Prepare))
    action3.preInit(Seq(srcSubFeed), Seq())(context3.copy(phase = ExecutionPhase.Init))
    action3.init(Seq(srcSubFeed3))(context3.copy(phase = ExecutionPhase.Init))
    action3.exec(Seq(srcSubFeed3))(context3)

    {
      val expected = Seq(
        ("doe", "john", 5, null, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
        ("doe", "john", 10, null, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(refTimestamp3.minusNanos(1000000L))),
        ("doe", "john", 10, "test", Timestamp.valueOf(refTimestamp3), definitions.Environment.historizationUpperHorizonTimestamp),
        ("pan", "peter", 5, null, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L)))
      ).toDF("lastname", "firstname", "rating", "test", "dl_ts_captured", "dl_ts_delimited")
      val actual = tgtDO.getSparkDataFrame()(context3)
        .drop(Historization.historizeDummyColName)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("historize 3rd load mergeModeEnable with schema evolution", Seq())(actual)(expected)
      assert(resultat)
    }
  }

  test("activate merge mode on existing dataframe no null dl_hash") {

    // setup DataObjects
    val srcTable = Table(Some("default"), "historize_input", catalog = Some("iceberg1"))
    val srcPath = tempPath + s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject("src1", Some(srcPath), table = srcTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "historize_output", catalog = Some("iceberg1"), primaryKey = Some(Seq("id")))
    val tgtPath = tempPath + s"/${tgtTable.fullName}"
    val tgtDO = IcebergTableDataObject("tgt1", Some(tgtPath), table = tgtTable, allowSchemaEvolution = true)
    val context = TestUtil.getDefaultActionPipelineContext
    tgtDO.dropTable(context)
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load without merge mode
    val refTimestamp1 = LocalDateTime.now()
    val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = HistorizeAction("ha",
      inputId = srcDO.id,
      outputId = tgtDO.id
    )

    val l1 = Seq((1, "doe", "john", 5)).toDF("id", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1)(context1)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
    action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
    action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(srcSubFeed))(context1)

    // 1. expectation schema should not have dl_hash column
    assert(!tgtDO.getSparkDataFrame()(context1).columns.contains("dl_hash"))

    // prepare & start 2st load
    val refTimestamp2 = LocalDateTime.now()
    val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val action2 = HistorizeAction("ha",
      inputId = srcDO.id,
      outputId = tgtDO.id,
      mergeModeEnable = true
    )

    val l2 = Seq((1, "doe", "john", 4)).toDF("id", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l2)(context2)
    val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
    action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
    action2.preInit(Seq(srcSubFeed), Seq())(context2.copy(phase = ExecutionPhase.Init))
    action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
    action2.exec(Seq(srcSubFeed2))(context2)

    // expectation dl_hash should not have null values
    assert(tgtDO.getSparkDataFrame()(context2).where($"dl_hash".isNull).count() == 0)


  }

  test("update hash on existing non updated rows") {

    // setup DataObjects
    val srcTable = Table(Some("default"), "historize_input", catalog = Some("iceberg1"))
    val srcPath = tempPath + s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject("src1", Some(srcPath), table = srcTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgtTable = Table(Some("default"), "historize_output", catalog = Some("iceberg1"), primaryKey = Some(Seq("id")))
    val tgtPath = tempPath + s"/${tgtTable.fullName}"
    val tgtDO = IcebergTableDataObject("tgt1", Some(tgtPath), table = tgtTable, allowSchemaEvolution = true)
    val context = TestUtil.getDefaultActionPipelineContext
    tgtDO.dropTable(context)
    instanceRegistry.register(tgtDO)

    // prepare & start 1st load
    val refTimestamp1 = LocalDateTime.now()
    val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
    val action1 = HistorizeAction("ha", inputId = srcDO.id, outputId = tgtDO.id)

    val l1 = Seq((1, "doe", "john", 5)).toDF("id", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1)(context1)
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
    action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
    action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
    action1.exec(Seq(srcSubFeed))(context1)

    // prepare & start 2st load with merge mode
    val refTimestamp2 = LocalDateTime.now()
    val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
    val action2 = HistorizeAction("ha", inputId = srcDO.id, outputId = tgtDO.id, mergeModeEnable = true)

    val l2 = Seq((1, "doe", "john", 5)).toDF("id", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l2)(context2)
    val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
    action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
    action2.preInit(Seq(srcSubFeed), Seq())(context2.copy(phase = ExecutionPhase.Init))
    action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
    action2.exec(Seq(srcSubFeed2))(context2)

    // expectation dl_hash should not have null values
    assert(tgtDO.getSparkDataFrame()(context2).where($"dl_hash".isNull).count() == 0)
  }
}
