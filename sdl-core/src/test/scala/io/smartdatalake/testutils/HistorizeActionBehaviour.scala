/*
 * Smart Data Lake - Build your data lake the smart way.
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

package io.smartdatalake.testutils

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions
import io.smartdatalake.testutils.GenericTestTool.printFailedTestResult
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.HistorizeAction
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ExecutionPhase}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * This trait defines tests for the behaviour of HistorizeAction.
 * They can be used with various output DataObject types to ensure consistent behaviour for e.g. Jdbc, DeltaLake, ...
 */
trait HistorizeActionBehaviour {
  this: AnyFunSuite with Matchers with SmartDataLakeLogger =>

  implicit private val implicitLogger: Logger = logger

  def registerDataObject[A <: TableDataObject](dataObject: A)
                        (implicit instanceRegistry: InstanceRegistry, context: ActionPipelineContext): A = {
    dataObject.dropTable(context)
    instanceRegistry.register(dataObject)
    dataObject
  }

  def historizeWithMergeMode(createSrcDataObject: ((String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame),
                             createTgtDataObject: ((String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame),
                             tgtConnection: Option[Connection]): Unit = {

    test("historize load mergeModeEnable") {

      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)

      implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(srcDO.getSubFeedSupportedTypes.head)
      import helper.implicits._


      // prepare & start 1st load
      val refTimestamp1 = LocalDateTime.now()
      val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id, mergeModeEnable = true)
      val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec, currentAction=Some(action1))
      val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
      srcDO.writeDataFrame(l1, Seq())(context1)
      val srcSubFeed = SparkSubFeed(None, "src1", Seq())
      action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
      action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
      action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
      action1.exec(Seq(srcSubFeed))(context1)

      {
        val expected = Seq(("doe", "john", 5, Timestamp.valueOf(refTimestamp1), definitions.Environment.historizationUpperHorizonTimestamp))
          .toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame()(context1)
          .drop(Historization.historizeHashColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResult("historize 1st load mergeModeEnable", Seq())(actual)(expected)
        assert(resultat)
      }

      // prepare & start 2nd load
      val refTimestamp2 = LocalDateTime.now()
      val action2 = HistorizeAction("ha2", srcDO.id, tgtDO.id, mergeModeEnable = true)
      val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec, currentAction = Some(action2))
      val l2 = Seq(("doe", "john", 10)).toDF("lastname", "firstname", "rating")
      srcDO.writeDataFrame(l2, Seq())(context2)
      val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
      action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
      action2.preInit(Seq(srcSubFeed2), Seq())(context2.copy(phase = ExecutionPhase.Init))
      action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
      action2.exec(Seq(srcSubFeed2))(context2)

      {
        val expected = Seq(
          ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
          ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), definitions.Environment.historizationUpperHorizonTimestamp)
        ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame()(context2)
          .drop(Historization.historizeHashColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResult("historize 2nd load mergeModeEnable", Seq())(actual)(expected)
        assert(resultat)
      }

      // prepare & start 3rd load with schema evolution
      val refTimestamp3 = LocalDateTime.now()
      val action3 = HistorizeAction("ha3", srcDO.id, tgtDO.id, mergeModeEnable = true)
      val context3 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec, currentAction = Some(action3))
      val l3 = Seq(("doe", "john", 10, "test")).toDF("lastname", "firstname", "rating", "test")
      srcDO.writeDataFrame(l3, Seq())(context3)
      val srcSubFeed3 = SparkSubFeed(None, "src1", Seq())
      action3.prepare(context3.copy(phase = ExecutionPhase.Prepare))
      action3.preInit(Seq(srcSubFeed3), Seq())(context3.copy(phase = ExecutionPhase.Init))
      action3.init(Seq(srcSubFeed3))(context3.copy(phase = ExecutionPhase.Init))
      action3.exec(Seq(srcSubFeed3))(context3)

      {
        val expected = Seq(
          ("doe", "john", 5, null, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
          ("doe", "john", 10, null, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(refTimestamp3.minusNanos(1000000L))),
          ("doe", "john", 10, "test", Timestamp.valueOf(refTimestamp3), definitions.Environment.historizationUpperHorizonTimestamp)
        ).toDF("lastname", "firstname", "rating", "test", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame()(context3)
          .drop(Historization.historizeHashColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResult("historize 3rd load mergeModeEnable with schema evolution", Seq())(actual)(expected)
        assert(resultat)
      }
    }

    test("historize load mergeModeEnable CDC") {

      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)

      implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(srcDO.getSubFeedSupportedTypes.head)
      import helper.implicits._

      // prepare & start 1st load
      val refTimestamp1 = LocalDateTime.now()
      val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id, mergeModeEnable = true, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
      val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec, currentAction = Some(action1))
      val l1 = Seq(("doe", "john", 5, "new"), ("pan", "peter", 5, "new")).toDF("lastname", "firstname", "rating", "operation")
      srcDO.writeDataFrame(l1, Seq())(context1)
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
        val actual = tgtDO.getDataFrame()(context1)
          .drop(Historization.historizeDummyColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResult("historize 1st load mergeModeEnable", Seq())(actual)(expected)
        assert(resultat)
      }

      // prepare & start 2nd load
      val refTimestamp2 = LocalDateTime.now()
      val action2 = HistorizeAction("ha2", srcDO.id, tgtDO.id, mergeModeEnable = true, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
      val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec, currentAction = Some(action2))
      val l2 = Seq(("doe", "john", 10, "updated"), ("pan", "peter", 5, "deleted")).toDF("lastname", "firstname", "rating", "operation")
      srcDO.writeDataFrame(l2, Seq())(context1)
      val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
      action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
      action2.preInit(Seq(srcSubFeed2), Seq())(context2.copy(phase = ExecutionPhase.Init))
      action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
      action2.exec(Seq(srcSubFeed2))(context2)

      {
        val expected = Seq(
          ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
          ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), definitions.Environment.historizationUpperHorizonTimestamp),
          ("pan", "peter", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
        ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame()(context1)
          .drop(Historization.historizeDummyColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResult("historize 2nd load mergeModeEnable", Seq())(actual)(expected)
        assert(resultat)
      }

      // prepare & start 3rd load with schema evolution
      val refTimestamp3 = LocalDateTime.now()
      val action3 = HistorizeAction("ha3", srcDO.id, tgtDO.id, mergeModeEnable = true, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
      val context3 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec, currentAction = Some(action3))
      val l3 = Seq(("doe", "john", 10, "test", "updated")).toDF("lastname", "firstname", "rating", "test", "operation")
      srcDO.writeDataFrame(l3, Seq())(context3)
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
        val actual = tgtDO.getDataFrame()(context3)
          .drop(Historization.historizeDummyColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResult("historize 3rd load mergeModeEnable with schema evolution", Seq())(actual)(expected)
        assert(resultat)
      }
    }


    test("activate merge mode on existing dataframe no null dl_hash") {

      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)

      implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("id")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(srcDO.getSubFeedSupportedTypes.head)
      import helper.col
      import helper.implicits._

      // prepare & start 1st load without merge mode
      val refTimestamp1 = LocalDateTime.now()
      val action1 = HistorizeAction("ha1", inputId = srcDO.id, outputId = tgtDO.id)
      val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec, currentAction = Some(action1))

      val l1 = Seq((1, "doe", "john", 5)).toDF("id", "lastname", "firstname", "rating")
      srcDO.writeDataFrame(l1)(context1)
      val srcSubFeed = SparkSubFeed(None, "src1", Seq())
      action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
      action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
      action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
      action1.exec(Seq(srcSubFeed))(context1)

      // 1. expectation schema should not have dl_hash column
      assert(!tgtDO.getDataFrame()(context1).columns.contains("dl_hash"))

      // prepare & start 2st load
      val refTimestamp2 = LocalDateTime.now()
      val action2 = HistorizeAction("ha2", inputId = srcDO.id, outputId = tgtDO.id, mergeModeEnable = true)
      val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec, currentAction = Some(action2))

      val l2 = Seq((1, "doe", "john", 4)).toDF("id", "lastname", "firstname", "rating")
      srcDO.writeDataFrame(l2)(context2)
      val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
      action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
      action2.preInit(Seq(srcSubFeed), Seq())(context2.copy(phase = ExecutionPhase.Init))
      action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
      action2.exec(Seq(srcSubFeed2))(context2)

      // expectation dl_hash should not have null values
      assert(tgtDO.getDataFrame()(context2).where(col("dl_hash").isNull).count == 0)
    }

    test("update hash on existing non updated rows") {

      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)

      implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("id")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(srcDO.getSubFeedSupportedTypes.head)
      import helper.col
      import helper.implicits._

      // prepare & start 1st load with merge mode
      val refTimestamp1 = LocalDateTime.now()
      val action1 = HistorizeAction("ha1", inputId = srcDO.id, outputId = tgtDO.id)
      val context1 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec, currentAction = Some(action1))

      val l1 = Seq((1, "doe", "john", 5)).toDF("id", "lastname", "firstname", "rating")
      srcDO.writeDataFrame(l1)(context1)
      val srcSubFeed = SparkSubFeed(None, "src1", Seq())
      action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
      action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
      action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
      action1.exec(Seq(srcSubFeed))(context1)

      // prepare & start 2st load
      val refTimestamp2 = LocalDateTime.now()
      val action2 = HistorizeAction("ha2", inputId = srcDO.id, outputId = tgtDO.id, mergeModeEnable = true)
      val context2 = TestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec, currentAction = Some(action2))

      val l2 = Seq((1, "doe", "john", 5)).toDF("id", "lastname", "firstname", "rating")
      srcDO.writeDataFrame(l2)(context2)
      val srcSubFeed2 = SparkSubFeed(None, "src1", Seq())
      action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
      action2.preInit(Seq(srcSubFeed), Seq())(context2.copy(phase = ExecutionPhase.Init))
      action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
      action2.exec(Seq(srcSubFeed2))(context2)

      // expectation dl_hash should not have null values
      assert(tgtDO.getDataFrame()(context2).where(col("dl_hash").isNull).count == 0)
    }
  }

}
