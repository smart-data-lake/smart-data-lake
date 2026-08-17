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
package io.smartdatalake.testutils

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions
import io.smartdatalake.definitions.{CdcChangeType, Environment}
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.plainScala.ScalaTestUtil.getCommonSubFeed
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.executionMode.DataFrameIncrementalMode
import io.smartdatalake.workflow.action.{CopyAction, HistorizeAction, NoDataToProcessWarning}
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.{ActionDAGRun, ActionPipelineContext, DataFrameSubFeed, ExecutionPhase}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * This trait defines tests for the behaviour of HistorizeAction. They can be used with various
 * output DataObject types to ensure consistent behaviour for e.g. Jdbc, DeltaLake, ...
 */
trait HistorizeActionBehaviour extends GenericTestTool {
  this: AnyFunSuite with Matchers with SmartDataLakeLogger =>

  implicit private val implicitLogger: Logger = logger

  import io.smartdatalake.testutils.plainScala.ScalaTestUtil.registerDataObject

  def defaultEngineConnection: Connection with EngineConnection

  /**
   * @param supportsColumnNamesWithUnderscorePrefix set to false for output DataObjects which can not write columns
   *                                                with a name starting with an underscore, e.g. JDBC tables where
   *                                                identifiers are not quoted in the generated SQL statements.
   */
  def historizeWithMergeMode(
                              createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
                              createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame,
                              tgtConnection: Option[Connection] = None,
                              supportsColumnNamesWithUnderscorePrefix: Boolean = true
                            ): Unit = {
    logger.debug(s"historizeWithMergeMode START: tgtConnection=$tgtConnection")

    test("historize load using merge") {

      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)
      instanceRegistry.register(defaultEngineConnection)

      implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(phase = ExecutionPhase.Exec)

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
      import helper.implicits._

      // prepare & start 1st load
      val refTimestamp1 = LocalDateTime.now()
      val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id)
      val context1 =
        ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec,
          currentAction = Some(action1))
      val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
      srcDO.writeDataFrame(l1, Seq())
      val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
      action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
      action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
      action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
      action1.exec(Seq(srcSubFeed))(context1)

      {
        val expected = Seq(("doe", "john", 5, Timestamp.valueOf(refTimestamp1), definitions.Environment.historizationUpperHorizonTimestamp))
          .toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame()
          .drop(Historization.historizeHashColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize 1st load mergeModeEnable", Seq())(actual)(expected)
        assert(resultat)
      }

      // prepare & start 2nd load
      val refTimestamp2 = LocalDateTime.now()
      val action2 = HistorizeAction("ha2", srcDO.id, tgtDO.id)
      val context2 =
        ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec,
          currentAction = Some(action2))
      val l2 = Seq(("doe", "john", 10)).toDF("lastname", "firstname", "rating")
      srcDO.writeDataFrame(l2, Seq())
      val srcSubFeed2 = ScalaSubFeed(None, "src1", Seq())
      action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
      action2.preInit(Seq(srcSubFeed2), Seq())(context2.copy(phase = ExecutionPhase.Init))
      action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
      action2.exec(Seq(srcSubFeed2))(context2)

      {
        val expected = Seq(
          ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
          ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), definitions.Environment.historizationUpperHorizonTimestamp)
        ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame()
          .drop(Historization.historizeHashColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize 2nd load mergeModeEnable", Seq())(actual)(expected)
        assert(resultat)
      }

      // prepare & start 3rd load with schema evolution
      val refTimestamp3 = LocalDateTime.now()
      val action3 = HistorizeAction("ha3", srcDO.id, tgtDO.id)
      val context3 =
        ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec,
          currentAction = Some(action3))
      val l3 = Seq(("doe", "john", 10, "test")).toDF("lastname", "firstname", "rating", "test")
      srcDO.writeDataFrame(l3, Seq())
      val srcSubFeed3 = ScalaSubFeed(None, "src1", Seq())
      action3.prepare(context3.copy(phase = ExecutionPhase.Prepare))
      action3.preInit(Seq(srcSubFeed3), Seq())(context3.copy(phase = ExecutionPhase.Init))
      action3.init(Seq(srcSubFeed3))(context3.copy(phase = ExecutionPhase.Init))
      logger.debug(s"exec action3: $action3")
      action3.exec(Seq(srcSubFeed3))(context3)

      {
        val expected = Seq(
          ("doe", "john", 5, null, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
          ("doe", "john", 10, null, Timestamp.valueOf(refTimestamp2), Timestamp.valueOf(refTimestamp3.minusNanos(1000000L))),
          ("doe", "john", 10, "test", Timestamp.valueOf(refTimestamp3), definitions.Environment.historizationUpperHorizonTimestamp)
        ).toDF("lastname", "firstname", "rating", "test", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame()
          .drop(Historization.historizeHashColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize 3rd load mergeModeEnable with schema evolution", Seq())(actual)(expected)
        assert(resultat)
      }
    }

    test("historize load using merge CDC") {

      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)
      instanceRegistry.register(defaultEngineConnection)

      implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(phase = ExecutionPhase.Exec)

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
      import helper.implicits._

      // prepare & start 1st load
      val refTimestamp1 = LocalDateTime.now()
      val action1 =
        HistorizeAction("ha", srcDO.id, tgtDO.id, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
      val context1 =
        ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec,
          currentAction = Some(action1))
      val l1 = Seq(("doe", "john", 5, "new"), ("pan", "peter", 5, "new")).toDF("lastname", "firstname", "rating", "operation")
      srcDO.writeDataFrame(l1, Seq())
      val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
      action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
      action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
      action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
      action1.exec(Seq(srcSubFeed))(context1)

      {
        val expected = Seq(
          ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), definitions.Environment.historizationUpperHorizonTimestamp),
          ("pan", "peter", 5, Timestamp.valueOf(refTimestamp1), definitions.Environment.historizationUpperHorizonTimestamp)
        ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame()
          .drop(Historization.historizeDummyColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize 1st load mergeModeEnable", Seq())(actual)(expected)
        assert(resultat)
      }

      // prepare & start 2nd load
      val refTimestamp2 = LocalDateTime.now()
      val action2 =
        HistorizeAction("ha2", srcDO.id, tgtDO.id, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
      val context2 =
        ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec,
          currentAction = Some(action2))
      val l2 = Seq(("doe", "john", 10, "updated"), ("pan", "peter", 5, "deleted")).toDF("lastname", "firstname", "rating", "operation")
      srcDO.writeDataFrame(l2, Seq())
      val srcSubFeed2 = ScalaSubFeed(None, "src1", Seq())
      action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
      action2.preInit(Seq(srcSubFeed2), Seq())(context2.copy(phase = ExecutionPhase.Init))
      action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
      action2.exec(Seq(srcSubFeed2))(context2)

      {
        val expected = Seq(
          ("doe", "john", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
          ("doe", "john", 10, Timestamp.valueOf(refTimestamp2), definitions.Environment.historizationUpperHorizonTimestamp),
          ("pan", "peter", 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L)))
        ).toDF("lastname", "firstname", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame()
          .drop(Historization.historizeDummyColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize 2nd load mergeModeEnable", Seq())(actual)(expected)
        assert(resultat)
      }

      // prepare & start 3rd load with schema evolution
      val refTimestamp3 = LocalDateTime.now()
      val action3 =
        HistorizeAction("ha3", srcDO.id, tgtDO.id, mergeModeCDCColumn = Some("operation"), mergeModeCDCDeletedValue = Some("deleted"))
      val context3 =
        ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp3), phase = ExecutionPhase.Exec,
          currentAction = Some(action3))
      val l3 = Seq(("doe", "john", 10, "test", "updated")).toDF("lastname", "firstname", "rating", "test", "operation")
      srcDO.writeDataFrame(l3, Seq())(context3)
      val srcSubFeed3 = ScalaSubFeed(None, "src1", Seq())
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
        val actual = tgtDO.getDataFrame()
          .drop(Historization.historizeDummyColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize 3rd load mergeModeEnable with schema evolution", Seq())(actual)(expected)
        assert(resultat)
      }
    }

    test("switch from incremental cdc historization to incremental historization on existing dataframe") {
      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)
      instanceRegistry.register(defaultEngineConnection)

      implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(phase = ExecutionPhase.Exec)

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("id")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
      import helper.implicits._

      // prepare & start 1st load
      val refTimestamp1 = LocalDateTime.now()
      val context1 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)
      val action1 = HistorizeAction("ha",
        inputId = srcDO.id,
        outputId = tgtDO.id,
        mergeModeCDCColumn = Some("operation"),
        mergeModeCDCDeletedValue = Some("deleted")
      )

      val l1 = Seq((1, "doe", "john", 5, "new")).toDF("id", "lastname", "firstname", "rating", "operation")
      srcDO.writeDataFrame(l1)(context1)
      val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
      action1.prepare(context1.copy(phase = ExecutionPhase.Prepare))
      action1.preInit(Seq(srcSubFeed), Seq())(context1.copy(phase = ExecutionPhase.Init))
      action1.init(Seq(srcSubFeed))(context1.copy(phase = ExecutionPhase.Init))
      action1.exec(Seq(srcSubFeed))(context1)

      // 1. expectation schema should not have dl_hash column
      assert(!tgtDO.getDataFrame().columns.map(_.toLowerCase).contains("dl_hash"))

      // prepare & start 2nd load
      val refTimestamp2 = LocalDateTime.now()
      val context2 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec)
      val action2 = HistorizeAction("ha",
        inputId = srcDO.id,
        outputId = tgtDO.id
      )

      val srcSubFeed2 = ScalaSubFeed(None, "src1", Seq())
      action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
      action2.preInit(Seq(srcSubFeed), Seq())(context2.copy(phase = ExecutionPhase.Init))
      action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
      try
        action2.exec(Seq(srcSubFeed))(context2)
      catch {
        // some DataObjects might detect that there is no new data to process
        case _: NoDataToProcessWarning => ()
      }

      // 2. expectation schema should have dl_hash column
      assert(tgtDO.getDataFrame().columns.map(_.toLowerCase).contains("dl_hash"))

    }

    test("historize CDC change events of standard cdc columns") {

      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)
      instanceRegistry.register(defaultEngineConnection)

      implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(phase = ExecutionPhase.Exec)

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("id")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
      import helper.implicits._

      // commit timestamps of the change events in the source system. They are independent of the runs reference
      // timestamp, as the source system committed the changes before SDLB read them.
      val commitTs1 = Timestamp.valueOf(LocalDateTime.now().minusDays(3))
      val commitTs2 = Timestamp.valueOf(LocalDateTime.now().minusDays(2))
      val commitTs3 = Timestamp.valueOf(LocalDateTime.now().minusDays(1))
      def previousTick(ts: Timestamp) = Timestamp.from(ts.toInstant.minusMillis(1))

      // prepare & start 1st load: initial snapshot of the source table, including a record deleted in the meantime
      val action1 = cdcHistorizeAction("ha", srcDO.id, tgtDO.id)
      val context1 = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(referenceTimestamp = Some(LocalDateTime.now()), phase = ExecutionPhase.Exec, currentAction = Some(action1))
      val l1 = Seq(
        (1, 5, CdcChangeType.read, commitTs1, 0),
        (2, 5, CdcChangeType.read, commitTs1, 1),
        (3, 5, CdcChangeType.delete, commitTs1, 2)
      ).toDF("id", "rating", cdcChangeTypeCol, cdcCommitTimestampCol, cdcChangeOrdinalCol)
      srcDO.writeDataFrame(l1, Seq())(context1)
      execCdcHistorizeAction(action1, context1)

      {
        // deleted records are not part of the initial history, and the CDC columns are not written to the output
        val expected = Seq(
          (1, 5, commitTs1, definitions.Environment.historizationUpperHorizonTimestamp),
          (2, 5, commitTs1, definitions.Environment.historizationUpperHorizonTimestamp)
        ).toDF("id", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame().drop(Historization.historizeDummyColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize cdc 1st load", Seq())(actual)(expected)
        assert(resultat)
      }

      // prepare & start 2nd load: an update (delivered as preimage and postimage), a delete, and a delete of a
      // record which never existed
      val action2 = cdcHistorizeAction("ha2", srcDO.id, tgtDO.id)
      val context2 = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(referenceTimestamp = Some(LocalDateTime.now()), phase = ExecutionPhase.Exec, currentAction = Some(action2))
      val l2 = Seq(
        (1, 5, CdcChangeType.updatePreimage, commitTs2, 0),
        (1, 10, CdcChangeType.updatePostimage, commitTs2, 1),
        (2, 5, CdcChangeType.delete, commitTs2, 2),
        (99, 0, CdcChangeType.delete, commitTs2, 3)
      ).toDF("id", "rating", cdcChangeTypeCol, cdcCommitTimestampCol, cdcChangeOrdinalCol)
      srcDO.writeDataFrame(l2, Seq())(context2)
      execCdcHistorizeAction(action2, context2)

      {
        // the preimage is ignored, as its value is already in the history. The deleted record 99 has no version to
        // close, so it is not added to the history.
        val expected = Seq(
          (1, 5, commitTs1, previousTick(commitTs2)),
          (1, 10, commitTs2, definitions.Environment.historizationUpperHorizonTimestamp),
          (2, 5, commitTs1, previousTick(commitTs2))
        ).toDF("id", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame().drop(Historization.historizeDummyColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize cdc 2nd load", Seq())(actual)(expected)
        assert(resultat)
      }

      // prepare & start 3rd load: several change events for the same primary key in one batch
      val action3 = cdcHistorizeAction("ha3", srcDO.id, tgtDO.id)
      val context3 = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(referenceTimestamp = Some(LocalDateTime.now()), phase = ExecutionPhase.Exec, currentAction = Some(action3))
      val l3 = Seq(
        (4, 1, CdcChangeType.insert, commitTs3, 0),
        (4, 2, CdcChangeType.updatePostimage, commitTs3, 1),
        (1, 20, CdcChangeType.updatePostimage, commitTs3, 2)
      ).toDF("id", "rating", cdcChangeTypeCol, cdcCommitTimestampCol, cdcChangeOrdinalCol)
      srcDO.writeDataFrame(l3, Seq())(context3)
      execCdcHistorizeAction(action3, context3)

      {
        // only the last change event per primary key is historized, e.g. record 4 is created with rating 2
        val expected = Seq(
          (1, 5, commitTs1, previousTick(commitTs2)),
          (1, 10, commitTs2, previousTick(commitTs3)),
          (1, 20, commitTs3, definitions.Environment.historizationUpperHorizonTimestamp),
          (2, 5, commitTs1, previousTick(commitTs2)),
          (4, 2, commitTs3, definitions.Environment.historizationUpperHorizonTimestamp)
        ).toDF("id", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame().drop(Historization.historizeDummyColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize cdc 3rd load", Seq())(actual)(expected)
        assert(resultat)
      }
    }

    test("historize CDC change events using the runs reference timestamp") {

      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)
      instanceRegistry.register(defaultEngineConnection)

      implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(phase = ExecutionPhase.Exec)

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("id")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
      import helper.implicits._

      val commitTs = Timestamp.valueOf(LocalDateTime.now().minusDays(3))

      // prepare & start 1st load
      val refTimestamp1 = LocalDateTime.now()
      val action1 = cdcHistorizeAction("ha", srcDO.id, tgtDO.id).copy(mergeModeCDCUseSourceTimestamp = false)
      val context1 = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec, currentAction = Some(action1))
      val l1 = Seq((1, 5, CdcChangeType.insert, commitTs, 0))
        .toDF("id", "rating", cdcChangeTypeCol, cdcCommitTimestampCol, cdcChangeOrdinalCol)
      srcDO.writeDataFrame(l1, Seq())(context1)
      execCdcHistorizeAction(action1, context1)

      // prepare & start 2nd load
      val refTimestamp2 = LocalDateTime.now()
      val action2 = cdcHistorizeAction("ha2", srcDO.id, tgtDO.id).copy(mergeModeCDCUseSourceTimestamp = false)
      val context2 = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec, currentAction = Some(action2))
      val l2 = Seq((1, 10, CdcChangeType.updatePostimage, commitTs, 0))
        .toDF("id", "rating", cdcChangeTypeCol, cdcCommitTimestampCol, cdcChangeOrdinalCol)
      srcDO.writeDataFrame(l2, Seq())(context2)
      execCdcHistorizeAction(action2, context2)

      {
        // the validity of the versions is defined by the reference timestamp of the runs, not by the commit timestamp
        val expected = Seq(
          (1, 5, Timestamp.valueOf(refTimestamp1), Timestamp.valueOf(refTimestamp2.minusNanos(1000000L))),
          (1, 10, Timestamp.valueOf(refTimestamp2), definitions.Environment.historizationUpperHorizonTimestamp)
        ).toDF("id", "rating", "dl_ts_captured", "dl_ts_delimited")
        val actual = tgtDO.getDataFrame().drop(Historization.historizeDummyColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize cdc with reference timestamp", Seq())(actual)(expected)
        assert(resultat)
      }
    }

    // this test writes the CDC columns to the output DataObject, as they are treated as normal attributes
    if (supportsColumnNamesWithUnderscorePrefix) test("historize CDC change events with mergeModeCDCAutoDetect disabled") {

      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)
      instanceRegistry.register(defaultEngineConnection)

      implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(phase = ExecutionPhase.Exec)

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("id")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
      import helper.implicits._

      val commitTs = Timestamp.valueOf(LocalDateTime.now().minusDays(3))

      // prepare & start load with auto detection of the standard CDC columns disabled
      val refTimestamp1 = LocalDateTime.now()
      val action1 = HistorizeAction("ha", srcDO.id, tgtDO.id, mergeModeCDCAutoDetect = false)
      val context1 = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec, currentAction = Some(action1))
      val l1 = Seq((1, 5, CdcChangeType.insert, commitTs, 0))
        .toDF("id", "rating", cdcChangeTypeCol, cdcCommitTimestampCol, cdcChangeOrdinalCol)
      srcDO.writeDataFrame(l1, Seq())(context1)
      execCdcHistorizeAction(action1, context1)

      {
        // the CDC columns are historized as normal attributes, and the reference timestamp defines the validity
        val expected = Seq((1, 5, CdcChangeType.insert, commitTs, 0, Timestamp.valueOf(refTimestamp1),
          definitions.Environment.historizationUpperHorizonTimestamp))
          .toDF("id", "rating", cdcChangeTypeCol, cdcCommitTimestampCol, cdcChangeOrdinalCol, "dl_ts_captured",
            "dl_ts_delimited")
        val actual = tgtDO.getDataFrame().drop(Historization.historizeHashColName)
        val resultat = expected.isEqual(actual)
        if (!resultat) printFailedTestResultGdf("historize cdc without auto detection", Seq())(actual)(expected)
        assert(resultat)
      }
    }
  }

  private def cdcChangeTypeCol = Environment.cdcChangeTypeColumnName
  private def cdcCommitTimestampCol = Environment.cdcCommitTimestampColumnName
  private def cdcChangeOrdinalCol = Environment.cdcChangeOrdinalColumnName

  /**
   * HistorizeAction for input using SDLBs standard CDC columns. Note that no CDC specific configuration is needed.
   */
  private def cdcHistorizeAction(actionId: String, srcId: DataObjectId, tgtId: DataObjectId)(implicit
      instanceRegistry: InstanceRegistry
  ) = HistorizeAction(actionId, srcId, tgtId)

  private def execCdcHistorizeAction(action: HistorizeAction, context: ActionPipelineContext): Unit = {
    val srcSubFeed = ScalaSubFeed(None, action.inputId, Seq())
    action.prepare(context.copy(phase = ExecutionPhase.Prepare))
    action.preInit(Seq(srcSubFeed), Seq())(context.copy(phase = ExecutionPhase.Init))
    action.init(Seq(srcSubFeed))(context.copy(phase = ExecutionPhase.Init))
    action.exec(Seq(srcSubFeed))(context)
  }

  def historizeIncrementalPipeline(
      createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (
          String,
          Option[Seq[String]],
          InstanceRegistry
      ) => TransactionalTableDataObject with CanMergeDataFrame with CanCreateIncrementalOutput,
      tgtConnection: Option[Connection] = None
  ): Unit =

    test("historize load mergeModeEnable and copy incremental action") {
      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)
      instanceRegistry.register(defaultEngineConnection)

      implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(phase = ExecutionPhase.Exec)

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgt1DO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
      val tgt2DO = registerDataObject(createTgtDataObject("tgt2", Some(Seq("lastname", "firstname")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgt1DO))
      import helper.implicits._

      // define DAG
      val context1: ActionPipelineContext = context.copy(referenceTimestamp = Some(LocalDateTime.now()))
      val action1 = HistorizeAction("ha", srcDO.id, tgt1DO.id)
      instanceRegistry.register(action1)
      val action2 =
        CopyAction("ca", tgt1DO.id, tgt2DO.id, executionMode = Some(DataFrameIncrementalMode("dl_ts_captured")))(instanceRegistry)
      instanceRegistry.register(action2)
      val stateStore = MemoryDagRunStateStore()
      val dag = ActionDAGRun(Seq(action1, action2), stateStore = Some(stateStore))(context1)

      // start first load
      val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
      srcDO.writeDataFrame(l1, Seq())
      dag.prepare(context1.copy(phase = ExecutionPhase.Prepare))
      dag.init(context1.copy(phase = ExecutionPhase.Init))
      val r1 = dag.exec(context1)

      assert(tgt1DO.getDataFrame().count == 1)
      assert(!tgt1DO.getDataFrame().columns.map(_.toLowerCase).contains("dl_operation"))
      assert(!tgt2DO.getDataFrame().columns.map(_.toLowerCase).contains("dl_operation"))
      assert(!r1.head.isSkipped)

      // start second load -> updated record
      val l2 = Seq(("doe", "john", 10)).toDF("lastname", "firstname", "rating")
      srcDO.writeDataFrame(l2, Seq())
      val context2: ActionPipelineContext = context.copy(referenceTimestamp = Some(LocalDateTime.now()))
      dag.prepare(context2.copy(phase = ExecutionPhase.Prepare))
      dag.init(context2.copy(phase = ExecutionPhase.Init))
      val r2 = dag.exec(context2)

      assert(tgt1DO.getDataFrame().count == 2)
      assert(tgt2DO.getDataFrame().count == 1)
      assert(!r2.head.isSkipped)

      // start third load with same record again -> should be skipped, because merge mode should detect that there is no change, so there is no changed record in tgt1
      val context3: ActionPipelineContext = context.copy(referenceTimestamp = Some(LocalDateTime.now()))
      dag.prepare(context3.copy(phase = ExecutionPhase.Prepare))
      dag.init(context3.copy(phase = ExecutionPhase.Init))
      val r3 = dag.exec(context3)

      assert(tgt1DO.getDataFrame().count == 2)
      assert(tgt2DO.getDataFrame().count == 1)
      assert(r3.head.isSkipped)
    }


  def activateMergeMode(
                         createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
                         createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame,
                         tgtConnection: Option[Connection] = None
                       ): Unit =
    test("activate merge mode on legacy full historized dataframe, no null dl_hash") {

      implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
      tgtConnection.foreach(instanceRegistry.register)
      instanceRegistry.register(defaultEngineConnection)

      implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
        .copy(phase = ExecutionPhase.Exec)

      // setup DataObjects
      val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
      val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("id")), instanceRegistry))
      val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
      import helper.col
      import helper.implicits._

      // prepare & start 1st load without merge mode
      val refTimestamp1 = LocalDateTime.now()
      val context1 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp1), phase = ExecutionPhase.Exec)

      // create a legacy historized dataframe without dl_hash column, and write to target
      val l1 = Seq((1, "doe", "john", 5, Timestamp.valueOf(refTimestamp1), Environment.historizationUpperHorizonTimestamp))
        .toDF("id", "lastname", "firstname", "rating", Environment.capturedColumnName, Environment.delimitedColumnName)
      tgtDO.writeDataFrame(l1)(context1)

      // 1. expectation schema should not have dl_hash column
      assert(!tgtDO.getDataFrame().columns.contains("dl_hash"))

      // prepare & start load with merge mode and migrate existing data to merge mode
      val refTimestamp2 = LocalDateTime.now()
      val action2 = HistorizeAction("ha2", inputId = srcDO.id, outputId = tgtDO.id)
      val context2 = ScalaTestUtil.getDefaultActionPipelineContext.copy(referenceTimestamp = Some(refTimestamp2), phase = ExecutionPhase.Exec,
        currentAction = Some(action2))

      val l2 = Seq((1, "doe", "john", 4)).toDF("id", "lastname", "firstname", "rating")
      srcDO.writeDataFrame(l2)(context2)
      val srcSubFeed2 = ScalaSubFeed(None, "src1", Seq())
      action2.prepare(context2.copy(phase = ExecutionPhase.Prepare))
      action2.preInit(Seq(srcSubFeed2), Seq())(context2.copy(phase = ExecutionPhase.Init))
      action2.init(Seq(srcSubFeed2))(context2.copy(phase = ExecutionPhase.Init))
      action2.exec(Seq(srcSubFeed2))(context2)

      // expectation dl_hash should not have null values
      assert(tgtDO.getDataFrame().where(col("dl_hash").isNull).count == 0)
    }


}
