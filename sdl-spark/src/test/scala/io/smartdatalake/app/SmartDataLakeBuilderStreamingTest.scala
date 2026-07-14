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
package io.smartdatalake.app

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{InstanceRegistry, SdlConfigObject}
import io.smartdatalake.definitions.{Environment, SDLSaveMode}
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.util.LogUtils.debugLog
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.GetSession.loggEnv
import io.smartdatalake.util.spark.dataset.Quality
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action._
import io.smartdatalake.workflow.action.executionMode.{PartitionDiffMode, SparkStreamingMode}
import io.smartdatalake.workflow.action.generic.transformer.SQLDfTransformer
import io.smartdatalake.workflow.action.spark.customlogic.SparkUDFCreatorConfig
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfTransformer
import io.smartdatalake.workflow.connection.SparkClassicConnection
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed.getSparkSession
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema}
import io.smartdatalake.workflow.dataobject.CsvFileDataObject
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext, ExecutionPhase, HadoopFileActionDAGRunStateStore}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamingQueryListener}
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

import java.nio.file
import _root_.scala.util.{Failure, Success, Try}

class SmartDataLakeBuilderStreamingTest extends AnyFunSuite with Quality with SmartDataLakeLogger with BeforeAndAfter {
  @transient implicit private lazy val loggImpl: Logger = logger
  protected implicit val session: SparkSession = SparkTestUtil.session

  import session.implicits._

  private val tempDir = file.Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  val statePath = "target/streamingStateTest/"
  val checkpointPath = "target/streamingCheckpointTest/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))

  private val sdlb = DefaultSmartDataLakeBuilder
  implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry

  val sparkClassicConnection = SparkClassicConnection(
    id = Environment.defaultEngineConnectionId,
    master = Some("local"),
    sparkUDFs = Some(Map("udfAddX" -> SparkUDFCreatorConfig(className = classOf[TestUDFAddXCreator].getName, options = Some(Map("x" -> "1")))))
  )

  private def getRecordsWritten(info: RuntimeInfo): Long = {
    val metricsMap: MetricsMap = info.results.head.metrics.getOrElse(Map())
    metricsMap.get("records_written").map {
      case l: Long => l
      case bi: BigInt => bi.toLong  // json's deserializes integers as BigInt when reading from state store
      case other => other.toString.toLong
    }.getOrElse({
      logger.error("getRecordsWritten: key records_written not found in metrics. returning -1")
      logger.error(s"getRecordsWritten(${info.executionId}, in: ${info.inputIds.mkString(",")}, out: ${info.outputIds.mkString(",")}):" +
        s" metricsMap has ${metricsMap.size} entries: $metricsMap")
      -1L
    })
  }

  loggEnv
  debugLog(s"SmartDataLakeBuilderStreamingTest: tempPath = $tempPath")

  private val dfSrc2 = Seq(("20190101", "company", "olmo", "-", 10)) // second partition 20190101
    .toDF("dt", "type", "lastname", "firstname", "rating")

  before {
    instanceRegistry.clear()
    instanceRegistry.register(sparkClassicConnection)
  }

  after {
    // Remove listeners FIRST so that stopping lingering queries below does not cause
    // SparkStreamingQueryListener.onQueryTerminated to set stopStreamingGracefully=true
    // asynchronously (after we already reset it), which would corrupt the next test.
    session.streams.listListeners().foreach(session.streams.removeListener)
    // Now stop any lingering streaming queries with no listener left to set stopStreamingGracefully
    session.streams.active.foreach(q => try { q.stop() } catch { case _: Exception => () })
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false // reset stopping gracefully
  }

  test("sdlb streaming run with normal action, executionMode=PartitionDiffMode") {

    // init sdlb
    val appName = "sdlb-normal"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val contextExec: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

    // setup DataObjects
    // source table has partitions columns dt and type
    val srcDO = MockSparkDataObject("src1", partitions = Seq("dt", "type")).register
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = MockSparkDataObject(id = "tgt1", partitions = Seq("dt", "type"),
      primaryKey = Some(Seq("lastname", "firstname"))).register

    // fill src table with first partition
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc1)

    // start streaming dag run
    // use only first partition col (dt) for partition diff mode
    val action1 = CopyAction(id = "a", inputId = srcDO.id, outputId = tgt1DO.id,
      executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))),
      metadata = Some(ActionMetadata(feed = Some(feedName))),
      transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, udfAddX(rating) rating from src1")))
    )
    instanceRegistry.register(action1)

    // create state listener to control execution
    val stateListener = new StateListener with SmartDataLakeLogger {
      private var dfWritten = false

      override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[ActionId]): Unit = {
        assert(state.runId == context.executionId.runId && state.attemptId == context.executionId.attemptId)
        logger.info(s"Received metrics for runId=${state.runId} attemptId=${state.attemptId} final=${state.isFinal}")
        // check results after runId=1
        if (state.isFinal && state.runId == 1) {
          // check results
          assert(tgt1DO.listPartitions.map(_.apply("dt")) == Seq("20180101"))
          assert(tgt1DO.getSparkDataFrame().select($"lastname").as[String].collect().toSeq == Seq("doe"))
        }
        // add additional source partition in runId=2 for runId=3
        if (state.isFinal && state.runId == 2 && !dfWritten) {
          dfWritten = true
          // add some more data
          logger.info("adding more data")
          val dfSrc2 = Seq(("20180102", "company", "olmo", "-", 10)) // second partition 20190101
            .toDF("dt", "type", "lastname", "firstname", "rating")
          srcDO.writeSparkDataFrame(dfSrc2, Seq())
        }
        // stop after runId=3
        if (state.isFinal && state.runId >= 3) {
          Environment.stopStreamingGracefully = true
        }
      }
    }

    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"),
      feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath), streaming = true)
    Environment._additionalStateListeners = Seq(stateListener)
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false
    Environment._additionalStateListeners = Seq()

    debugLog("check data after streaming is terminated")
    assert(tgt1DO.listPartitions.map(_.apply("dt")) == Seq("20180101", "20180102"))
    assert(tgt1DO.getSparkDataFrame().select($"rating").as[Int].collect().toSeq == Seq(6, 11)) // +1 because of udfAddX

    // check state after streaming is terminated
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateId = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateId)
      assert(runState.runId >= 3)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.view.mapValues(_.state).toMap
      val expectedActionsState = Map((action1.id, RuntimeEventState.SKIPPED))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.partitionValues.isEmpty)
    }
  }

  ignore("sdlb streaming run with streaming action asynchronously, csv files") {

    //TODO: Fix this test.
    // Sometimes it succeeds but mostly it fails with  Set("20180101") did not equal Set("20180101", "20190101")
    // Seldom it fails with ArraySeq(6) did not equal List(6, 11)
    // Why is the test not deterministic?

    // init sdlb
    val appName = "sdlb-streaming"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(tempPath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(checkpointPath), doWarn = false)
    implicit val contextExec: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

    // setup DataObjects
    // source has partitions columns dt and type
    val srcDOPath = s"$tempPath/src1"
    val srcDO = CsvFileDataObject(id = "src1", path = srcDOPath, partitions = Seq("dt", "type"),
      schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject(id = "tgt1", path = tempPath + "/tgt1", partitions = Seq("dt", "type"), saveMode = SDLSaveMode.Overwrite
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt1DO)

    debugLog("fill src with first files")
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc1, PartitionValues.fromDataFrame(SparkDataFrame(dfSrc1.select($"dt", $"type"))))
    srcDO.getSparkDataFrame().createdLog("srcDO", showRows = true)

    debugLog("prepare streaming action")
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id,
      executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))),
      metadata = Some(ActionMetadata(feed = Some(feedName))),
      transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, udfAddX(rating) rating from src1"))))
    instanceRegistry.register(action1)

    debugLog("streaming event listener will add data and stop streaming after second data batch is processed")
    val testStreamingQueryListener = new StreamingQueryListener {
      private var dfWritten = false
      private var batchAfterWriteProcessed = false
      private val actionRegex = s"Action~(${SdlConfigObject.idRegexStr})".r.unanchored

      override def onQueryIdle(event: StreamingQueryListener.QueryIdleEvent): Unit = {
        debugLog(s"onQueryIdle: batchAfterWriteProcessed=$batchAfterWriteProcessed")
        // TODO: Adapt comment to Spark 4!
        // In Spark 3.5+, idle fires when no new data available instead of onQueryProgress
        // Only stop after the batch that processes the newly written data has completed
        if (batchAfterWriteProcessed) {
          logger.info("onQueryIdle: stopping streaming query after idle")
          session.streams.active.find(_.runId == event.runId).get.stop()
        }
        // else: data was recently written, wait for next trigger to pick it up
      }

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        debugLog(s"onQueryStarted called")
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val prog = event.progress
        logger.info(s"onQueryProgress: progress ${prog.batchId} ${prog.name}." +
          s" batchAfterWriteProcessed=$batchAfterWriteProcessed , dfWritten=$dfWritten , numInputRows=${prog.numInputRows}")
        prog.name match {
          case actionRegex(_) =>
            prog.batchId match {
              case 0 if !dfWritten =>
                dfWritten = true
                debugLog("onQueryProgress: adding some more data")
                srcDO.writeSparkDataFrame(dfSrc2)(contextExec)
                srcDO.getSparkDataFrame()(contextExec).createdLog("srcDO", showRows = true)
              case x if x > 2 && batchAfterWriteProcessed =>
                debugLog(s"onQueryProgress: x=$x stopping streaming query")
                session.streams.active.find(_.name == prog.name).get.stop()
              case x if x > 0 && dfWritten && !batchAfterWriteProcessed && prog.numInputRows > 0 =>
                debugLog(s"onQueryProgress: x=$x setting batchAfterWriteProcessed to true")
                batchAfterWriteProcessed = true
              case x => debugLog(s"onQueryProgress: x=$x doing nothing")
            }
        }
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        debugLog(s"onQueryTerminated called")
      }
    }
    session.streams.addListener(testStreamingQueryListener)

    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName,
      applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    debugLog(s"start run sdlConfig=$sdlConfig")
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false

    debugLog("check data after streaming is terminated")
    val tgt1DOdf = tgt1DO.getSparkDataFrame()
    tgt1DOdf.debLog(dsName = "tgt1DOdf", showRows = true)
    Try {
      assert(tgt1DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101", "20190101"))
      assert(tgt1DOdf.select($"rating").as[Int].collect().toSeq == Seq(6, 11)) // +1 because of udfAddX
    } match {
      case Success(_) =>
      case Failure(e) => println()
        logger.error("!!! ASSERTION FAILED !!!")
        logger.error(s"sdlConfig: ${sdlConfig.toDebugString}")
        logger.error(s"action1  : ${action1.toDebugString}")
        logger.error(s"srcDO    : ${srcDO.toDebugString}")
        logger.error(s"content of $srcDOPath: ${new java.io.File(srcDOPath).list.mkString(", ")}")
        srcDO.getSparkDataFrame().debLog(dsName = "src1", showRows = true)
        tgt1DOdf.debLog(dsName = "tgt1DOdf", showRows = true)
      throw e
    }
    debugLog(s"action1.runtimeData: ${action1.runtimeData}")
    debugLog(s"${action1.runtimeData.executions.length} action1.runtimeData.executions: " +
      s"${action1.runtimeData.executions.mkString(",")}")

    val action1InfoSdl1: RuntimeInfo = action1.getRuntimeInfo(Some(SDLExecutionId(1))).get
    debugLog(s"action1InfoSdl1 = $action1InfoSdl1")
    debugLog(s"${action1InfoSdl1.results.length} action1InfoSdl1.results = ${action1InfoSdl1.results.mkString(",")}")
    assert(action1InfoSdl1.state == RuntimeEventState.SUCCEEDED) // State for SDL execution 1 is reported as SUCCEEDED by streaming action
    assert(getRecordsWritten(action1InfoSdl1) == 1)

    val action1InfoSdl2 = action1.getRuntimeInfo(Some(SDLExecutionId(2))).get
    assert(action1InfoSdl2.state == RuntimeEventState.STREAMING) // State for SDL execution 2 is reported as STREAMING by streaming action

    val action1InfoStream1 = action1.getRuntimeInfo(Some(SparkStreamingExecutionId(0)))
    assert(action1InfoStream1.isDefined)
    assert(action1InfoStream1.get.state == RuntimeEventState.SUCCEEDED)
    assert(getRecordsWritten(action1InfoStream1.get) == 1)

    val action1InfoStream2 = action1.getRuntimeInfo(Some(SparkStreamingExecutionId(1)))
    assert(action1InfoStream2.isDefined)
    assert(action1InfoStream2.get.state == RuntimeEventState.SUCCEEDED)
    /* TODO: This assert fails every now and then. No Idea why.
    2026-06-09 09:43:11 ERROR SmartDataLakeBuilderStreamingTest - getRecordsWritten: key records_written not found in metrics. returning -1 [ScalaTest-run-running-SmartDataLakeBuilderStreamingTest]
    2026-06-09 09:43:11 ERROR SmartDataLakeBuilderStreamingTest - getRecordsWritten(1, in: DataObject~src1, out: DataObject~tgt1): metricsMap has 2 entries: Map(no_data -> false, batch_duration -> 2078) [ScalaTest-run-running-SmartDataLakeBuilderStreamingTest]
    -1 did not equal 1
    ScalaTestFailureLocation: io.smartdatalake.app.SmartDataLakeBuilderStreamingTest at (SmartDataLakeBuilderStreamingTest.scala:299)
    Expected :1
    Actual   :-1 */
    assert(getRecordsWritten(action1InfoStream2.get) == 1)

    debugLog("check state after streaming is terminated")

    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateId = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateId)
      debugLog("only one SDL run executed (streaming action is asynchronous)")
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.view.mapValues(s => (s.executionId, s.state)).toMap
      val expectedActionsState = Map((action1.id, (SDLExecutionId(1), RuntimeEventState.SUCCEEDED))) // State for SDL execution 1 is reported as SUCCEEDED by streaming action
      assert(resultActionsState == expectedActionsState)
      assert(getRecordsWritten(runState.actionsState(action1.id)) == 1)
    }
  }

  test("sdlb streaming run with synchronously and asynchronously streaming action, csv files") {

    // init sdlb
    val appName = "sdlb-streaming2"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(tempPath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(checkpointPath), doWarn = false)
    implicit val contextExec: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject("src1", tempPath + "/src1", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject("tgt1", tempPath + "/tgt1", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt1DO)
    // second table has partitions columns dt and type (same as source)
    val tgt2DO = CsvFileDataObject("tgt2", tempPath + "/tgt2", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt2DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc1)

    // prepare partition diff action
    val actionA = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, rating from src1"))))
    // prepare streaming action
    val actionB = CopyAction("b", tgt1DO.id, tgt2DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, udfAddX(rating) rating from tgt1"))))
    instanceRegistry.register(Seq(actionA, actionB))

    // streaming event listener will add data and stop streaming after 3 micro-batches
    val testStreamingQueryListener = new StreamingQueryListener {
      private var dfWritten = false
      private val actionRegex = s"Action~(${SdlConfigObject.idRegexStr})".r.unanchored

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        logger.info(s"onQueryProgress: progress ${event.progress.batchId} ${event.progress.name}")
        event.progress.name match {
          case actionRegex(_) =>
            event.progress.batchId match {
              case 0 if !dfWritten =>
                dfWritten = true
                // add some more data
                logger.info("adding more data")
                val dfSrc2 = Seq(("20190101", "company", "olmo", "-", 10)) // second partition 20190101
                  .toDF("dt", "type", "lastname", "firstname", "rating")
                srcDO.writeSparkDataFrame(dfSrc2, Seq())
              case x if x > 0 && event.progress.numInputRows > 0 =>
                // stop streaming gracefully when second data partition was processed
                logger.info("stopping streaming gracefully")
                Environment.stopStreamingGracefully = true
              case _ => ()
            }
        }
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    }
    session.streams.addListener(testStreamingQueryListener)

    // start run
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName,
      applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false

    // check data after streaming is terminated
    assert(tgt1DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101", "20190101"))
    assert(tgt2DO.getSparkDataFrame().select($"rating").as[Int].collect().toSeq == Seq(6, 11)) // +1 because of udfAddX
  }

  test("sdlb streaming recovery, synchronous action failing before asynchronously streaming action") {

    // init sdlb
    val appName = "sdlb-streaming3"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(tempPath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(checkpointPath), doWarn = false)
    implicit val contextExec: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject("src1", tempPath + "/src1", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject("tgt1", tempPath + "/tgt1", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt1DO)
    // second table has partitions columns dt and type (same as source)
    val tgt2DO = CsvFileDataObject("tgt2", tempPath + "/tgt2", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt2DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc1)

    // prepare partition diff action
    val actionAFail = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassSparkDfTransformer(className = classOf[RuntimeFailTransformer].getName)))
    val actionA = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, rating from src1"))))
    // prepare streaming action
    val actionB = CopyAction("b", tgt1DO.id, tgt2DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, udfAddX(rating) rating from tgt1"))))

    // streaming event listener will add data and stop streaming after 3 micro-batches
    val testStreamingQueryListener = new StreamingQueryListener {
      private var dfWritten = false
      private val actionRegex = s"Action~(${SdlConfigObject.idRegexStr})".r.unanchored

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        logger.info(s"progress ${event.progress.batchId} ${event.progress.name}")
        event.progress.name match {
          case actionRegex(_) =>
            event.progress.batchId match {
              case 0 if !dfWritten =>
                dfWritten = true
                // add some more data
                logger.info("adding more data")
                srcDO.writeSparkDataFrame(dfSrc2, Seq())
              case x if x > 0 && event.progress.numInputRows > 0 =>
                // stop streaming gracefully when second data partition was processed
                logger.info("stopping streaming gracefully")
                Environment.stopStreamingGracefully = true
              case _ => ()
            }
        }
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    }
    session.streams.addListener(testStreamingQueryListener)

    // start run failing actionA
    instanceRegistry.register(Seq(actionAFail, actionB))
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    intercept[TaskFailedException](sdlb.run(sdlConfig))
    Environment.stopStreamingGracefully = false

    // start recovery run succeeding
    instanceRegistry.remove(actionAFail.id)
    instanceRegistry.register(actionA)
    actionAFail.reset
    actionA.reset
    actionB.reset
    session.streams.resetTerminated() // reset terminated streaming query list
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false

    // check data after streaming is terminated
    assert(tgt1DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101", "20190101"))
    assert(tgt2DO.getSparkDataFrame().select($"rating").as[Int].collect().toSeq == Seq(6, 11)) // +1 because of udfAddX
  }

  ignore("sdlb spark streaming failure, synchronous action before asynchronously streaming action, asynchronous action failing after first run") {
    //TODO: Fix this test. It fails with
    // Expected exception org.apache.spark.sql.streaming.StreamingQueryException to be thrown, but no exception was thrown

    // init sdlb
    val appName = "sdlb-streaming4"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(tempPath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(checkpointPath), doWarn = false)
    implicit val contextExec: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject("src1", tempPath + "/src1", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject("tgt1", tempPath + "/tgt1", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt1DO)
    // second table has partitions columns dt and type (same as source)
    val tgt2DO = CsvFileDataObject("tgt2", tempPath + "/tgt2", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt2DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc1)
    srcDO.getSparkDataFrame().createdLog("srcDO", showRows = true)

    // prepare partition diff action
    val actionA = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, rating from src1"))))
    // prepare streaming action
    val actionB = CopyAction("b", tgt1DO.id, tgt2DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, udfAddX(rating) rating from tgt1"))))

    // streaming event listener will add data and stop streaming after 3 micro-batches
    val testStreamingQueryListener = new StreamingQueryListener {
      private var dfWritten = false
      private val actionRegex = s"Action~(${SdlConfigObject.idRegexStr})".r.unanchored

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        logger.info(s"progress ${event.progress.batchId} ${event.progress.name}")
        event.progress.name match {
          case actionRegex(_) =>
            event.progress.batchId match {
              case 0 if !dfWritten =>
                dfWritten = true
                // add some more data which will fail streaming query (udfAddX fails if input=999)
                logger.info("adding more data")
                val dfSrc2 = Seq(("20190101", "company", "olmo", "-", 999)) // second partition 20190101
                  .toDF("dt", "type", "lastname", "firstname", "rating")
                srcDO.writeSparkDataFrame(dfSrc2, Seq())
            }
        }
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    }
    session.streams.addListener(testStreamingQueryListener)

    // start run, actionB will fail after first runId
    instanceRegistry.register(Seq(actionA, actionB))
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName,
      applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    debugLog(s"sdlConfig = $sdlConfig")
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    intercept[StreamingQueryException](sdlb.run(sdlConfig))
    Environment.stopStreamingGracefully = false
  }

  test("sdlb streaming recovery, asynchronously action failing before synchronous streaming action") {

    // init sdlb
    val appName = "sdlb-streaming5"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(tempPath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(checkpointPath), doWarn = false)
    implicit val contextExec: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject("src1", tempPath + "/src1", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject("tgt1", tempPath + "/tgt1", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt1DO)
    // second table has partitions columns dt and type (same as source)
    val tgt2DO = CsvFileDataObject("tgt2", tempPath + "/tgt2", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt2DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc1)
    srcDO.getSparkDataFrame().createdLog("srcDO", showRows = true)

    // prepare streaming action
    val actionAFail = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassSparkDfTransformer(className = classOf[RuntimeFailTransformer].getName)))
    val actionA = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, rating from src1"))))
    // prepare partition diff action
    val actionB = CopyAction("b", tgt1DO.id, tgt2DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, udfAddX(rating) rating from tgt1"))))

    // start run failing actionA
    instanceRegistry.register(Seq(actionAFail, actionB))
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    intercept[TaskFailedException](sdlb.run(sdlConfig))
    Environment.stopStreamingGracefully = false

    // create state listener for controlling execution
    val stateListener: StateListener with SmartDataLakeLogger = new StateListener with SmartDataLakeLogger {
      var dfSrc2Written = false

      override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[ActionId]): Unit = {
        assert(state.runId == context.executionId.runId && state.attemptId == context.executionId.attemptId)
        logger.info(s"Received metrics for runId=${state.runId} attemptId=${state.attemptId} final=${state.isFinal}")
        // add additional source partition for runId=2
        if (state.isFinal && state.runId == 2 && !dfSrc2Written) {
          dfSrc2Written = true
          logger.info("adding more data")
          val dfSrc2 = Seq(("20180102", "company", "olmo", "-", 10)) // second partition 20190101
            .toDF("dt", "type", "lastname", "firstname", "rating")
          srcDO.writeSparkDataFrame(dfSrc2, Seq())
        }
        // stop after runId=3
        if (state.isFinal && state.runId >= 3) {
          Environment.stopStreamingGracefully = true
        }
      }
    }

    // start recovery run succeeding
    instanceRegistry.remove(actionAFail.id)
    instanceRegistry.register(actionA)
    actionAFail.reset
    actionA.reset
    actionB.reset
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment._additionalStateListeners = Seq(stateListener)
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false
    Environment._additionalStateListeners = Seq()

    // check data after streaming is terminated
    assert(tgt2DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101", "20180102"))
    assert(tgt2DO.getSparkDataFrame().select($"rating").as[Int].collect().toSeq == Seq(6, 11)) // +1 because of udfAddX
  }

  ignore("sdlb streaming restart, synchronous action skipped before asynchronously streaming action") {
    //TODO: Fix this test. It fails with
    // Task b failed. Root cause is 'IllegalArgumentException:
    //  Cannot start query with name sdlb-streaming6 Action~b writing DataObject~tgt2
    //  as a query with that name is already active in this SparkSession'

    // init sdlb
    val appName = "sdlb-streaming6"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(tempPath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    HdfsUtil.deleteFiles(new Path(checkpointPath), doWarn = false)
    implicit val contextExec: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject("src1", tempPath + "/src1", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject("tgt1", tempPath + "/tgt1", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt1DO)
    // second table has partitions columns dt and type (same as source)
    val tgt2DO = CsvFileDataObject("tgt2", tempPath + "/tgt2", partitions = Seq("dt", "type")
      , schema = Some(SparkSchema(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int"))))
    instanceRegistry.register(tgt2DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc1)
    srcDO.getSparkDataFrame().createdLog("srcDO", showRows = true)

    // prepare partition diff action
    val actionA = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, rating from src1"))))
    // prepare streaming action
    val actionB = CopyAction("b", tgt1DO.id, tgt2DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = Some("select dt, type, lastname, firstname, udfAddX(rating) rating from tgt1"))))

    // streaming event listener will add data and stop streaming after 3 micro-batches
    val testStreamingQueryListener: StreamingQueryListener = new StreamingQueryListener {
      var dfSrc2Written = false
      private val actionRegex = s"Action~(${SdlConfigObject.idRegexStr})".r.unanchored

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        logger.info(s"progress ${event.progress.batchId} ${event.progress.name}")
        event.progress.name match {
          case actionRegex(_) =>
            event.progress.batchId match {
              case 0 if !dfSrc2Written =>
                dfSrc2Written = true
                // add some more data
                logger.info("adding more data")
                srcDO.writeSparkDataFrame(dfSrc2, Seq())
              case x if x > 0 && event.progress.numInputRows > 0 =>
                // stop streaming gracefully when second data partition was processed
                logger.info("stopping streaming gracefully")
                Environment.stopStreamingGracefully = true
              case _ => ()
            }
        }
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    }
    session.streams.addListener(testStreamingQueryListener)

    debugLog(s"start run failing actionA = $actionA")
    instanceRegistry.register(Seq(actionA, actionB))
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName,
      applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    debugLog(s"sdlConfig = $sdlConfig")
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false

    debugLog("check data after streaming is terminated")
    assert(tgt1DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101", "20190101"))

    debugLog("restart run")
    val currentRunId = actionA.runtimeData.currentExecutionId.get.asInstanceOf[SDLExecutionId].runId
    session.streams.resetTerminated() // reset terminated streaming query list
    actionA.reset
    actionB.reset
    debugLog("this listener adds more data after first skipped run")
    Environment._additionalStateListeners = Seq(new PartitionStreamingTestStateListener2(currentRunId + 1))
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false
    Environment._additionalStateListeners = Seq()

    debugLog("check data after streaming is terminated")
    assert(tgt1DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101", "20180102", "20190101"))
  }

}

/**
 * Add more data after given runId
 *
 * @param runIdToAddData
 */
class PartitionStreamingTestStateListener2(runIdToAddData: Int) extends StateListener with SmartDataLakeLogger {
  var srcDO: CsvFileDataObject = _
  private var dfWritten = false

  override def init(context: ActionPipelineContext): Unit = {
    srcDO = Environment.instanceRegistry.get[CsvFileDataObject](DataObjectId("src1"))
  }

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[ActionId]): Unit = {
    implicit val _context: ActionPipelineContext = context
    implicit val _sparkSession: SparkSession = getSparkSession
    import _sparkSession.implicits._
    assert(state.runId == context.executionId.runId && state.attemptId == context.executionId.attemptId)
    logger.info(s"Received metrics for runId=${state.runId} attemptId=${state.attemptId} final=${state.isFinal}")
    // add additional source partition after runIdToAddData
    if (state.isFinal && state.runId == runIdToAddData && !dfWritten) {
      dfWritten = true
      logger.info("adding more data")
      val dfSrc2 = Seq(("20180102", "company", "olmo", "-", 10)) // second partition 20190101
        .toDF("dt", "type", "lastname", "firstname", "rating")
      srcDO.writeSparkDataFrame(dfSrc2, Seq())
    }
  }
}
