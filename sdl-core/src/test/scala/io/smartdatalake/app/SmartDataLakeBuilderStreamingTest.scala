/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{InstanceRegistry, SdlConfigObject}
import io.smartdatalake.definitions.{Environment, PartitionDiffMode, SparkStreamingMode}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.sparktransformer.{SQLDfTransformer, ScalaClassDfTransformer}
import io.smartdatalake.workflow.action._
import io.smartdatalake.workflow.dataobject.{CsvFileDataObject, HiveTableDataObject, Table}
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext, HadoopFileActionDAGRunStateStore}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamingQueryListener}
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files

class SmartDataLakeBuilderStreamingTest extends FunSuite with SmartDataLakeLogger with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  val statePath = "target/streamingStateTest/"
  val checkpointPath = "target/streamingCheckpointTest/"
  val filesystem = HdfsUtil.getHadoopFs(new Path(statePath))

  after {
    // ensure cleanup
    // listListeners not available in Spark 2.4
    //session.streams.listListeners().foreach(session.streams.removeListener)
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false // reset stopping gracefully
  }

  test("sdlb streaming run with normal action, executionMode=PartitionDiffMode") {

    // init sdlb
    val appName = "sdlb-normal"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), filesystem, false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcTable = Table(Some("default"), "ap_input")
    // source table has partitions columns dt and type
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), partitions = Seq("dt","type"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = HiveTableDataObject( "tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), partitions = Seq("dt","type"), table = tgt1Table, numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)

    // fill src table with first partition
    val dfSrc1 = Seq(("20180101", "person", "doe","john",5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc1, Seq())

    // start streaming dag run
    // use only first partition col (dt) for partition diff mode
    val action1 = CopyAction( "a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, udfAddX(rating) rating from src1")))
    instanceRegistry.register(action1)

    // create state listener to control execution
    val stateListener = new StateListener with SmartDataLakeLogger {
      override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext): Unit = {
        assert(state.runId == context.executionId.runId && state.attemptId == context.executionId.attemptId)
        logger.info(s"Received metrics for runId=${state.runId} attemptId=${state.attemptId} final=${state.isFinal}")
        // check results after runId=1
        if (state.isFinal && state.runId==1) {
          // check results
          assert(tgt1DO.listPartitions.map(_.apply("dt")) == Seq("20180101"))
          assert(tgt1DO.getDataFrame(Seq()).select($"lastname").as[String].collect().toSeq == Seq("doe"))
        }
        // add additional source partition in runId=2 for runId=3
        if (state.isFinal && state.runId==2) {
          val dfSrc2 = Seq(("20180102", "company", "olmo","-",10)) // second partition 20190101
            .toDF("dt", "type", "lastname", "firstname", "rating")
          srcDO.writeDataFrame(dfSrc2, Seq())
        }
        // stop after runId=3
        if (state.isFinal && state.runId>=3) {
          Environment.stopStreamingGracefully = true
        }
      }
    }

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath), streaming = true)
    Environment._additionalStateListeners = Seq(stateListener)
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false
    Environment._additionalStateListeners = Seq()

    // check data after streaming is terminated
    assert(tgt1DO.listPartitions.map(_.apply("dt")) == Seq("20180101","20180102"))
    assert(tgt1DO.getDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(6,11)) // +1 because of udfAddX

    // check state after streaming is terminated
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName)
      val stateId = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateId)
      assert(runState.runId >= 3)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(_.state)
      val expectedActionsState = Map((action1.id , RuntimeEventState.SKIPPED))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.subFeed.partitionValues.isEmpty)
    }
  }

  test("sdlb streaming run with streaming action asynchronously, csv files") {

    // init sdlb
    val appName = "sdlb-streaming"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(tempPath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(statePath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(checkpointPath), filesystem, false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source has partitions columns dt and type
    val srcDO = CsvFileDataObject( "src1", tempPath+"/src1", partitions = Seq("dt","type")
                                 , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject( "tgt1", tempPath+"/tgt1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt1DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe","john",5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc1, Seq())

    // prepare streaming action
    val action1 = CopyAction( "a", srcDO.id, tgt1DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, udfAddX(rating) rating from src1")))
    instanceRegistry.register(action1)

    // streaming event listener will add data and stop streaming after 3 micro-batches
    val testStreamingQueryListener = new StreamingQueryListener {
      private val actionRegex = (s"Action~(${SdlConfigObject.idRegexStr})").r.unanchored
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = Unit
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        logger.info(s"progress ${event.progress.batchId} ${event.progress.name}")
        event.progress.name match {
          case actionRegex(actionId) =>
            event.progress.batchId match {
              case 0 =>
                // add some more data
                logger.info("adding more data")
                val dfSrc2 = Seq(("20190101", "company", "olmo", "-", 10)) // second partition 20190101
                  .toDF("dt", "type", "lastname", "firstname", "rating")
                srcDO.writeDataFrame(dfSrc2, Seq())
              case 2 =>
                // stop streaming query
                logger.info("stopping streaming query")
                session.streams.active.find(_.name == event.progress.name).get.stop
              case _ => Unit
            }
        }
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = Unit
    }
    session.streams.addListener(testStreamingQueryListener)

    // start run
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false
    session.streams.removeListener(testStreamingQueryListener)

    // check data after streaming is terminated
    assert(tgt1DO.listPartitions.map(_.apply("dt")).toSet == Seq("20180101","20190101").toSet)
    assert(tgt1DO.getDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(6,11)) // +1 because of udfAddX

    val action1InfoSdl1 = action1.getRuntimeInfo(Some(SDLExecutionId(1))).get
    assert(action1InfoSdl1.state == RuntimeEventState.SUCCEEDED) // State for SDL execution 1 is reported as SUCCEEDED by streaming action
    assert(action1InfoSdl1.results.head.mainMetrics.isEmpty) // no metrics for SDL execution 1 of streaming actions
    val action1InfoSdl2 = action1.getRuntimeInfo(Some(SDLExecutionId(2))).get
    assert(action1InfoSdl2.state == RuntimeEventState.STREAMING) // State for SDL execution 2 is reported as STREAMING by streaming action
    val action1InfoStream1 = action1.getRuntimeInfo(Some(SparkStreamingExecutionId(0))).get
    assert(action1InfoStream1.state == RuntimeEventState.SUCCEEDED)
    assert(action1InfoStream1.results.head.mainMetrics("records_written") == 1)
    val action1InfoStream2 = action1.getRuntimeInfo(Some(SparkStreamingExecutionId(1))).get
    assert(action1InfoStream2.state == RuntimeEventState.SUCCEEDED)
    assert(action1InfoStream2.results.head.mainMetrics("records_written") == 1)

    // check state after streaming is terminated
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName)
      val stateId = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateId)
      // only one SDL run executed (streaming action is asynchronous)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(s => (s.executionId,s.state))
      val expectedActionsState = Map((action1.id, (SDLExecutionId(1), RuntimeEventState.SUCCEEDED))) // State for SDL execution 1 is reported as SUCCEEDED by streaming action
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState(action1.id).results.head.mainMetrics.isEmpty) // no metrics for SDL execution 1 of streaming actions
    }
  }

  test("sdlb streaming run with synchronously and asynchronously streaming action, csv files") {

    // init sdlb
    val appName = "sdlb-streaming2"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(tempPath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(statePath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(checkpointPath), filesystem, false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject( "src1", tempPath+"/src1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject( "tgt1", tempPath+"/tgt1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt1DO)
    // second table has partitions columns dt and type (same as source)
    val tgt2DO = CsvFileDataObject( "tgt2", tempPath+"/tgt2", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt2DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe","john",5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc1, Seq())

    // prepare partition diff action
    val actionA = CopyAction( "a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, rating from src1")))
    // prepare streaming action
    val actionB = CopyAction( "b", tgt1DO.id, tgt2DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, udfAddX(rating) rating from tgt1")))
    instanceRegistry.register(Seq(actionA, actionB))

    // streaming event listener will add data and stop streaming after 3 micro-batches
    val testStreamingQueryListener = new StreamingQueryListener {
      private val actionRegex = (s"Action~(${SdlConfigObject.idRegexStr})").r.unanchored
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = Unit
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        logger.info(s"progress ${event.progress.batchId} ${event.progress.name}")
        event.progress.name match {
          case actionRegex(actionId) =>
            event.progress.batchId match {
              case 0 =>
                // add some more data
                logger.info("adding more data")
                val dfSrc2 = Seq(("20190101", "company", "olmo", "-", 10)) // second partition 20190101
                  .toDF("dt", "type", "lastname", "firstname", "rating")
                srcDO.writeDataFrame(dfSrc2, Seq())
              case x if x>0 && event.progress.numInputRows > 0 =>
                // stop streaming gracefully when second data partition was processed
                logger.info("stopping streaming gracefully")
                Environment.stopStreamingGracefully = true
              case _ => Unit
            }
        }
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = Unit
    }
    session.streams.addListener(testStreamingQueryListener)

    // start run
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false
    session.streams.removeListener(testStreamingQueryListener)

    // check data after streaming is terminated
    assert(tgt1DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101","20190101"))
    assert(tgt2DO.getDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(6,11)) // +1 because of udfAddX
  }

  test("sdlb streaming recovery, synchronous action failing before asynchronously streaming action") {

    // init sdlb
    val appName = "sdlb-streaming3"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(tempPath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(statePath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(checkpointPath), filesystem, false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject( "src1", tempPath+"/src1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject( "tgt1", tempPath+"/tgt1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt1DO)
    // second table has partitions columns dt and type (same as source)
    val tgt2DO = CsvFileDataObject( "tgt2", tempPath+"/tgt2", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt2DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe","john",5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc1, Seq())

    // prepare partition diff action
    val actionAFail = CopyAction( "a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassDfTransformer(className = classOf[FailTransformer].getName)))
    val actionA = CopyAction( "a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, rating from src1")))
    // prepare streaming action
    val actionB = CopyAction( "b", tgt1DO.id, tgt2DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, udfAddX(rating) rating from tgt1")))

    // streaming event listener will add data and stop streaming after 3 micro-batches
    val testStreamingQueryListener = new StreamingQueryListener {
      private val actionRegex = (s"Action~(${SdlConfigObject.idRegexStr})").r.unanchored
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = Unit
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        logger.info(s"progress ${event.progress.batchId} ${event.progress.name}")
        event.progress.name match {
          case actionRegex(actionId) =>
            event.progress.batchId match {
              case 0 =>
                // add some more data
                logger.info("adding more data")
                val dfSrc2 = Seq(("20190101", "company", "olmo", "-", 10)) // second partition 20190101
                  .toDF("dt", "type", "lastname", "firstname", "rating")
                srcDO.writeDataFrame(dfSrc2, Seq())
              case x if x>0 && event.progress.numInputRows > 0 =>
                // stop streaming gracefully when second data partition was processed
                logger.info("stopping streaming gracefully")
                Environment.stopStreamingGracefully = true
              case _ => Unit
            }
        }
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = Unit
    }
    session.streams.addListener(testStreamingQueryListener)

    // start run failing actionA
    instanceRegistry.register(Seq(actionAFail, actionB))
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), streaming = true, statePath = Some(statePath))
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
    session.streams.removeListener(testStreamingQueryListener)

    // check data after streaming is terminated
    assert(tgt1DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101","20190101"))
    assert(tgt2DO.getDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(6,11)) // +1 because of udfAddX
  }

  test("sdlb spark streaming failure, synchronous action before asynchronously streaming action, asynchronous action failing after first run") {

    // init sdlb
    val appName = "sdlb-streaming4"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(tempPath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(statePath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(checkpointPath), filesystem, false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject( "src1", tempPath+"/src1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject( "tgt1", tempPath+"/tgt1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt1DO)
    // second table has partitions columns dt and type (same as source)
    val tgt2DO = CsvFileDataObject( "tgt2", tempPath+"/tgt2", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt2DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe","john",5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc1, Seq())

    // prepare partition diff action
    val actionA = CopyAction( "a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, rating from src1")))
    // prepare streaming action
    val actionB = CopyAction( "b", tgt1DO.id, tgt2DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, udfAddX(rating) rating from tgt1")))

    // streaming event listener will add data and stop streaming after 3 micro-batches
    val testStreamingQueryListener = new StreamingQueryListener {
      private val actionRegex = (s"Action~(${SdlConfigObject.idRegexStr})").r.unanchored
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = Unit
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        logger.info(s"progress ${event.progress.batchId} ${event.progress.name}")
        event.progress.name match {
          case actionRegex(actionId) =>
            event.progress.batchId match {
              case 0 =>
                // add some more data which will fail streaming query (udfAddX fails if input=999)
                logger.info("adding more data")
                val dfSrc2 = Seq(("20190101", "company", "olmo", "-", 999)) // second partition 20190101
                  .toDF("dt", "type", "lastname", "firstname", "rating")
                srcDO.writeDataFrame(dfSrc2, Seq())
            }
        }
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = Unit
    }
    session.streams.addListener(testStreamingQueryListener)

    // start run, actionB will fail after first runId
    instanceRegistry.register(Seq(actionA, actionB))
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    intercept[StreamingQueryException](sdlb.run(sdlConfig))
    Environment.stopStreamingGracefully = false
    session.streams.removeListener(testStreamingQueryListener)
  }

  test("sdlb streaming recovery, asynchronously action failing before synchronous streaming action") {

    // init sdlb
    val appName = "sdlb-streaming5"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(tempPath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(statePath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(checkpointPath), filesystem, false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject( "src1", tempPath+"/src1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject( "tgt1", tempPath+"/tgt1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt1DO)
    // second table has partitions columns dt and type (same as source)
    val tgt2DO = CsvFileDataObject( "tgt2", tempPath+"/tgt2", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt2DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe","john",5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc1, Seq())

    // prepare streaming action
    val actionAFail = CopyAction( "a", srcDO.id, tgt1DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassDfTransformer(className = classOf[FailTransformer].getName)))
    val actionA = CopyAction( "a", srcDO.id, tgt1DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, rating from src1")))
    // prepare partition diff action
    val actionB = CopyAction( "b", tgt1DO.id, tgt2DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, udfAddX(rating) rating from tgt1")))

    // start run failing actionA
    instanceRegistry.register(Seq(actionAFail, actionB))
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    intercept[TaskFailedException](sdlb.run(sdlConfig))
    Environment.stopStreamingGracefully = false

    // create state listener for controlling execution
    val stateListener = new StateListener with SmartDataLakeLogger {
      override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext): Unit = {
        assert(state.runId == context.executionId.runId && state.attemptId == context.executionId.attemptId)
        logger.info(s"Received metrics for runId=${state.runId} attemptId=${state.attemptId} final=${state.isFinal}")
        // add additional source partition for runId=2
        if (state.isFinal && state.runId==2) {
          val dfSrc2 = Seq(("20180102", "company", "olmo","-",10)) // second partition 20190101
            .toDF("dt", "type", "lastname", "firstname", "rating")
          srcDO.writeDataFrame(dfSrc2, Seq())
        }
        // stop after runId=3
        if (state.isFinal && state.runId>=3) {
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
    try {
      sdlb.run(sdlConfig)
    } catch {
      case _: java.lang.InterruptedException => Unit // Ignore - this occurs on Spark 2.x only when stopping streaming queries
    }
    Environment.stopStreamingGracefully = false
    Environment._additionalStateListeners = Seq()

    // check data after streaming is terminated
    assert(tgt2DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101","20180102"))
    assert(tgt2DO.getDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(6,11)) // +1 because of udfAddX
  }

  test("sdlb streaming restart, synchronous action skipped before asynchronously streaming action") {

    // init sdlb
    val appName = "sdlb-streaming6"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(tempPath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(statePath), filesystem, false)
    HdfsUtil.deleteFiles(new Path(checkpointPath), filesystem, false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject( "src1", tempPath+"/src1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject( "tgt1", tempPath+"/tgt1", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt1DO)
    // second table has partitions columns dt and type (same as source)
    val tgt2DO = CsvFileDataObject( "tgt2", tempPath+"/tgt2", partitions = Seq("dt","type")
      , schema = Some(StructType.fromDDL("dt string, type string, lastname string, firstname string, rating int")))
    instanceRegistry.register(tgt2DO)

    // fill src with first files
    val dfSrc1 = Seq(("20180101", "person", "doe","john",5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc1, Seq())

    // prepare partition diff action
    val actionA = CopyAction( "a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, rating from src1")))
    // prepare streaming action
    val actionB = CopyAction( "b", tgt1DO.id, tgt2DO.id, executionMode = Some(SparkStreamingMode(checkpointPath, "ProcessingTime", Some("1 seconds"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, udfAddX(rating) rating from tgt1")))

    // streaming event listener will add data and stop streaming after 3 micro-batches
    val testStreamingQueryListener = new StreamingQueryListener {
      private val actionRegex = (s"Action~(${SdlConfigObject.idRegexStr})").r.unanchored
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = Unit
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        logger.info(s"progress ${event.progress.batchId} ${event.progress.name}")
        event.progress.name match {
          case actionRegex(actionId) =>
            event.progress.batchId match {
              case 0 =>
                // add some more data
                logger.info("adding more data")
                val dfSrc2 = Seq(("20190101", "company", "olmo", "-", 10)) // second partition 20190101
                  .toDF("dt", "type", "lastname", "firstname", "rating")
                srcDO.writeDataFrame(dfSrc2, Seq())
              case x if x>0 && event.progress.numInputRows > 0 =>
                // stop streaming gracefully when second data partition was processed
                logger.info("stopping streaming gracefully")
                Environment.stopStreamingGracefully = true
              case _ => Unit
            }
        }
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = Unit
    }
    session.streams.addListener(testStreamingQueryListener)

    // start run failing actionA
    instanceRegistry.register(Seq(actionA, actionB))
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), streaming = true, statePath = Some(statePath))
    session.streams.resetTerminated() // reset terminated streaming query list
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false

    // check data after streaming is terminated
    assert(tgt1DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101","20190101"))

    // restart run
    val currentRunId = actionA.runtimeData.currentExecutionId.get.asInstanceOf[SDLExecutionId].runId
    session.streams.resetTerminated() // reset terminated streaming query list
    actionA.reset
    actionB.reset
    // this listener adds more data after first skipped run
    Environment._additionalStateListeners = Seq(new PartitionStreamingTestStateListener2(currentRunId+1))
    Environment.stopStreamingGracefully = false
    sdlb.run(sdlConfig)
    Environment.stopStreamingGracefully = false
    Environment._additionalStateListeners = Seq()
    session.streams.removeListener(testStreamingQueryListener)

    // check data after streaming is terminated
    assert(tgt1DO.listPartitions.map(_.apply("dt")).toSet == Set("20180101", "20180102", "20190101"))
  }
}

/**
 * Add more data after given runId
 * @param nextRunId
 */
class PartitionStreamingTestStateListener2(runIdToAddData: Int) extends StateListener with SmartDataLakeLogger {
  var srcDO: CsvFileDataObject = _
  override def init(): Unit = {
    srcDO = Environment.instanceRegistry.get[CsvFileDataObject](DataObjectId("src1"))
  }
  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext): Unit = {
    implicit val _context = context
    implicit val _sparkSession = Environment.sparkSession
    import _sparkSession.implicits._
    assert(state.runId == context.executionId.runId && state.attemptId == context.executionId.attemptId)
    logger.info(s"Received metrics for runId=${state.runId} attemptId=${state.attemptId} final=${state.isFinal}")
    // add additional source partition after runIdToAddData
    if (state.isFinal && state.runId==runIdToAddData) {
      val dfSrc2 = Seq(("20180102", "company", "olmo","-",10)) // second partition 20190101
        .toDF("dt", "type", "lastname", "firstname", "rating")
      srcDO.writeDataFrame(dfSrc2, Seq())
    }
  }
}
