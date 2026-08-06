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

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.{Condition, Environment}
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.plainScala.ScalaTestUtil.getCommonSubFeed
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action._
import io.smartdatalake.workflow.action.executionMode.{DataFrameIncrementalMode, PartitionDiffMode}
import io.smartdatalake.workflow.action.generic.customlogic.{CustomGenericDfTransformer, CustomGenericDfsTransformer}
import io.smartdatalake.workflow.action.generic.transformer.{FilterTransformer, GenericDfTransformer, ScalaClassGenericDfTransformer, ScalaClassGenericDfsTransformer}
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, HadoopFileActionDAGRunStateStore}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.Assertions
import org.slf4j.Logger

/**
 * Engine-agnostic end-to-end tests for [[io.smartdatalake.app.SmartDataLakeBuilder]],
 * to be instantiated against any [[GenericDataFrame]] implementation (Spark, plain-Scala, ...).
 *
 * These tests use configuration test/resources/application.conf.
 */
trait SmartDataLakeBuilderBehaviour extends Assertions {
  this: SmartDataLakeLogger =>

  implicit private val implicitLogger: Logger = logger

  /**
   * Default engine connection registered at the beginning of each behaviour test.
   */
  def defaultEngineConnection: Connection with EngineConnection

  /**
   * Create and register a transactional mock table DataObject for the engine under test.
   */
  def createMockDataObject(id: String, partitions: Seq[String] = Seq(), primaryKey: Option[Seq[String]] = None, expectations: Seq[Expectation] = Seq())(
      implicit instanceRegistry: InstanceRegistry
  ): TransactionalTableDataObject with CanCreateDataFrame with CanWriteDataFrame with CanHandlePartitions

  /**
   * A transformer that makes an action fail during the Exec phase, e.g. at engine runtime.
   */
  def failTransformer: GenericDfTransformer

  /**
   * An expectation named 'testCount' asserting that the dataset is empty (count = 0),
   * e.g. CountExpectation(name = "testCount", expectation = Some("= 0")) for Spark.
   */
  def testCountExpectation: Expectation

  protected val sdlb: DefaultSmartDataLakeBuilder.type = DefaultSmartDataLakeBuilder

  val statePath = "target/stateTest/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))

  protected def prepareRegistry(): InstanceRegistry = {
    sdlb.instanceRegistry.clear()
    sdlb.instanceRegistry.register(defaultEngineConnection)
    sdlb.instanceRegistry
  }

  protected def getStateStore(appName: String): HadoopFileActionDAGRunStateStore = {
    HadoopFileActionDAGRunStateStore(statePath, appName, filesystem.getConf)
  }

  private def execFailTransformer =
    ScalaClassGenericDfTransformer(className = classOf[GenericExecFailTransformer].getName, runtimeOptions = Map("phase" -> "executionPhase"))

  private def execNoDataTransformer =
    ScalaClassGenericDfTransformer(className = classOf[GenericExecNoDataTransformer].getName, runtimeOptions = Map("phase" -> "executionPhase"))

  def testRecoveryAfterActionFailed(): Unit = {

    // init sdlb
    val appName = "sdlb-recovery1"
    val feedName = "test"

    // configure SDLPlugin for testing
    Environment._sdlPlugins = Seq(new TestSDLPlugin)

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = prepareRegistry()
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source table has partitions columns dt and type
    val srcDO = createMockDataObject("src1", partitions = Seq("dt", "type"))
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = createMockDataObject("tgt1", partitions = Seq("dt", "type"), primaryKey = Some(Seq("lastname", "firstname")))
    // second table has partition columns dt only (reduced)
    val tgt2DO = createMockDataObject("tgt2", partitions = Seq("dt"), primaryKey = Some(Seq("lastname", "firstname")))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgt1DO))
    import helper._
    import helper.implicits._

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe", "john", 5) // partition 20180101 is included in partition values filter
      , ("20190101", "company", "olmo", "-", 10)) // partition 20190101 is not included
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc, Seq())

    // start first dag run -> fail
    // load partition 20180101 only
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1.copy())
    val action2fail = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(failTransformer))
    instanceRegistry.register(action2fail.copy())
    val selectedPartitions = Seq(PartitionValues(Map("dt" -> "20180101")))
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath)
      , partitionValues = Some(selectedPartitions))
    intercept[TaskFailedException](sdlb.run(sdlConfig))

    // make sure smart data lake builder cant be started with different config
    val sdlConfigChanged = sdlConfig.copy(partitionValues = None)
    intercept[AssertionError](sdlb.run(sdlConfigChanged))

    // check failed results
    assert(tgt1DO.getDataFrame().select(col("rating")).collect[Int] == Seq(5))
    assert(!tgt2DO.isTableExisting)

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.view.mapValues(_.state).toMap
      val expectedActionsState = Map((action1.id, RuntimeEventState.SUCCEEDED), (action2fail.id, RuntimeEventState.FAILED))
      assert(resultActionsState == expectedActionsState)
    }

    // reset actions in registry
    instanceRegistry.register(action1.copy())

    // start recovery dag run
    // this should execute action b with partition 20180101 only!
    val action2success = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2success.copy())
    sdlb.run(sdlConfig)

    // check results
    assert(tgt2DO.getDataFrame().select(col("rating")).collect[Int] == Seq(5))

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 2)
      val resultActionsState = runState.actionsState.view.mapValues(x => (x.state, x.executionId)).toMap
      val expectedActionsState = Map(action1.id -> (RuntimeEventState.SUCCEEDED, SDLExecutionId(1)),
        action2success.id -> (RuntimeEventState.SUCCEEDED, SDLExecutionId(1, 2)))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.partitionValues == selectedPartitions)
      assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty)
    }

    // test and reset SDLPlugin config
    assert(TestSDLPlugin.startupCalled)
    assert(TestSDLPlugin.configureCalled)
    assert(TestSDLPlugin.shutdownCalled)
    Environment._sdlPlugins = Seq()
  }

  def testRecoveryWithSkippedAction(): Unit = {

    // init sdlb
    val appName = "sdlb-recovery2"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = prepareRegistry()
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source table has partitions columns dt and type
    val srcDO = createMockDataObject("src1", partitions = Seq("dt", "type"))
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = createMockDataObject("tgt1", partitions = Seq("dt", "type"), primaryKey = Some(Seq("lastname", "firstname")))
    // second table has partition columns dt only (reduced)
    val tgt2DO = createMockDataObject("tgt2", partitions = Seq("dt"), primaryKey = Some(Seq("lastname", "firstname")))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgt1DO))
    import helper.implicits._

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe", "john", 5), ("20190101", "company", "olmo", "-", 10))
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc, Seq())
    tgt1DO.writeDataFrame(dfSrc, Seq()) // create table because it's needed but first action is skipped

    // start first dag run -> fail
    // action1 skipped (executionMode.applyCondition = false)
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionCondition = Some(Condition("false", Some("always skip this action"))), metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1.copy())
    val action2fail = CopyAction("b", tgt1DO.id, tgt2DO.id, executionCondition = Some(Condition("true", Some("always execute this action"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(execFailTransformer))
    instanceRegistry.register(action2fail.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    intercept[TaskFailedException](sdlb.run(sdlConfig))

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.view.mapValues(_.state).toMap
      val expectedActionsState = Map((action1.id, RuntimeEventState.SKIPPED), (action2fail.id, RuntimeEventState.FAILED))
      assert(resultActionsState == expectedActionsState)
    }

    // now fill tgt1 with both partitions
    tgt1DO.writeDataFrame(dfSrc, Seq())

    // reset actions in registry
    instanceRegistry.register(action1.copy())

    // start recovery dag run
    val action2success = CopyAction("b", tgt1DO.id, tgt2DO.id, executionCondition = Some(Condition("true", Some("always execute this action"))), metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2success.copy())
    sdlb.run(sdlConfig)

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 2)
      val resultActionsState = runState.actionsState.view.mapValues(x => (x.state, x.executionId)).toMap
      val expectedActionsState = Map(action1.id -> (RuntimeEventState.SKIPPED, SDLExecutionId(1)),
        action2success.id -> (RuntimeEventState.SUCCEEDED, SDLExecutionId(1, 2)))
      assert(resultActionsState == expectedActionsState)
      assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty)
    }
  }

  def testComplexRecoveryWithSkippedActions(): Unit = {

    // init sdlb
    val appName = "sdlb-recovery3"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = prepareRegistry()
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcDO = createMockDataObject("src1", partitions = Seq("dt"))
    val tgt1DO = createMockDataObject("tgt1", partitions = Seq("dt"))
    val tgt2DO = createMockDataObject("tgt2", partitions = Seq("dt"))
    val tgt3DO = createMockDataObject("tgt3", partitions = Seq("dt"))
    val tgt4DO = createMockDataObject("tgt4", partitions = Seq("dt"))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgt1DO))
    import helper.implicits._

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe", "john", 5), ("20190101", "company", "olmo", "-", 10))
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc, Seq())

    // start first dag run -> fail
    // action1 skipped (executionMode.applyCondition = false)
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionCondition = Some(Condition("false", Some("always skip this action"))), metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1.copy())
    // action2 fails
    val action2fail = CopyAction("b", srcDO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(failTransformer))
    instanceRegistry.register(action2fail.copy())
    // action3 is cancelled because action2 fails
    val action3 = CopyAction("c", tgt2DO.id, tgt3DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action3.copy())
    // action4 is cancelled because action3 is cancelled (cancelled has higher prio than skipped from action1)
    val action4 = CustomDataFrameAction("d", Seq(tgt1DO.id, tgt3DO.id), Seq(tgt4DO.id), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassGenericDfsTransformer(className = classOf[GenericPickInputDfsTransformer].getName, options = Map("inputId" -> "tgt1", "outputId" -> "tgt4"))))
    instanceRegistry.register(action4.copy())
    // action5 is skipped because action1 is skipped
    val action5 = CustomDataFrameAction("e", Seq(tgt1DO.id), Seq(tgt4DO.id), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassGenericDfsTransformer(className = classOf[GenericPickInputDfsTransformer].getName, options = Map("inputId" -> "tgt1", "outputId" -> "tgt4"))))
    instanceRegistry.register(action5.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    intercept[TaskFailedException](sdlb.run(sdlConfig))

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.view.mapValues(_.state).toMap
      val expectedActionsState = Map(
        (action1.id, RuntimeEventState.SKIPPED),
        (action2fail.id, RuntimeEventState.FAILED),
        (action3.id, RuntimeEventState.CANCELLED),
        (action4.id, RuntimeEventState.CANCELLED),
        (action5.id, RuntimeEventState.SKIPPED)
      )
      assert(resultActionsState == expectedActionsState)
    }

    // reset actions in registry
    instanceRegistry.register(action1.copy())
    val action2success = CopyAction("b", srcDO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2success)
    instanceRegistry.register(action3.copy())
    instanceRegistry.register(action4.copy())
    instanceRegistry.register(action5.copy())

    // start recovery dag run
    sdlb.run(sdlConfig)

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 2)
      val resultActionsState = runState.actionsState.view.mapValues(x => (x.state, x.executionId)).toMap
      val expectedActionsState = Map(
        action1.id -> (RuntimeEventState.SKIPPED, SDLExecutionId(1)),
        action2success.id -> (RuntimeEventState.SUCCEEDED, SDLExecutionId(1, 2)),
        action3.id -> (RuntimeEventState.SUCCEEDED, SDLExecutionId(1, 2)),
        action4.id -> (RuntimeEventState.SKIPPED, SDLExecutionId(1, 2)),
        action5.id -> (RuntimeEventState.SKIPPED, SDLExecutionId(1))
      )
      assert(resultActionsState == expectedActionsState)
      assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty)
    }
  }

  def testSkippedActionChainTriggeredFromExecPhase(): Unit = {

    // init sdlb
    val appName = "sdlb-skipped-skipped"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = prepareRegistry()
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcDO = createMockDataObject("src1", partitions = Seq("dt"))
    val tgt1DO = createMockDataObject("tgt1", partitions = Seq("dt"))
    val tgt2DO = createMockDataObject("tgt2", partitions = Seq("dt"))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgt1DO))
    import helper.implicits._

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe", "john", 5), ("20190101", "company", "olmo", "-", 10))
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc, Seq())

    // action1 skipped (GenericExecNoDataTransformer)
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(execNoDataTransformer))
    instanceRegistry.register(action1.copy())
    // action2 is skipped because action1 is skipped
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

    // start dag run
    sdlb.run(sdlConfig)

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.view.mapValues(x => (x.state, x.executionId)).toMap
      val expectedActionsState = Map(
        action1.id -> (RuntimeEventState.SKIPPED, SDLExecutionId(1)),
        action2.id -> (RuntimeEventState.SKIPPED, SDLExecutionId(1))
      )
      assert(resultActionsState == expectedActionsState)
    }
  }

  def testSkippedActionMetrics(): Unit = {

    // init sdlb
    val appName = "sdlb-skipped-metrics"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = prepareRegistry()
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcDO = createMockDataObject("src1", partitions = Seq("dt"))
    val tgt1DO = createMockDataObject("tgt1", partitions = Seq("dt"))
    val tgt2DO = createMockDataObject("tgt2", partitions = Seq("dt"))
    val subFeedType = getCommonSubFeed(srcDO, tgt1DO)
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe", "john", 5), ("20190101", "company", "olmo", "-", 10))
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc, Seq())

    // action1 should execute
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1.copy())
    // action2 skipped (no data)
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(FilterTransformer(filterClause = "false", subFeedTypeForValidation = subFeedType.typeSymbol.fullName)) // force no data, so that the Action gets skipped
    )
    instanceRegistry.register(action2.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

    // start dag run
    sdlb.run(sdlConfig)

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.view.mapValues(x => (x.state, x.executionId)).toMap
      val expectedActionsState = Map(
        action1.id -> (RuntimeEventState.SUCCEEDED, SDLExecutionId(1)),
        action2.id -> (RuntimeEventState.SKIPPED, SDLExecutionId(1))
      )
      assert(resultActionsState == expectedActionsState)
      val action1Metrics = runState.actionsState(action1.id).results.head.metrics.get
      assert(action1Metrics == Map("count" -> 2, "records_written" -> 2, "count#mainInput" -> 2, "count#src1" -> 2))
      // no metrics for skipped Action2
      assert(runState.actionsState(action2.id).results.head.metrics.isEmpty)
    }
  }

  def testIncrementalChain(): Unit = {

    // init sdlb
    val appName = "sdlb-incremental"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = prepareRegistry()
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val src1DO = createMockDataObject("src1")
    val src2DO = createMockDataObject("src2")
    val tgt1DO = createMockDataObject("tgt1")
    val tgt2DO = createMockDataObject("tgt2")
    val tgt3DO = createMockDataObject("tgt3")
    val tgt4DO = createMockDataObject("tgt4")
    val helper = DataFrameSubFeed.getCompanion(src1DO.getSubFeedSupportedTypes.head)
    import helper.implicits._

    // prepare data
    val dfSrc1 = Seq((1, "20180101", "person", "doe", "john", 5), (2, "20190101", "company", "olmo", "-", 10))
      .toDF("id", "dt", "type", "lastname", "firstname", "rating")
    src1DO.writeDataFrame(dfSrc1, Seq())
    val dfSrc2 = Seq((1, "abc"))
      .toDF("id", "comment")
    src2DO.writeDataFrame(dfSrc2, Seq())

    // action1 has data
    val action1 = CopyAction("a", src1DO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , executionMode = Some(DataFrameIncrementalMode("id"))
    )
    instanceRegistry.register(action1.copy())
    // action2 is skipped in init phase as data is not yet there, but should execute in exec phase
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , executionMode = Some(DataFrameIncrementalMode("id"))
    )
    instanceRegistry.register(action2.copy())
    // action3 is skipped in init phase as data is not yet there, but should execute in exec phase
    val action3 = CopyAction("c", tgt2DO.id, tgt3DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , executionMode = Some(DataFrameIncrementalMode("id"))
    )
    instanceRegistry.register(action3.copy())
    // action4 is skipped in init phase as data is not yet there, but should execute in exec phase
    val action4 = CustomDataFrameAction("d", Seq(tgt3DO.id, src2DO.id), Seq(tgt4DO.id), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , executionMode = Some(DataFrameIncrementalMode("id"))
      , transformers = Seq(ScalaClassGenericDfsTransformer(className = classOf[GenericPickInputDfsTransformer].getName, options = Map("inputId" -> "tgt3", "outputId" -> "tgt4")))
    )
    instanceRegistry.register(action4.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

    // start dag run
    sdlb.run(sdlConfig)

    // check results
    assert(tgt4DO.getDataFrame().count == 2)
  }

  def testPartitionDiffModeSecondRunStateListener(): Unit = {

    // init sdlb
    val appName = "sdlb-runId"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = prepareRegistry()
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source table has partitions columns dt and type
    val srcDO = createMockDataObject("src1", partitions = Seq("dt", "type"))
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = createMockDataObject("tgt1", partitions = Seq("dt", "type"), primaryKey = Some(Seq("lastname", "firstname")))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgt1DO))
    import helper._
    import helper.implicits._

    // fill src table with first partition
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc1, Seq())

    // start first dag run
    // use only first partition col (dt) for partition diff mode
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassGenericDfTransformer(className = classOf[GenericIdentityDfTransformer].getName)))
    instanceRegistry.register(action1.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    sdlb.run(sdlConfig)

    // check results
    assert(tgt1DO.getDataFrame().select(col("rating")).collect[Int] == Seq(5))

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.view.mapValues(_.state).toMap
      val expectedActionsState = Map((action1.id, RuntimeEventState.SUCCEEDED))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.partitionValues == Seq(PartitionValues(Map("dt" -> "20180101"))))
    }

    // now fill src table with second partitions
    val dfSrc2 = Seq(("20190101", "company", "olmo", "-", 10)) // second partition 20190101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc2, Seq())

    // reset actions in registry
    instanceRegistry.register(action1.copy())

    // start second run
    sdlb.run(sdlConfig)

    // check results
    assert(tgt1DO.getDataFrame().select(col("rating")).collect[Int].sorted == Seq(5, 10))

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 2)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.view.mapValues(_.state).toMap
      val expectedActionsState = Map((action1.id, RuntimeEventState.SUCCEEDED))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.partitionValues == Seq(PartitionValues(Map("dt" -> "20190101"))))
      assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty)
    }

    // check state listener
    {
      assert(TestStateListener.context.isDefined)
      val stateListener = TestStateListener.context.get.globalConfig.stateListeners.head.listener.asInstanceOf[TestStateListener]
      assert(stateListener.firstState.isDefined && !stateListener.firstState.get.isFinal)
      assert(stateListener.finalState.isDefined && stateListener.finalState.get.isFinal)
    }
  }

  def testPartitionDiffModeRecoveryWithExpectation(): Unit = {

    // init sdlb
    val appName = "sdlb-recovery4"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = prepareRegistry()
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // partition columns: dt, type
    val srcDO = createMockDataObject("src1", partitions = Seq("dt", "type"))
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = createMockDataObject("tgt1", partitions = Seq("dt", "type"), primaryKey = Some(Seq("lastname", "firstname")))
    val tgt2DO = createMockDataObject("tgt2", partitions = Seq("dt", "type"), primaryKey = Some(Seq("lastname", "firstname")))
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgt1DO))
    import helper._
    import helper.implicits._

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe", "john", 5), ("20190101", "company", "olmo", "-", 10))
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc, Seq())

    // start first dag run
    // load partition 20180101 only
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))),
      executionMode = Some(PartitionDiffMode(nbOfPartitionValuesPerRun = Some(1), partitionColNb = Some(1))))
    instanceRegistry.register(action1)
    val action2failRuntime = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))),
      transformers = Seq(failTransformer))
    instanceRegistry.register(action2failRuntime)
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    intercept[TaskFailedException](sdlb.run(sdlConfig))

    // check failed results
    assert(tgt1DO.getDataFrame().select(col("rating")).collect[Int] == Seq(5))

    // check latest state
    // failed action2 should have partition values in state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.view.mapValues(s => (s.state, s.results.head.partitionValues)).toMap
      val expectedActionsState = Map(
        (action1.id, (RuntimeEventState.SUCCEEDED, Seq(PartitionValues(Map("dt" -> "20180101"))))),
        (action2failRuntime.id, (RuntimeEventState.FAILED, Seq(PartitionValues(Map("dt" -> "20180101")))))
      )
      assert(resultActionsState == expectedActionsState)
    }

    // reset actions in registry
    instanceRegistry.register(action1.copy())

    // start recovery dag run
    // this should fail because of expectation of tgt2
    // expectation "testCount" will fail, as count should be 1...
    createMockDataObject("tgt2", partitions = Seq("dt", "type"), primaryKey = Some(Seq("lastname", "firstname")), expectations = Seq(testCountExpectation))
    val action2success = action2failRuntime.copy(transformers = Seq())
    instanceRegistry.register(action2success)
    intercept[TaskFailedException](sdlb.run(sdlConfig))

    // check latest state
    // action2 should have metric testCount in state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 2)
      val resultActionsState = runState.actionsState.view.mapValues(s => (s.state, s.results.head.metrics.flatMap(_.get("testCount")))).toMap
      val expectedActionsState = Map(
        (action1.id, (RuntimeEventState.SUCCEEDED, None)),
        (action2success.id, (RuntimeEventState.FAILED, Some(1)))
      )
      assert(resultActionsState == expectedActionsState)
    }

    // reset actions in registry
    instanceRegistry.register(tgt2DO)
    instanceRegistry.register(action2success.copy())

    // start recovery dag run
    // this should execute action b with partition 20180101 only!
    sdlb.run(sdlConfig)

    // check results
    assert(tgt2DO.getDataFrame().select(col("rating")).collect[Int] == Seq(5))

    // check latest state
    {
      val stateStore = getStateStore(appName)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 3)
      val resultActionsState = runState.actionsState.view.mapValues(s => (s.state, s.results.head.partitionValues)).toMap
      val expectedActionsState = Map(
        (action1.id, (RuntimeEventState.SUCCEEDED, Seq(PartitionValues(Map("dt" -> "20180101"))))),
        (action2failRuntime.id, (RuntimeEventState.SUCCEEDED, Seq(PartitionValues(Map("dt" -> "20180101")))))
      )
      assert(resultActionsState == expectedActionsState)
      assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty)
    }
  }
}

/**
 * Generic transformer failing in Exec phase.
 * Use with runtimeOptions = Map("phase" -> "executionPhase").
 */
class GenericExecFailTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    if (options("phase") == "Exec") throw new IllegalStateException(s"($dataObjectId) aborted by GenericExecFailTransformer")
    else df
  }
}

/**
 * Generic transformer throwing NoDataToProcessWarning in Exec phase.
 * Use with runtimeOptions = Map("phase" -> "executionPhase").
 */
class GenericExecNoDataTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    if (options("phase") == "Exec") throw NoDataToProcessWarning(dataObjectId, s"($dataObjectId) skipped by GenericExecNoDataTransformer")
    else df
  }
}

/**
 * Generic transformer returning the input DataFrame unchanged.
 */
class GenericIdentityDfTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = df
}

/**
 * Generic Dfs transformer returning the input DataFrame named by option 'inputId' as output DataFrame named by option 'outputId'.
 */
class GenericPickInputDfsTransformer extends CustomGenericDfsTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], dfs: Map[String, GenericDataFrame]): Map[String, GenericDataFrame] = {
    Map(options("outputId") -> dfs(options("inputId")))
  }
}
