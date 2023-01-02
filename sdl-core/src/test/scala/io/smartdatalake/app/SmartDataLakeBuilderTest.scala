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

package io.smartdatalake.app

import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId, stringToDataObjectId}
import io.smartdatalake.config.{ConfigParser, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions._
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.EnvironmentUtil
import io.smartdatalake.workflow.action._
import io.smartdatalake.workflow.action.executionMode.{DataFrameIncrementalMode, DataObjectStateIncrementalMode, PartitionDiffMode}
import io.smartdatalake.workflow.action.generic.transformer.{AdditionalColumnsTransformer, SQLDfTransformer, SQLDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.{CustomDfTransformer, SparkUDFCreator}
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfTransformer
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext, ExecutionPhase, HadoopFileActionDAGRunStateStore}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files

/**
 * This tests use configuration test/resources/application.conf
 */
class SmartDataLakeBuilderTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  val statePath = "target/stateTest/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))


  test("sdlb run with 2 actions and positive top-level partition values filter, recovery after action 2 failed the first time") {

    // init sdlb
    val appName = "sdlb-recovery1"
    val feedName = "test"

    // configure SDLPlugin for testing
    Environment._sdlPlugin = Some(Some(new TestSDLPlugin))

    HdfsUtil.deleteFiles(new Path(statePath), false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcTable = Table(Some("default"), "ap_input")
    // source table has partitions columns dt and type
    val srcDO = MockDataObject( "src1", partitions = Seq("dt","type")).register
    val tgt1Table = Table(Some("default"), "ap_copy1", None, Some(Seq("lastname","firstname")))
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = TickTockHiveTableDataObject( "tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), partitions = Seq("dt","type"), table = tgt1Table, numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy2", None, Some(Seq("lastname","firstname")))
    // second table has partition columns dt only (reduced)
    val tgt2DO = HiveTableDataObject( "tgt2", Some(tempPath+s"/${tgt2Table.fullName}"), partitions = Seq("dt"), table = tgt2Table, numInitialHdfsPartitions = 1)
    tgt2DO.dropTable
    instanceRegistry.register(tgt2DO)

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe","john",5) // partition 20180101 is included in partition values filter
      ,("20190101", "company", "olmo","-",10)) // partition 20190101 is not included
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc, Seq())

    // start first dag run -> fail
    // load partition 20180101 only
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1.copy())
    val action2fail = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassSparkDfTransformer(className = classOf[FailTransformer].getName)))
    instanceRegistry.register(action2fail.copy())
    val selectedPartitions = Seq(PartitionValues(Map("dt"->"20180101")))
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath)
      , partitionValues = Some(selectedPartitions))
    intercept[TaskFailedException](sdlb.run(sdlConfig))

    // make sure smart data lake builder cant be started with different config
    val sdlConfigChanged = sdlConfig.copy(partitionValues = None)
    intercept[AssertionError](sdlb.run(sdlConfigChanged))

    // check failed results
    assert(tgt1DO.getSparkDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(5))
    assert(!tgt2DO.isTableExisting)

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(_.state)
      val expectedActionsState = Map((action1.id, RuntimeEventState.SUCCEEDED), (action2fail.id, RuntimeEventState.FAILED))
      assert(resultActionsState == expectedActionsState)
    }

    // now fill tgt1 with both partitions
    tgt1DO.writeSparkDataFrame(dfSrc, Seq())

    // reset DataObjects
    instanceRegistry.clear()
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgt1DO)
    instanceRegistry.register(tgt2DO)
    instanceRegistry.register(action1.copy())

    // start recovery dag run
    // this should execute action b with partition 20180101 only!
    val action2success = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2success.copy())
    sdlb.run(sdlConfig)

    // check results
    assert(tgt2DO.getSparkDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(5))

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 2)
      val resultActionsState = runState.actionsState.mapValues(x=>(x.state, x.executionId))
      val expectedActionsState = Map(action1.id -> (RuntimeEventState.SUCCEEDED,SDLExecutionId(1,1)), action2success.id -> (RuntimeEventState.SUCCEEDED,SDLExecutionId(1,2)))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.subFeed.partitionValues == selectedPartitions)
      if (!EnvironmentUtil.isWindowsOS) assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty)
    }

    // test and reset SDLPlugin config
    assert(TestSDLPlugin.startupCalled)
    assert(TestSDLPlugin.shutdownCalled)
    Environment._sdlPlugin = None
  }

  test("sdlb run with skipped action and recovery after action 2 failed the first time") {

    // init sdlb
    val appName = "sdlb-recovery2"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source table has partitions columns dt and type
    val srcDO = MockDataObject( "src1", partitions = Seq("dt","type")).register
    val tgt1Table = Table(Some("default"), "ap_copy1", None, Some(Seq("lastname","firstname")))
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = TickTockHiveTableDataObject( "tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), partitions = Seq("dt","type"), table = tgt1Table, numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy2", None, Some(Seq("lastname","firstname")))
    // second table has partition columns dt only (reduced)
    val tgt2DO = HiveTableDataObject( "tgt2", Some(tempPath+s"/${tgt2Table.fullName}"), partitions = Seq("dt"), table = tgt2Table, numInitialHdfsPartitions = 1)
    tgt2DO.dropTable
    instanceRegistry.register(tgt2DO)

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe","john",5), ("20190101", "company", "olmo","-",10))
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc, Seq())
    tgt1DO.writeSparkDataFrame(dfSrc) // create table because it's needed but first action is skipped

    // start first dag run -> fail
    // action1 skipped (executionMode.applyCondition = false)
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionCondition = Some(Condition("false", Some("always skip this action"))), metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1.copy())
    val action2fail = CopyAction("b", tgt1DO.id, tgt2DO.id, executionCondition = Some(Condition("true", Some("always execute this action"))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassSparkDfTransformer(className = classOf[ExecFailTransformer].getName, runtimeOptions = Map("phase"-> "executionPhase"))))
    instanceRegistry.register(action2fail.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    intercept[TaskFailedException](sdlb.run(sdlConfig))

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(_.state)
      val expectedActionsState = Map((action1.id, RuntimeEventState.SKIPPED), (action2fail.id, RuntimeEventState.FAILED))
      assert(resultActionsState == expectedActionsState)
    }

    // now fill tgt1 with both partitions
    tgt1DO.writeSparkDataFrame(dfSrc, Seq())

    // reset DataObjects
    instanceRegistry.clear()
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgt1DO)
    instanceRegistry.register(tgt2DO)
    instanceRegistry.register(action1.copy())

    // start recovery dag run
    val action2success = CopyAction("b", tgt1DO.id, tgt2DO.id, executionCondition = Some(Condition("true", Some("always execute this action"))),  metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2success.copy())
    sdlb.run(sdlConfig)

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 2)
      val resultActionsState = runState.actionsState.mapValues(x=>(x.state, x.executionId))
      val expectedActionsState = Map(action1.id -> (RuntimeEventState.SKIPPED,SDLExecutionId(1,1)), action2success.id -> (RuntimeEventState.SUCCEEDED, SDLExecutionId(1,2)))
      assert(resultActionsState == expectedActionsState)
      if (!EnvironmentUtil.isWindowsOS) assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty)
    }

  }

  test("complex sdlb run with skipped action and recovery after action 2 failed the first time") {

    // init sdlb
    val appName = "sdlb-recovery3"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcDO = MockDataObject( "src1", partitions = Seq("dt")).register
    val tgt1DO = MockDataObject( "tgt1", partitions = Seq("dt")).register
    val tgt2DO = MockDataObject( "tgt2", partitions = Seq("dt")).register
    val tgt3DO = MockDataObject( "tgt3", partitions = Seq("dt")).register
    val tgt4DO = MockDataObject( "tgt4", partitions = Seq("dt")).register
    val tgt5DO = MockDataObject( "tgt5", partitions = Seq("dt")).register

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe","john",5), ("20190101", "company", "olmo","-",10))
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc, Seq())

    // start first dag run -> fail
    // action1 skipped (executionMode.applyCondition = false)
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionCondition = Some(Condition("false", Some("always skip this action"))), metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1.copy())
    // action2 fails
    val action2fail = CopyAction("b", srcDO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassSparkDfTransformer(className = classOf[FailTransformer].getName)))
    instanceRegistry.register(action2fail.copy())
    // action3 is cancelled because action2 fails
    val action3 = CopyAction("c", tgt2DO.id, tgt3DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action3.copy())
    // action4 is cancelled because action3 is cancelled (cancelled has higher prio than skipped from action1)
    val action4 = CustomDataFrameAction("d", Seq(tgt1DO.id, tgt3DO.id), Seq(tgt4DO.id), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfsTransformer(code = Map(tgt4DO.id.id -> "select * from tgt1"))))
    instanceRegistry.register(action4.copy())
    // action5 is skipped because action1 is skipped
    val action5 = CustomDataFrameAction("e", Seq(tgt1DO.id), Seq(tgt4DO.id), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfsTransformer(code = Map(tgt4DO.id.id -> "select * from tgt1"))))
    instanceRegistry.register(action5.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    intercept[TaskFailedException](sdlb.run(sdlConfig))

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(_.state)
      val expectedActionsState = Map(
        (action1.id, RuntimeEventState.SKIPPED),
        (action2fail.id, RuntimeEventState.FAILED),
        (action3.id, RuntimeEventState.CANCELLED),
        (action4.id, RuntimeEventState.CANCELLED),
        (action5.id, RuntimeEventState.SKIPPED)
      )
      assert(resultActionsState == expectedActionsState)
    }

    // reset registry
    instanceRegistry.clear()
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgt1DO)
    instanceRegistry.register(tgt2DO)
    instanceRegistry.register(tgt3DO)
    instanceRegistry.register(tgt4DO)
    instanceRegistry.register(tgt5DO)

    instanceRegistry.register(action1.copy())
    val action2success = CopyAction("b", srcDO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2success.copy())
    instanceRegistry.register(action3.copy())
    instanceRegistry.register(action4.copy())
    instanceRegistry.register(action5.copy())

    // start recovery dag run
    sdlb.run(sdlConfig)

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 2)
      val resultActionsState = runState.actionsState.mapValues(x=>(x.state, x.executionId))
      val expectedActionsState = Map(
        action1.id -> (RuntimeEventState.SKIPPED,SDLExecutionId(1,1)),
        action2success.id -> (RuntimeEventState.SUCCEEDED, SDLExecutionId(1,2)),
        action3.id -> (RuntimeEventState.SUCCEEDED, SDLExecutionId(1,2)),
        action4.id -> (RuntimeEventState.SKIPPED, SDLExecutionId(1,2)),
        action5.id -> (RuntimeEventState.SKIPPED, SDLExecutionId(1,1))
      )
      assert(resultActionsState == expectedActionsState)
      if (!EnvironmentUtil.isWindowsOS) assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty)
    }
  }

  test("sdlb run skipped action chain triggered from exec phase") {

    // init sdlb
    val appName = "sdlb-skipped-skipped"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcDO = MockDataObject( "src1", partitions = Seq("dt")).register
    val tgt1DO = MockDataObject( "tgt1", partitions = Seq("dt")).register
    val tgt2DO = MockDataObject( "tgt2", partitions = Seq("dt")).register

    // prepare data
    val dfSrc = Seq(("20180101", "person", "doe","john",5), ("20190101", "company", "olmo","-",10))
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc, Seq())

    // start first dag run -> fail
    // action1 skipped (ExecNoDataTransformer)
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(ScalaClassSparkDfTransformer(className = classOf[ExecNoDataTransformer].getName, runtimeOptions = Map("phase"-> "executionPhase"))))
    instanceRegistry.register(action1.copy())
    // action2 is skipped because action1 is skipped
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

    // start dag run
    sdlb.run(sdlConfig)

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(x=>(x.state, x.executionId))
      val expectedActionsState = Map(
        action1.id -> (RuntimeEventState.SKIPPED,SDLExecutionId(1,1)),
        action2.id -> (RuntimeEventState.SKIPPED, SDLExecutionId(1,1))
      )
      assert(resultActionsState == expectedActionsState)
    }
  }

  test("sdlb run incremental chain") {

    // init sdlb
    val appName = "sdlb-incremental"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val src1DO = MockDataObject("src1").register
    val src2DO = MockDataObject("src2").register
    val tgt1DO = MockDataObject("tgt1").register
    val tgt2DO = MockDataObject("tgt2").register
    val tgt3DO = MockDataObject("tgt3").register
    val tgt4DO = MockDataObject("tgt4").register

    // prepare data
    val dfSrc1 = Seq((1,"20180101", "person", "doe","john",5), (2,"20190101", "company", "olmo","-",10))
      .toDF("id", "dt", "type", "lastname", "firstname", "rating")
    src1DO.writeSparkDataFrame(dfSrc1, Seq())
    val dfSrc2 = Seq((1,"abc"))
      .toDF("id", "comment")
    src2DO.writeSparkDataFrame(dfSrc2, Seq())

    // start first dag run -> fail
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
    val action4 = CustomDataFrameAction("d", Seq(tgt3DO.id,src2DO.id), Seq(tgt4DO.id), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , executionMode = Some(DataFrameIncrementalMode("id"))
      , transformers = Seq(SQLDfsTransformer(code = Map("tgt4" -> "select dt, type, lastname, firstname, udfAddX(rating) rating from tgt3")))
    )
    instanceRegistry.register(action4.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))

    // start dag run
    sdlb.run(sdlConfig)

    // check results
    assert(tgt4DO.getSparkDataFrame(Seq()).count == 2)
  }

  test("sdlb run with executionMode=PartitionDiffMode, increase runId on second run, state listener") {

    // init sdlb
    val appName = "sdlb-runId"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    instanceRegistry.clear

    // setup DataObjects
    val srcTable = Table(Some("default"), "ap_input")
    // source table has partitions columns dt and type
    val srcDO = HiveTableDataObject("src1", Some(tempPath + s"/${srcTable.fullName}"), partitions = Seq("dt", "type"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname", "firstname")))
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = TickTockHiveTableDataObject("tgt1", Some(tempPath + s"/${tgt1Table.fullName}"), partitions = Seq("dt", "type"), table = tgt1Table, numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)

    // fill src table with first partition
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc1, Seq())

    // start first dag run
    // use only first partition col (dt) for partition diff mode

    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformers = Seq(SQLDfTransformer(code = "select dt, type, lastname, firstname, udfAddX(rating) rating from src1")))
    instanceRegistry.register(action1.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    sdlb.run(sdlConfig)

    // check results
    assert(tgt1DO.getSparkDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(6)) // +1 because of udfAddX

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(_.state)
      val expectedActionsState = Map((action1.id, RuntimeEventState.SUCCEEDED))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.subFeed.partitionValues == Seq(PartitionValues(Map("dt" -> "20180101"))))
    }

    // now fill src table with second partitions
    val dfSrc2 = Seq(("20190101", "company", "olmo", "-", 10)) // second partition 20190101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc2, Seq())

    // reset Actions / DataObjects
    instanceRegistry.clear
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgt1DO)
    instanceRegistry.register(action1.copy())

    // start second run
    sdlb.run(sdlConfig)

    // check results
    assert(tgt1DO.getSparkDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(6, 11)) // +1 because of udfAddX

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName, session.sparkContext.hadoopConfiguration)
      val stateFile = stateStore.getLatestStateId().get
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 2)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(_.state)
      val expectedActionsState = Map((action1.id, RuntimeEventState.SUCCEEDED))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.subFeed.partitionValues == Seq(PartitionValues(Map("dt" -> "20190101"))))
      if (!EnvironmentUtil.isWindowsOS) assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty) // doesnt work on windows
    }

    // check state listener
    {
      assert(TestStateListener.context.isDefined)
      val stateListener = TestStateListener.context.get.globalConfig.stateListeners.head.listener.asInstanceOf[TestStateListener]
      assert(stateListener.firstState.isDefined && !stateListener.firstState.get.isFinal)
      assert(stateListener.finalState.isDefined && stateListener.finalState.get.isFinal)
    }
  }

  test("sdlb run with executionMode=DataObjectStateIncrementalMode") {

    // init sdlb
    val appName = "sdlb-runId"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), false)
    HdfsUtil.deleteFiles(new Path(tempPath), false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    instanceRegistry.clear

    // setup DataObjects
    val srcDO1 = TestIncrementalDataObject("src1")
    instanceRegistry.register(srcDO1)
    val srcDO2 = TestIncrementalDataObject("src2", initVal = 5)
    instanceRegistry.register(srcDO2)
    val tgt1DO = CsvFileDataObject( "tgt1", tempPath+s"/tgt1", saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt1DO)
    val tgt2DO = CsvFileDataObject( "tgt2", tempPath+s"/tgt2", saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt2DO)

    // start first dag run
    val action1 = CopyAction( "a", srcDO1.id, tgt1DO.id, executionMode = Some(DataObjectStateIncrementalMode())
      , transformers = Seq(AdditionalColumnsTransformer(additionalColumns = Map("run_id"-> "runId")))
      , metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1)
    val action2 = CopyAction( "b", srcDO2.id, tgt2DO.id, executionMode = Some(DataObjectStateIncrementalMode())
      , transformers = Seq(AdditionalColumnsTransformer(additionalColumns = Map("run_id"-> "runId")))
      , metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2)
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    sdlb.run(sdlConfig)

    // check results
    val dfResult1 = tgt1DO.getSparkDataFrame(Seq())
    assert(dfResult1.select(functions.max($"nb".cast("int")), functions.count("*")).as[(Int,Long)].head == (10,10))

    // start second dag run
    action1.reset
    action2.reset
    sdlb.run(sdlConfig)

    // check results
    val dfResult2 = tgt1DO.getSparkDataFrame(Seq())
    assert(dfResult2.select(functions.max($"nb".cast("int")), functions.count("*")).as[(Int,Long)].head == (20,20))

    // start 3rd dag run -> no data
    action1.reset
    action2.reset
    sdlb.run(sdlConfig)

    // check results
    val dfResult3 = tgt1DO.getSparkDataFrame(Seq())
    assert(dfResult3.select(functions.max($"nb".cast("int")), functions.count("*")).as[(Int,Long)].head == (20,20))

    // start 4th dag run
    action1.reset
    action2.reset
    sdlb.run(sdlConfig)

    // check results
    val dfResult4 = tgt1DO.getSparkDataFrame(Seq())
    assert(dfResult4.select(functions.max($"nb".cast("int")), functions.count("*")).as[(Int,Long)].head == (30,30))
  }

  test("sdlb simulation run") {

    // init sdlb
    val appName = "sdlb-simulation"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    instanceRegistry.clear

    // setup DataObjects
    val srcTable = Table(Some("default"), "ap_input")
    val srcPath = tempPath + s"/${srcTable.fullName}"
    // source table has partitions columns dt and type
    val srcDO = HiveTableDataObject("src1", Some(srcPath), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname", "firstname")))
    val tgt1Path = tempPath + s"/${tgt1Table.fullName}"
    val tgt1DO = TickTockHiveTableDataObject("tgt1", Some(tgt1Path), table = tgt1Table, numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname", "firstname")))
    val tgt2Path = tempPath + s"/${tgt2Table.fullName}"
    val tgt2DO = HiveTableDataObject("tgt2", Some(tgt2Path), table = tgt2Table, numInitialHdfsPartitions = 1)
    tgt2DO.dropTable
    instanceRegistry.register(tgt2DO)

    // prepare input DataFrame
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5))
      .toDF("dt", "type", "lastname", "firstname", "rating")

    // start first dag run
    val action1 = DeduplicateAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1)
    val action2 = CopyAction( "b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2)
    val configStart = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName))
    val (finalSubFeeds, stats) = sdlb.startSimulation(configStart, Seq(SparkSubFeed(Some(SparkDataFrame(dfSrc1)), srcDO.id, Seq())))

    // check results
    assert(finalSubFeeds.size == 1)
    assert(stats == Map(RuntimeEventState.INITIALIZED -> 2))
    assert(finalSubFeeds.head.dataFrame.get.select(dfSrc1.columns.map(SparkSubFeed.col)).symmetricDifference(SparkDataFrame(dfSrc1)).isEmpty)
  }

  test("sdlb run converting col names to lower case") {

    val sdlb = new DefaultSmartDataLakeBuilder()

    val config = ConfigFactory.parseString(
      """
        |actions = {
        |   act = {
        |     type = CopyAction
        |     inputId = src
        |     outputId = tgt
        |     transformers = [{
        |       type = StandardizeColNamesTransformer
        |     }]
        |   }
        |}
        |dataObjects {
        |  src {
        |    #id = ~{id}
        |    type = CsvFileDataObject
        |    path = "target/src"
        |  }
        |  tgt {
        |    type = CsvFileDataObject
        |    path = "target/tgt"
        |  }
        |}
        |""".stripMargin).resolve

    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel="ids:act")

    val srcDO = instanceRegistry.get[CsvFileDataObject]("src")
    assert(srcDO != None)
    val dfSrc = Seq(("testData", "Foo"),("bar", "Space")).toDF("testColumn", "c?olumnN[ä]me")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    // Run SDLB
    sdlb.startSimulation(sdlConfig, Seq(SparkSubFeed(Some(SparkDataFrame(dfSrc)), srcDO.id, Seq())))

    // check result
    val tgt = instanceRegistry.get[CsvFileDataObject]("tgt")
    val dfTgt = tgt.getSparkDataFrame()
    val colName = dfTgt.columns
    assert(colName.toSeq == Seq("test_column", "column_naeme"))
  }


  test("sdlb run converting column names to lower without additional options") {

    val sdlb = new DefaultSmartDataLakeBuilder()

    val config = ConfigFactory.parseString(
      """
        |actions = {
        |   act = {
        |     type = CopyAction
        |     inputId = src
        |     outputId = tgt
        |     transformers = [{
        |       type = StandardizeColNamesTransformer
        |       camelCaseToLower = false
        |       normalizeToAscii = false
        |       removeNonStandardSQLNameChars = false
        |     }]
        |   }
        |}
        |dataObjects {
        |  src {
        |    #id = ~{id}
        |    type = CsvFileDataObject
        |    path = "target/src"
        |  }
        |  tgt {
        |    type = CsvFileDataObject
        |    path = "target/tgt"
        |  }
        |}
        |""".stripMargin).resolve

    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel="ids:act")

    val srcDO = instanceRegistry.get[CsvFileDataObject]("src")
    assert(srcDO != None)
    val dfSrc = Seq(("testData", "Foo"),("bar", "Space")).toDF("FOO", "noCamel")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    // Run SDLB
    sdlb.startSimulation(sdlConfig, Seq(SparkSubFeed(Some(SparkDataFrame(dfSrc)), srcDO.id, Seq())))

    // check result
    val tgt = instanceRegistry.get[CsvFileDataObject]("tgt")
    val dfTgt = tgt.getSparkDataFrame()
    val colName = dfTgt.columns
    assert(colName.toSeq == Seq("foo", "nocamel"))
  }


  test("sdlb run with external state file using FinalStateWriter and Environment setting override from config") {

    val feedName = "test"
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // write csv data to target/src1, which is defined in "/configState/WithFinalStateWriter.conf"
    val dummySrcDO = CsvFileDataObject("dummysrc1", "target/src1")
    val dfSrc1 = Seq("testData").toDF("testColumn")
    dummySrcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())

    // reset environment setting to check
    Environment._dagGraphLogMaxLineLength = None

    // load data from configuration file
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = Some(Seq(
      getClass.getResource("/configState/WithFinalStateWriter.conf").getPath)))

    // Run SDLB
    sdlb.run(sdlConfig)

    // check override of environment setting from global config
    // NOTE: this might fail with parallel test execution, because Environment is shared between all Tests...
    assert(Environment.dagGraphLogMaxLineLength == 100)

    // check result
    val fileResult = filesystem.exists(new Path("ext-state/state-test"))
    assert(fileResult)
  }
}

class FailTransformer extends CustomDfTransformer {
  import functions.col
  def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    // DataFrame needs at least one string column in schema
    val firstStringColumn = df.schema.fields.find(_.dataType == StringType).map(_.name).get
    val udfFail = udf((s: String) => {throw new IllegalStateException("aborted by FailTransformer"); s})
    // fail at spark runtime
    df.withColumn(firstStringColumn, udfFail(col(firstStringColumn)))
  }
}

class ExecFailTransformer extends CustomDfTransformer {
  def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    if (options("phase")=="Exec") throw new IllegalStateException("aborted by FailTransformer")
    else df
  }
}

class ExecNoDataTransformer extends CustomDfTransformer {
  def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    if (options("phase")=="Exec") throw NoDataToProcessWarning(dataObjectId, "skipped by ExecNoDataTransformer")
    else df
  }
}

class TestStateListener(options: Map[String,String]) extends StateListener {
  var firstState: Option[ActionDAGRunState] = None
  var finalState: Option[ActionDAGRunState] = None
  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId : Option[ActionId]): Unit = {
    if (TestStateListener.context.isEmpty) TestStateListener.context = Some(context)
    if (firstState.isEmpty) firstState = Some(state)
    finalState = Some(state)
  }
}
object TestStateListener {
  var context: Option[ActionPipelineContext] = None
}

class TestUDFAddXCreator() extends SparkUDFCreator {
  override def get(options: Map[String, String]): UserDefinedFunction = {
    udf((v: Int) => {
      if (v==999) throw new IllegalStateException("failing streaming query on input value 999 for testing purposes")
      else v + options("x").toInt
    })
  }
}

class TestSDLPlugin extends SDLPlugin {
  override def startup(): Unit = { TestSDLPlugin.startupCalled = true }
  override def shutdown(): Unit = { TestSDLPlugin.shutdownCalled = true }
}
object TestSDLPlugin {
  var startupCalled = false
  var shutdownCalled = false
}

/**
 * This test DataObject delivers the 10 next numbers on every increment.
 */
case class TestIncrementalDataObject(override val id: DataObjectId, override val metadata: Option[DataObjectMetadata] = None, initVal: Int = 1)
  extends DataObject with CanCreateSparkDataFrame with CanCreateIncrementalOutput {

  // State is the start number of the last delivered increment
  var previousState: Int = initVal
  var nextState: Option[Int] = None
  var noDataCreated: Boolean = false

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {
    val session = context.sparkSession
    import session.implicits._
    // simulate no data for one request
    if (previousState == 21 && !noDataCreated && context.phase==ExecutionPhase.Exec) {
      noDataCreated = true
      throw NoDataToProcessWarning(id.id, "test")
    }
    nextState = Some(previousState + 10)
    logger.info(s"($id) selecting values $previousState to ${nextState.get}")
    (previousState until nextState.get).toDF("nb")
  }

  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {
    previousState = state.map(_.toInt).getOrElse(initVal)
  }

  override def getState: Option[String] = {
    nextState.map(_.toString)
  }

  override def factory: FromConfigFactory[DataObject] = null // this DataObject will not be instantiated from config files...
}

