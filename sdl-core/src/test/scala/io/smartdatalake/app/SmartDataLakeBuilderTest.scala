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

import java.nio.file.Files

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.{Environment, PartitionDiffMode}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc.EnvironmentUtil
import io.smartdatalake.workflow.action.customlogic.{CustomDfTransformer, CustomDfTransformerConfig, SparkUDFCreator}
import io.smartdatalake.workflow.action.{ActionMetadata, CopyAction, DeduplicateAction, RuntimeEventState}
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, Table, TickTockHiveTableDataObject}
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext, HadoopFileActionDAGRunStateStore, SparkSubFeed, TaskFailedException}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * This tests use configuration test/resources/application.conf
 */
class SmartDataLakeBuilderTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  val statePath = "target/stateTest/"
  val filesystem = HdfsUtil.getHadoopFs(new Path(statePath))


  test("sdlb run with 2 actions and positive top-level partition values filter, recovery after action 2 failed the first time") {

    // init sdlb
    val appName = "sdlb-recovery"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), filesystem, false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = ActionPipelineContext("testFeed", "testApp", 1, 1, instanceRegistry, None, SmartDataLakeBuilderConfig())

    // setup DataObjects
    val srcTable = Table(Some("default"), "ap_input")
    // source table has partitions columns dt and type
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), partitions = Seq("dt","type"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
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
    srcDO.writeDataFrame(dfSrc, Seq())

    // start first dag run -> fail
    // load partition 20180101 only
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1.copy())
    val action2fail = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName)))
      , transformer = Some(CustomDfTransformerConfig(className = Some(classOf[FailTransformer].getName))))
    instanceRegistry.register(action2fail.copy())
    val selectedPartitions = Seq(PartitionValues(Map("dt"->"20180101")))
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath)
      , partitionValues = Some(selectedPartitions))
    intercept[TaskFailedException](sdlb.run(sdlConfig))

    // make sure smart data lake builder cant be started with different config
    val sdlConfigChanged = sdlConfig.copy(partitionValues = None)
    intercept[AssertionError](sdlb.run(sdlConfigChanged))

    // check failed results
    assert(tgt1DO.getDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(5))
    assert(!tgt2DO.isTableExisting)

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName)
      val stateFile = stateStore.getLatestState()
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(_.state)
      val expectedActionsState = Map((action1.id, RuntimeEventState.SUCCEEDED), (action2fail.id, RuntimeEventState.FAILED))
      assert(resultActionsState == expectedActionsState)
    }

    // now fill tgt1 with both partitions
    tgt1DO.writeDataFrame(dfSrc, Seq())

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
    assert(tgt2DO.getDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(5))

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName)
      val stateFile = stateStore.getLatestState()
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 2)
      val resultActionsState = runState.actionsState.mapValues(_.state)
      val expectedActionsState = Map((action2success.id, RuntimeEventState.SUCCEEDED))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.subFeed.partitionValues == selectedPartitions)
      if (!EnvironmentUtil.isWindowsOS) assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty)
    }
  }

  test("sdlb run with initialExecutionMode=PartitionDiffMode, increase runId on second run, state listener") {

    // init sdlb
    val appName = "sdlb-runId"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), filesystem, false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext : ActionPipelineContext = ActionPipelineContext("testFeed", "testApp", 1, 1, instanceRegistry, None, SmartDataLakeBuilderConfig())

    // setup DataObjects
    val srcTable = Table(Some("default"), "ap_input")
    // source table has partitions columns dt and type
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), partitions = Seq("dt","type"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname","firstname")))
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = TickTockHiveTableDataObject( "tgt1", Some(tempPath+s"/${tgt1Table.fullName}"), partitions = Seq("dt","type"), table = tgt1Table, numInitialHdfsPartitions = 1)
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)

    // fill src table with first partition
    val dfSrc1 = Seq(("20180101", "person", "doe","john",5)) // first partition 20180101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc1, Seq())

    // start first dag run
    // use only first partition col (dt) for partition diff mode
    val action1 = CopyAction( "a", srcDO.id, tgt1DO.id, executionMode = Some(PartitionDiffMode(partitionColNb = Some(1))), metadata = Some(ActionMetadata(feed = Some(feedName)))
                            , transformer = Some(CustomDfTransformerConfig(sqlCode = Some("select dt, type, lastname, firstname, udfAddX(rating) rating from src1"))))
    instanceRegistry.register(action1.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    sdlb.run(sdlConfig)

    // check results
    assert(tgt1DO.getDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(6)) // +1 because of udfAddX

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName)
      val stateFile = stateStore.getLatestState()
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 1)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(_.state)
      val expectedActionsState = Map((action1.id , RuntimeEventState.SUCCEEDED))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.subFeed.partitionValues == Seq(PartitionValues(Map("dt"->"20180101"))))
    }

    // now fill src table with second partitions
    val dfSrc2 = Seq(("20190101", "company", "olmo","-",10)) // second partition 20190101
      .toDF("dt", "type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(dfSrc2, Seq())

    // reset Actions / DataObjects
    instanceRegistry.clear
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgt1DO)
    instanceRegistry.register(action1.copy())

    // start second run
    sdlb.run(sdlConfig)

    // check results
    assert(tgt1DO.getDataFrame(Seq()).select($"rating").as[Int].collect().toSeq == Seq(6,11)) // +1 because of udfAddX

    // check latest state
    {
      val stateStore = HadoopFileActionDAGRunStateStore(statePath, appName)
      val stateFile = stateStore.getLatestState()
      val runState = stateStore.recoverRunState(stateFile)
      assert(runState.runId == 2)
      assert(runState.attemptId == 1)
      val resultActionsState = runState.actionsState.mapValues(_.state)
      val expectedActionsState = Map((action1.id , RuntimeEventState.SUCCEEDED))
      assert(resultActionsState == expectedActionsState)
      assert(runState.actionsState.head._2.results.head.subFeed.partitionValues == Seq(PartitionValues(Map("dt"->"20190101"))))
      if (!EnvironmentUtil.isWindowsOS) assert(filesystem.listStatus(new Path(statePath, "current")).map(_.getPath).isEmpty) // doesnt work on windows
      val stateListener = Environment._globalConfig.stateListeners.head.listener.asInstanceOf[TestStateListener]
      assert(stateListener.firstState.isDefined && !stateListener.firstState.get.isFinal)
      assert(stateListener.finalState.isDefined && stateListener.finalState.get.isFinal)
    }
  }

  test("sdlb simulation run") {

    // init sdlb
    val appName = "sdlb-simulation"
    val feedName = "test"

    HdfsUtil.deleteFiles(new Path(statePath), filesystem, false)
    val sdlb = new DefaultSmartDataLakeBuilder()
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry

    // setup DataObjects
    val srcTable = Table(Some("default"), "ap_input")
    val srcPath = tempPath + s"/${srcTable.fullName}"
    // source table has partitions columns dt and type
    val srcDO = HiveTableDataObject("src1", Some(srcPath), table = srcTable, numInitialHdfsPartitions = 1)
    instanceRegistry.register(srcDO)
    val tgt1Table = Table(Some("default"), "ap_dedup", None, Some(Seq("lastname", "firstname")))
    val tgt1Path = tempPath + s"/${tgt1Table.fullName}"
    val tgt1DO = TickTockHiveTableDataObject("tgt1", Some(tgt1Path), table = tgt1Table, numInitialHdfsPartitions = 1)
    instanceRegistry.register(tgt1DO)
    val tgt2Table = Table(Some("default"), "ap_copy", None, Some(Seq("lastname", "firstname")))
    val tgt2Path = tempPath + s"/${tgt1Table.fullName}"
    val tgt2DO = HiveTableDataObject("tgt2", Some(tgt2Path), table = tgt2Table, numInitialHdfsPartitions = 1)
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
    val (finalSubFeeds, stats) = sdlb.startSimulation(configStart, Seq(SparkSubFeed(Some(dfSrc1), srcDO.id, Seq())))

    // check results
    assert(finalSubFeeds.size == 1)
    assert(stats == Map(RuntimeEventState.INITIALIZED -> 2))
    assert(finalSubFeeds.head.dataFrame.get.select(dfSrc1.columns.map(col):_*).symmetricDifference(dfSrc1).isEmpty)
  }
}

class FailTransformer extends CustomDfTransformer {
  def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    // DataFrame needs at least one string column in schema
    val firstStringColumn = df.schema.fields.find(_.dataType == StringType).map(_.name).get
    val udfFail = udf((s: String) => {throw new IllegalStateException("aborted by FailTransformer"); s})
    // fail at spark runtime
    df.withColumn(firstStringColumn, udfFail(col(firstStringColumn)))
  }
}

class TestStateListener(options: Map[String,String]) extends StateListener {
  var firstState: Option[ActionDAGRunState] = None
  var finalState: Option[ActionDAGRunState] = None
  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext): Unit = {
    if (firstState.isEmpty) firstState = Some(state)
    finalState = Some(state)
  }

}

class TestUDFAddXCreator() extends SparkUDFCreator {
  override def get(options: Map[String, String]): UserDefinedFunction = {
    udf((v: Int) => v + options("x").toInt)
  }
}
