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

import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.SdlConfigObject.{DataObjectId, stringToDataObjectId}
import io.smartdatalake.config.{ConfigParser, ExcludeFromSchemaExport, InstanceRegistry}
import io.smartdatalake.definitions._
import io.smartdatalake.testutils.custom.TestCustomDfsTransformer
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.testutils.{SmartDataLakeBuilderBehaviour, TestSDLPlugin, WebserviceTestUtil}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.{SmartDataLakeLogger, StateUploader}
import io.smartdatalake.util.spark.GetSession.loggEnv
import io.smartdatalake.workflow.action._
import io.smartdatalake.workflow.action.executionMode.DataObjectStateIncrementalMode
import io.smartdatalake.workflow.action.generic.transformer.{ColumnsTransformer, GenericDfTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.{CustomDfTransformer, SparkUDFCreator}
import io.smartdatalake.workflow.action.spark.transformer.{ScalaClassSparkDfTransformer, ScalaClassSparkDfsTransformer}
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed.getSparkSession
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.expectation.{CountExpectation, Expectation}
import io.smartdatalake.workflow.dataobject.generic.{CanCreateIncrementalOutput, Table, TransactionalTableDataObject}
import io.smartdatalake.workflow.dataobject.spark.CanCreateSparkDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, raise_error, udf}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

/**
 * End-to-end tests for [[SmartDataLakeBuilder]] with the Spark engine.
 * Engine-agnostic tests are inherited from [[SmartDataLakeBuilderBehaviour]],
 * spark-specific tests are defined here.
 *
 * This tests use configuration test/resources/application.conf
 */
class SmartDataLakeBuilderTest extends AnyFunSuite with BeforeAndAfter with SmartDataLakeLogger with SmartDataLakeBuilderBehaviour
  with io.smartdatalake.util.spark.dataset.Equality {
  @transient implicit private lazy val implicitLogger: org.slf4j.Logger = logger
  protected implicit val session: SparkSession = SparkTestUtil.session

  import session.implicits._

  override def defaultEngineConnection: Connection with EngineConnection = SparkTestUtil.defaultSparkConnection

  override def createMockDataObject(id: String, partitions: Seq[String], primaryKey: Option[Seq[String]], expectations: Seq[Expectation])(implicit instanceRegistry: InstanceRegistry): MockSparkDataObject = {
    MockSparkDataObject(id, partitions = partitions, primaryKey = primaryKey, expectations = expectations).register
  }

  // fails at spark runtime, after exec phase has started
  override def failTransformer: GenericDfTransformer =
    ScalaClassSparkDfTransformer(className = classOf[RuntimeFailTransformer].getName)

  override def testCountExpectation: Expectation =
    CountExpectation(name = "testCount", expectation = Some("= 0"))

  private val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:SmartDataLakeBuilderTest", "org.hsqldb.jdbcDriver")

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  loggEnv

  before {
    sdlb.instanceRegistry.clear()
    sdlb.instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
  }

  test("Test custom transformation of jdbc table with query and where clause") {

    // init sdlb
    val appName = "filtered-jdbc-transformation"
    val feedName = "add-rownumber"

    // configure SDLPlugin for testing
    Environment._sdlPlugins = Seq(new TestSDLPlugin)

    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext
    val contextExec = actionPipelineContext.copy(phase = ExecutionPhase.Exec)

    // setup DataObjects
    instanceRegistry.register(jdbcConnection)
    // prepare data
    val allData = Table(db = Some("public"), name = "allData")
    val dataObjectAll = JdbcTableDataObject(id = "jdbcAll", table = allData, connectionId = "jdbcCon1",
      jdbcOptions = Map("createTableColumnTypes" -> "id int, text varchar(255)"))
    dataObjectAll.dropTable
    val dfAll = List((1, "abc"), (2, "def"), (3, "abc"), (4, "ghi"), (5, "abc"), (6, "def")).toDF("id", "text")
    dataObjectAll
      .initSparkDataFrame(df = dfAll, partitionValues = Nil)(actionPipelineContext)
    dataObjectAll
      .writeSparkDataFrame(df = dfAll, partitionValues = Nil)(contextExec)

    // prepare view dataObject
    val filteredData = Table(db = Some("public"), name = "filteredData",
      query = Some("select id, text from public.allData where text<'g'"))
    val srcDO = JdbcTableDataObject(id = "srcData", table = filteredData, connectionId = "jdbcCon1")
    instanceRegistry.register(srcDO)

    // prepare target
    val targetTab = Table(db = Some("public"), name = "target")
    val tgtDO = JdbcTableDataObject(id = "target", table = targetTab, connectionId = "jdbcCon1",
      jdbcOptions = Map("createTableColumnTypes" -> "id int, text varchar(255), _rnk int"))
    tgtDO.dropTable
    val expected = List((1, "abc", 1), (2, "def", 1), (3, "abc", 2), (5, "abc", 3), (6, "def", 2))
      .toDF("id", "text", "_rnk")
    tgtDO.initSparkDataFrame(expected.where($"id"===1), Nil)(actionPipelineContext)
    tgtDO.writeSparkDataFrame(expected.where($"id"===1), Nil)(contextExec)
    instanceRegistry.register(tgtDO)

    // define and run the action
    val action = CustomDataFrameAction(id = "add-rownumber",
      inputIds = List(srcDO.id), outputIds = Seq(tgtDO.id),
      metadata = Some(ActionMetadata(feed = Some("add-rownumber"))),
      transformers = List(ScalaClassSparkDfsTransformer(className = classOf[TestCustomDfsTransformer].getName))
    )
    instanceRegistry.register(action.copy())

    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"),
      feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    sdlb.run(sdlConfig)
    val actual = tgtDO.getSparkDataFrame(Nil)(contextExec).orderBy($"id")
    assert(actual.equal(expected))

    // test and reset SDLPlugin config
    assert(TestSDLPlugin.startupCalled)
    assert(TestSDLPlugin.configureCalled)
    assert(TestSDLPlugin.shutdownCalled)
    Environment._sdlPlugins = Seq()
  }

  test("sdlb run with 2 actions and positive top-level partition values filter, recovery after action 2 failed the first time") {
    testRecoveryAfterActionFailed()
  }

  test("sdlb run recovered although state file contains only succeeded and cancelled actions") {
    testRecoveryOfCancelledRun()
  }

  test("sdlb run not recovered because failed state file was accepted by moving it to succeeded directory") {
    testAcceptFailedRunInSucceededDir()
  }

  test("sdlb run with skipped action and recovery after action 2 failed the first time") {
    testRecoveryWithSkippedAction()
  }

  test("complex sdlb run with skipped action and recovery after action 2 failed the first time") {
    testComplexRecoveryWithSkippedActions()
  }

  test("sdlb run skipped action chain triggered from exec phase") {
    testSkippedActionChainTriggeredFromExecPhase()
  }

  test("sdlb run 2nd action skipped, check metrics") {
    testSkippedActionMetrics()
  }

  test("sdlb run incremental chain") {
    testIncrementalChain()
  }

  test("sdlb run with executionMode=PartitionDiffMode, increase runId on second run, state listener") {
    testPartitionDiffModeSecondRunStateListener()
  }

  test("sdlb run with 2 actions and PartitionDiffMode, recovery after action 2 failed the first time") {
    testPartitionDiffModeRecoveryWithExpectation()
  }

  test("sdlb run recovery, runtime information of action completed in previous attempt is available") {

    // init sdlb
    val appName = "sdlb-recovery-predecessor"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = prepareRegistry()
    implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcDO = MockSparkDataObject("src1").register
    val tgt1DO = MockSparkDataObject("tgt1").register
    val tgt2DO = MockSparkDataObject("tgt2").register

    // prepare data
    val dfSrc = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(dfSrc, Seq())

    // action b only runs if its predecessor a wrote records
    val executionCondition = Some(Condition("predecessorActions['a'].metrics['tgt1']['records_written'] > 0",
      Some("run only if action a wrote records")))

    // start first dag run -> action a succeeds, action b fails
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1.copy())
    val action2fail = CopyAction("b", tgt1DO.id, tgt2DO.id, executionCondition = executionCondition,
      metadata = Some(ActionMetadata(feed = Some(feedName))), transformers = Seq(failTransformer))
    instanceRegistry.register(action2fail.copy())
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName,
      applicationName = Some(appName), statePath = Some(statePath))
    intercept[TaskFailedException](sdlb.run(sdlConfig))

    // start recovery dag run.
    // action a completed in attempt 1 and is not executed again, but its metrics must still be available to the
    // executionCondition of action b.
    instanceRegistry.register(action1.copy())
    val action2success = CopyAction("b", tgt1DO.id, tgt2DO.id, executionCondition = executionCondition,
      metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2success.copy())
    sdlb.run(sdlConfig)

    // if the predecessor was not visible, the condition would not be fulfilled and action b would have been skipped
    assert(tgt2DO.getSparkDataFrame().select($"rating").as[Int].collect().toSeq == Seq(5))
    val stateStore = getStateStore(appName)
    val runState = stateStore.recoverRunState(stateStore.getLatestStateId().get)
    assert(runState.attemptId == 2)
    assert(runState.actionsState(action2success.id).state == RuntimeEventState.SUCCEEDED)
  }

  test("sdlb run with executionMode=DataObjectStateIncrementalMode") {

    // init sdlb
    val appName = "sdlb-runId"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    HdfsUtil.deleteFiles(path = new Path(tempPath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val srcDO1 = TestIncrementalDataObject("src1")
    instanceRegistry.register(srcDO1)
    val srcDO2 = TestIncrementalDataObject("src2", initVal = 5)
    instanceRegistry.register(srcDO2)
    val tgt1DO = CsvFileDataObject("tgt1", tempPath + s"/tgt1", saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt1DO)
    val tgt2DO = CsvFileDataObject("tgt2", tempPath + s"/tgt2", saveMode = SDLSaveMode.Append)
    instanceRegistry.register(tgt2DO)

    // start first dag run
    val action1 = CopyAction("a", srcDO1.id, tgt1DO.id, executionMode = Some(DataObjectStateIncrementalMode())
      , transformers = Seq(ColumnsTransformer(additionalColumns = Map("run_id" -> "runId")))
      , metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1)
    val action2 = CopyAction("b", srcDO2.id, tgt2DO.id, executionMode = Some(DataObjectStateIncrementalMode())
      , transformers = Seq(ColumnsTransformer(additionalColumns = Map("run_id" -> "runId")))
      , metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2)
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))
    sdlb.run(sdlConfig)

    // check results
    val dfResult1 = tgt1DO.getSparkDataFrame(Seq())
    assert(dfResult1.select(functions.max($"nb".cast("int")), functions.count("*")).as[(Int, Long)].head() == (10, 10))

    // start second dag run
    action1.reset
    action2.reset
    sdlb.run(sdlConfig)

    // check results
    val dfResult2 = tgt1DO.getSparkDataFrame(Seq())
    assert(dfResult2.select(functions.max($"nb".cast("int")), functions.count("*")).as[(Int, Long)].head() == (20, 20))

    // start 3rd dag run -> no data
    action1.reset
    action2.reset
    sdlb.run(sdlConfig)

    // check results
    val dfResult3 = tgt1DO.getSparkDataFrame(Seq())
    assert(dfResult3.select(functions.max($"nb".cast("int")), functions.count("*")).as[(Int, Long)].head() == (20, 20))

    // start 4th dag run
    action1.reset
    action2.reset
    sdlb.run(sdlConfig)

    // check results
    val dfResult4 = tgt1DO.getSparkDataFrame(Seq())
    assert(dfResult4.select(functions.max($"nb".cast("int")), functions.count("*")).as[(Int, Long)].head() == (30, 30))
  }

  test("sdlb simulation run") {
    // init sdlb
    val appName = "sdlb-simulation"
    val feedName = "test"

    HdfsUtil.deleteFiles(path = new Path(statePath), doWarn = false)
    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
    implicit val actionPipelineContext: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    // source table has partitions columns dt and type
    val srcDO = MockSparkDataObject("src1").register
    instanceRegistry.register(jdbcConnection)
    val tgt1Table = Table(Some("public"), "ap_dedup", None, Some(Seq("lastname", "firstname")))
    val tgt1DO = JdbcTableDataObject("tgt1", table = tgt1Table, connectionId = "jdbcCon1")
    tgt1DO.dropTable
    instanceRegistry.register(tgt1DO)
    val tgt2DO = MockSparkDataObject("tgt2", primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare input DataFrame
    val dfSrc1 = Seq(("20180101", "person", "doe", "john", 5))
      .toDF("dt", "type", "lastname", "firstname", "rating")

    // start first dag run
    val action1 = DeduplicateAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action1)
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
    instanceRegistry.register(action2)
    val configStart = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = feedName, applicationName = Some(appName))
    val (finalSubFeeds, stats) = sdlb.startSimulation(appConfig = configStart,
      initialSubFeeds = Seq(SparkSubFeed(Some(SparkDataFrame(dfSrc1)), srcDO.id, Seq())))

    // check results
    assert(finalSubFeeds.size == 1)
    assert(stats.currentAttempt == Map(RuntimeEventState.INITIALIZED -> 2))
    assert(stats.previousAttempts.isEmpty)
    assert(finalSubFeeds.head.dataFrame.get.select(dfSrc1.columns.toList.map(SparkSubFeed.col)).symmetricDifference(SparkDataFrame(dfSrc1)).isEmpty)
  }

  test("sdlb run converting col names to lower case") {

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
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = "ids:act")

    val srcDO = instanceRegistry.get[CsvFileDataObject]("src")
    val dfSrc = Seq(("testData", "Foo"), ("bar", "Space")).toDF("testColumn", "c?olumnN[ä]me")

    // Run SDLB
    val (outputSubFeeds, _) = sdlb.startSimulation(sdlConfig, Seq(SparkSubFeed(Some(SparkDataFrame(dfSrc)), srcDO.id, Seq())))

    // check result
    val dfTgt = outputSubFeeds.head.dataFrame.get
    val colName = dfTgt.schema.columns
    assert(colName == Seq("test_column", "column_naeme"))
  }


  test("sdlb run converting column names to lower without additional options") {

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
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = "ids:act")

    val srcDO = instanceRegistry.get[CsvFileDataObject]("src")
    val dfSrc = Seq(("testData", "Foo"), ("bar", "Space")).toDF("FOO", "noCamel")

    // Run SDLB
    val (outputSubFeeds, _) = sdlb.startSimulation(sdlConfig, Seq(SparkSubFeed(Some(SparkDataFrame(dfSrc)), srcDO.id, Seq())))

    // check result
    val dfTgt = outputSubFeeds.head.dataFrame.get
    val colName = dfTgt.schema.columns
    assert(colName == Seq("foo", "nocamel"))
  }


  test("sdlb run with state file using FinalStateWriter, FinalMetricsWriter, uiBackend and Environment setting override from config") {

    val port = 8888
    val httpsPort = 8889
    val host = "127.0.0.1"
    val wireMockServer = WebserviceTestUtil.startWebservice(host, port, httpsPort)
    WebserviceTestUtil.setupWebserviceStubs()

    val feedName = "test"
    implicit val actionPipelineContext: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry)

    // write csv data to target/src1, which is defined in "/configState/WithFinalStateWriter.conf"
    val dummySrcDO = CsvFileDataObject("dummysrc1", "target/src1")(sdlb.instanceRegistry)
    val dfSrc1 = Seq("testData").toDF("testColumn")
    dummySrcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())

    // reset environment setting to check
    Environment._dagGraphLogMaxLineLength = None

    // load data from configuration file
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some("test"), configuration = Seq(
      getClass.getResource("/configState/WithFinalStateWriter.conf").getPath)
    )

    // Run SDLB
    sdlb.run(sdlConfig)

    // check override of environment setting from global config
    // NOTE: this might fail with parallel test execution, because Environment is shared between all Tests...
    assert(Environment.dagGraphLogMaxLineLength == 100)

    // check result
    val uploadStagePath = Environment._globalConfig.uiBackend.get.stagePath.get
    assert(filesystem.exists(new Path("target/ext-state/state-test")))
    assert(filesystem.exists(new Path(uploadStagePath)))
    assert(filesystem.listFiles(new Path(uploadStagePath), true).hasNext)
    val dfActionLog = sdlb.instanceRegistry.get[TransactionalTableDataObject with CanCreateSparkDataFrame](DataObjectId("actionLog")).getSparkDataFrame()
    assert(dfActionLog.select($"run_id", $"action_id", $"attempt_id", $"state").as[(Long, String, Int, String)].collect().toSet == Set((1L, "act", 1, "SUCCEEDED")))
    val dfMetricsLog = sdlb.instanceRegistry.get[TransactionalTableDataObject with CanCreateSparkDataFrame](DataObjectId("metricsLog")).getSparkDataFrame()
    assert(dfMetricsLog.select($"run_id", $"action_id", $"data_object_id", $"records_written").as[(Long, String, String, Long)].collect().toSet == Set((1L, "act", "tgt", 1L)))

    // check StateUploader retry
    val uiBackend2 = Environment._globalConfig.uiBackend.get.copy(baseUrl = "https://localhost/good/post/no_auth?tenant=1&repo=abc")
    val stateUploader = uiBackend2.getStateListener.asInstanceOf[StateUploader]
    stateUploader.prepare(actionPipelineContext)
    assert(stateUploader.stageStateStore.get.getFiles().isEmpty)

    wireMockServer.stop()
  }

  // Integration test - create a file 'ui-auth' in the project directory which contains key-value pairs for clientId, user and pwd.
  ignore("sdlb run test aws ui upload") {

    val feedName = "test"
    implicit val actionPipelineContext: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry)

    // write csv data to target/src1, which is defined in "/configState/WithFinalStateWriter.conf"
    val dummySrcDO = CsvFileDataObject("dummysrc1", "target/src1")(sdlb.instanceRegistry)
    val dfSrc1 = Seq("testData").toDF("testColumn")
    dummySrcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())

    // load data from configuration file
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some("test"), configuration = Seq(
      getClass.getResource("/configState/WithRealUIBackend.conf").getPath)
    )

    // Run SDLB
    sdlb.run(sdlConfig)
  }

}

class RuntimeFailTransformer extends CustomDfTransformer {
  override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    // fail at spark runtime
    df.withColumn(df.schema.fieldNames.head, raise_error(lit(s"($dataObjectId) aborted by RuntimeFailTransformer")))
  }
}

class TestUDFAddXCreator extends SparkUDFCreator {
  override def get(options: Map[String, String]): UserDefinedFunction = {
    udf((v: Int) => {
      if (v == 999) throw new IllegalStateException("failing streaming query on input value 999 for testing purposes")
      else v + options("x").toInt
    })
  }
}

/**
 * This test DataObject delivers the 10 next numbers on every increment.
 */
case class TestIncrementalDataObject(
                                      override val id: DataObjectId,
                                      override val metadata: Option[DataObjectMetadata] = None,
                                      initVal: Int = 1
                                    )(implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateSparkDataFrame with CanCreateIncrementalOutput with ExcludeFromSchemaExport {

  // State is the start number of the last delivered increment
  var previousState: Int = initVal
  var nextState: Option[Int] = None
  var noDataCreated: Boolean = false

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {
    val session = getSparkSession
    import session.implicits._
    // simulate no data for one request
    if (previousState == 21 && !noDataCreated && context.phase == ExecutionPhase.Exec) {
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
}
