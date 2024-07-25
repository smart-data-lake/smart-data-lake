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

import com.typesafe.config.Config
import io.smartdatalake.app
import io.smartdatalake.communication.statusinfo.StatusInfoServer
import io.smartdatalake.communication.statusinfo.api.SnapshotStatusInfoListener
import io.smartdatalake.communication.statusinfo.websocket.IncrementalStatusInfoListener
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{ConfigLoader, ConfigParser, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.dag.{DAGException, ExceptionSeverity}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{LogUtil, MemoryUtils, SerializableHadoopConfiguration, SmartDataLakeLogger}
import io.smartdatalake.workflow.ExecutionPhase.{Exec, ExecutionPhase, Init, Prepare}
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{Action, DataFrameActionImpl, RuntimeInfo, SDLExecutionId}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import scopt.{OParser, OParserBuilder}

import java.io.File
import java.time.{Duration, LocalDateTime}
import scala.annotation.tailrec
import scala.util.Try

trait SmartDataLakeBuilderConfigTrait[R] {
  //All builder functions for all fields
  def withfeedSel(value: String): R

  def withapplicationName(value: Option[String]): R

  def withconfiguration(value: Option[Seq[String]]): R

  def withmaster(value: Option[String]): R

  def withdeployMode(value: Option[String]): R

  def withusername(value: Option[String]): R

  def withkerberosDomain(value: Option[String]): R

  def withkeytabPath(value: Option[File]): R

  def withpartitionValues(value: Option[Seq[PartitionValues]]): R

  def withmultiPartitionValues(value: Option[Seq[PartitionValues]]): R

  def withparallelism(value: Int): R

  def withstatePath(value: Option[String]): R

  def withoverrideJars(value: Option[Seq[String]]): R

  def withtest(value: Option[TestMode.Value]): R

  def withstreaming(value: Boolean): R

  //All fieldnames of the corresponding case class
  def feedSel: String = null

  def applicationName: Option[String] = None

  def configuration: Option[Seq[String]] = None

  def master: Option[String] = None

  def deployMode: Option[String] = None

  def username: Option[String] = None

  def kerberosDomain: Option[String] = None

  def keytabPath: Option[File] = None

  def partitionValues: Option[Seq[PartitionValues]] = None

  def multiPartitionValues: Option[Seq[PartitionValues]] = None

  def parallelism: Int = 1

  def statePath: Option[String] = None

  def overrideJars: Option[Seq[String]] = None

  def test: Option[TestMode.Value] = None

  def streaming: Boolean = false
}

/**
 * This case class represents a default configuration for the App.
 * It is populated by parsing command-line arguments.
 * It also specifies default values.
 *
 * @param feedSel         Expressions to select the actions to execute. See [[AppUtil.filterActionList()]] or commandline help for syntax description.
 * @param applicationName Application name.
 * @param configuration   One or multiple configuration files or directories containing configuration files, separated by comma.
 * @param master          The Spark master URL passed to SparkContext when in local mode.
 * @param deployMode      The Spark deploy mode passed to SparkContext when in local mode.
 * @param username        Kerberos user name (`username`@`kerberosDomain`) for local mode.
 * @param kerberosDomain  Kerberos domain (`username`@`kerberosDomain`) for local mode.
 * @param keytabPath      Path to Kerberos keytab file for local mode.
 * @param test            Run in test mode:
 *                        - "config": validate configuration
 *                        - "dry-run": execute "prepare" and "init" phase to check environment
 */
case class SmartDataLakeBuilderConfig(override val feedSel: String = null,
                                      override val applicationName: Option[String] = None,
                                      override val configuration: Option[Seq[String]] = None,
                                      override val master: Option[String] = None,
                                      override val deployMode: Option[String] = None,
                                      override val username: Option[String] = None,
                                      override val kerberosDomain: Option[String] = None,
                                      override val keytabPath: Option[File] = None,
                                      override val partitionValues: Option[Seq[PartitionValues]] = None,
                                      override val multiPartitionValues: Option[Seq[PartitionValues]] = None,
                                      override val parallelism: Int = 1,
                                      override val statePath: Option[String] = None,
                                      override val overrideJars: Option[Seq[String]] = None,
                                      override val test: Option[TestMode.Value] = None,
                                      override val streaming: Boolean = false
                                     ) extends SmartDataLakeBuilderConfigTrait[SmartDataLakeBuilderConfig] {

  def validate(): Unit = {
    assert(!applicationName.exists(_.contains({
      HadoopFileActionDAGRunStateStore.fileNamePartSeparator
    })), s"Application name must not contain character '${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}' ($applicationName)")
    assert(!master.contains("yarn") || deployMode.nonEmpty, "spark deploy-mode must be set if spark master=yarn")
    assert(partitionValues.isEmpty || multiPartitionValues.isEmpty, "partitionValues and multiPartitionValues cannot be defined at the same time")
    assert(statePath.isEmpty || applicationName.isDefined, "application name must be defined if state path is set")
    assert(!streaming || statePath.isDefined, "state path must be set if streaming is enabled")
  }

  def getPartitionValues: Option[Seq[PartitionValues]] = partitionValues.orElse(multiPartitionValues)

  val appName: String = applicationName.getOrElse(feedSel)

  def isDryRun: Boolean = test.contains(TestMode.DryRun)
  override def withfeedSel(value: String): SmartDataLakeBuilderConfig = copy(feedSel = value)

  override def withapplicationName(value: Option[String]): SmartDataLakeBuilderConfig = copy(applicationName = value)

  override def withconfiguration(value: Option[Seq[String]]): SmartDataLakeBuilderConfig = copy(configuration = value)

  override def withmaster(value: Option[String]): SmartDataLakeBuilderConfig = copy(master = value)

  override def withdeployMode(value: Option[String]): SmartDataLakeBuilderConfig = copy(deployMode = value)

  override def withusername(value: Option[String]): SmartDataLakeBuilderConfig = copy(username = value)

  override def withkerberosDomain(value: Option[String]): SmartDataLakeBuilderConfig = copy(kerberosDomain = value)

  override def withkeytabPath(value: Option[File]): SmartDataLakeBuilderConfig = copy(keytabPath = value)

  override def withpartitionValues(value: Option[Seq[PartitionValues]]): SmartDataLakeBuilderConfig = copy(partitionValues = value)

  override def withmultiPartitionValues(value: Option[Seq[PartitionValues]]): SmartDataLakeBuilderConfig = copy(multiPartitionValues = value)

  override def withparallelism(value: Int): SmartDataLakeBuilderConfig = copy(parallelism = value)

  override def withstatePath(value: Option[String]): SmartDataLakeBuilderConfig = copy(statePath = value)

  override def withoverrideJars(value: Option[Seq[String]]): SmartDataLakeBuilderConfig = copy(overrideJars = value)

  override def withtest(value: Option[TestMode.Value]): SmartDataLakeBuilderConfig = copy(test = value)

  override def withstreaming(value: Boolean): SmartDataLakeBuilderConfig = copy(streaming = value)
}

object TestMode extends Enumeration {
  type TestMode = Value

  /**
   * Test if config is valid.
   * Note that this only parses and validates the configuration. No attempts are made to check the environment (e.g. connection informations...).
   */
  val Config: app.TestMode.Value = Value("config")

  /**
   * Test the environment if connections can be initalized and spark lineage can be created.
   * Note that no changes are made to the environment if possible.
   * The test executes "prepare" and "init" phase, but not the "exec" phase of an SDLB run.
   */
  val DryRun: app.TestMode.Value = Value("dry-run")
}

/**
 * Abstract Smart Data Lake Builder Command Line Application.
 */
abstract class SmartDataLakeBuilder extends SmartDataLakeLogger {

  val appVersion: String = "appVersion: " + BuildVersionInfo.appVersionInfo.orElse(AppUtil.getManifestVersion.map("version=" + _)).getOrElse("develop") +
    ", sdlbVersion: " + BuildVersionInfo.sdlbVersionInfo.getOrElse("unknown")
  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType

   /**
   * InstanceRegistry instance
   */
  val instanceRegistry: InstanceRegistry = new InstanceRegistry()

  /**
   * The Parser defines how to extract the options from the command line args.
   * Subclasses SmartDataLakeBuilder can define additional options to be extracted.
   */

  protected def parserGeneric[R <: SmartDataLakeBuilderConfigTrait[R]](feedSelRequired: Boolean = true): OParser[_, R] = {
    val builder: OParserBuilder[R] = OParser.builder[R]
    import builder._

    val nonRequiredFeedselParser =
      opt[String]('f', "feed-sel")
        .action((arg, config) => config.withfeedSel(arg))
        .valueName("<operation?><prefix:?><regex>[,<operation?><prefix:?><regex>...]")
        .text(
          """Select actions to execute by one or multiple expressions separated by comma (,). Results from multiple expressions are combined from left to right.
            |Operations:
            |- pipe symbol (|): the two sets are combined by union operation (default)
            |- ampersand symbol (&): the two sets are combined by intersection operation
            |- minus symbol (-): the second set is subtracted from the first set
            |Prefixes:
            |- 'feeds': select actions where metadata.feed is matched by regex pattern (default)
            |- 'names': select actions where metadata.name is matched by regex pattern
            |- 'ids': select actions where id is matched by regex pattern
            |- 'layers': select actions where metadata.layer of all output DataObjects is matched by regex pattern
            |- 'startFromActionIds': select actions which with id is matched by regex pattern and any dependent action (=successors)
            |- 'endWithActionIds': select actions which with id is matched by regex pattern and their predecessors
            |- 'startFromDataObjectIds': select actions which have an input DataObject with id is matched by regex pattern and any dependent action (=successors)
            |- 'endWithDataObjectIds': select actions which have an output DataObject with id is matched by regex pattern and their predecessors
            |All matching is done case-insensitive.
            |Example: to filter action 'A' and its successors but only in layer L1 and L2, use the following pattern: "startFromActionIds:a,&layers:(l1|l2)"""".stripMargin)

    val requiredFeedselParser = nonRequiredFeedselParser.required()

    OParser.sequence(
      head(appType, s"$appVersion"),
      if(feedSelRequired) requiredFeedselParser else nonRequiredFeedselParser,
      opt[String]('n', "name")
        .action((arg, config) => config.withapplicationName(Some(arg)))
        .text("Optional name of the application. If not specified feed-sel is used."),
      opt[Seq[String]]('c', "config")
        .action((arg, config) => config.withconfiguration(Some(arg)))
        .valueName("<file1>[,<file2>...]")
        .text("One or multiple configuration files or directories containing configuration files, separated by comma. Entries must be valid Hadoop URIs or a special URI with scheme \"cp\" which is treated as classpath entry."),
      opt[String]("partition-values")
        .action((arg, config) => config.withpartitionValues(Some(PartitionValues.parseSingleColArg(arg))))
        .valueName(PartitionValues.singleColFormat)
        .text(s"Partition values to process for one single partition column."),
      opt[String]("multi-partition-values")
        .action((arg, config) => config.withpartitionValues(Some(PartitionValues.parseMultiColArg(arg))))
        .valueName(PartitionValues.multiColFormat)
        .text(s"Partition values to process for multiple partition columns."),
      opt[Unit]('s', "streaming")
        .action((_, config) => config.withstreaming(true))
        .text(s"Enable streaming mode for continuous processing."),
      opt[Int]("parallelism")
        .action((arg, config) => config.withparallelism(arg))
        .valueName("<int>")
        .text(s"Parallelism for DAG run."),
      opt[String]("state-path")
        .action((arg, config) => config.withstatePath(Some(arg)))
        .valueName("<path>")
        .text(s"Path to save run state files. Must be set to enable recovery in case of failures."),
      opt[Seq[String]]("override-jars")
        .action((arg, config) => config.withoverrideJars(Some(arg)))
        .valueName("<jar1>[,<jar2>...]")
        .text("Comma separated list of jar filenames for child-first class loader. The jars must be present in classpath."),
      opt[String]("test")
        .action((arg, config) => config.withtest(Some(TestMode.withName(arg))))
        .valueName("<config|dry-run>")
        .text("Run in test mode: config -> validate configuration, dry-run -> execute prepare- and init-phase only to check environment and spark lineage"),
      help("help").text("Display the help text."),
      version("version").text("Display version information.")
    )

  }

  protected val parser: OParser[_, SmartDataLakeBuilderConfig] = {

    val builder = OParser.builder[SmartDataLakeBuilderConfig]

    import builder._

    OParser.sequence(
      programName(appType),
      head(appType, s"$appVersion"),
      parserGeneric()
    )
  }

  /**
   * Run the application with the provided configuration.
   *
   * @param appConfig Application configuration (parsed from command line).
   */
  def run(appConfig: SmartDataLakeBuilderConfig): Map[RuntimeEventState, Int] = {
    AppUtil.setSdlbRunLoggerContext(appConfig)
    val stats = try {
      // invoke SDLPlugin if configured
      Environment.sdlPlugin.foreach(_.startup())
      // create default hadoop configuration, as we did not yet load custom spark/hadoop properties
      implicit val defaultHadoopConf: Configuration = new Configuration()
      // handle state if defined
      if (appConfig.statePath.isDefined && !appConfig.isDryRun) {
        assert(appConfig.applicationName.nonEmpty, "Application name must be defined if statePath is set")
        // check if latest run succeeded
        val appName = appConfig.applicationName.get
        val stateStore = HadoopFileActionDAGRunStateStore(appConfig.statePath.get, appName, defaultHadoopConf)
        val latestRunId = stateStore.getLatestRunId
        if (latestRunId.isDefined) {
          val latestStateId = stateStore.getLatestStateId(latestRunId)
            .getOrElse(throw new IllegalStateException(s"State for last runId=$latestRunId not found"))
          val latestRunState = stateStore.recoverRunState(latestStateId)
          if (!latestRunState.isFinal || latestRunState.isFailed) {
            // start recovery
            assert(appConfig == latestRunState.appConfig, s"There is a failed run to be recovered. Either you clean-up this state fail or the command line parameters given must match the parameters of the run to be recovered: ConfigToRecover=${latestRunState.appConfig} ConfigGiven=${appConfig}")
            recoverRun(appConfig, stateStore, latestRunState)._2
          } else {
            val nextExecutionId = SDLExecutionId(latestRunState.runId + 1)
            startRun(appConfig, executionId = nextExecutionId, dataObjectsState = latestRunState.getDataObjectsState, stateStore = Some(stateStore))._2
          }
        } else {
          startRun(appConfig, stateStore = Some(stateStore))._2
        }
      } else startRun(appConfig)._2
    } catch {
      case e: Exception =>
        // try shutdown but catch potential exception
        Try(shutdown)
        // throw original exception
        throw e
    }
    shutdown
    // return
    stats
  }

  /**
   * Execute shutdown/cleanup tasks before SDLB job stops.
   */
  private def shutdown: Unit = {
    // make sure memory logger timer task is stopped
    MemoryUtils.stopMemoryLogger()

    // invoke SDLPlugin if configured
    Environment.sdlPlugin.foreach(_.shutdown())

    //Environment._globalConfig can be null here if global contains superfluous entries
    val stopStatusInfoServer = Option(Environment._globalConfig).flatMap(_.statusInfo.map(_.stopOnEnd)).getOrElse(false)
    if (stopStatusInfoServer) {
      StatusInfoServer.stop()
    }
  }

  /**
   * Recover previous failed run.
   */
  private[smartdatalake] def recoverRun[S <: StateId](appConfig: SmartDataLakeBuilderConfig, stateStore: ActionDAGRunStateStore[S], runState: ActionDAGRunState)(implicit hadoopConf: Configuration): (Seq[SubFeed], Map[RuntimeEventState, Int]) = {
    logger.info(s"recovering application ${appConfig.applicationName.get} runId=${runState.runId} lastAttemptId=${runState.attemptId}")

    // Accept recovery of old state files (without version), check version for newer formats
    assert(runState.runStateFormatVersion.isEmpty || runState.runStateFormatVersion.get == ActionDAGRunState.runStateFormatVersion,
      s"State file format version ${runState.runStateFormatVersion.get} does not match current version ${ActionDAGRunState.runStateFormatVersion}. Can not recover run from different state format."
    )

    // skip all succeeded actions
    val actionsToSkip = runState.actionsState
      .filter { case (id, info) => info.hasCompleted }
    val initialSubFeeds = actionsToSkip.flatMap(_._2.results).toSeq
    // get latest DataObject state and overwrite with current DataObject state
    val lastStateId = stateStore.getLatestStateId(Some(runState.runId - 1))
    val lastRunState = lastStateId.map(stateStore.recoverRunState)
    val dataObjectsState = (lastRunState.map(_.getDataObjectsState.map(_.getEntry).toMap).getOrElse(Map()) ++ runState.getDataObjectsState.map(_.getEntry).toMap).values.toSeq
    // start run, increase attempt counter
    val recoveryExecutionId = SDLExecutionId(runState.runId, runState.attemptId + 1)
    startRun(runState.appConfig, recoveryExecutionId, runState.runStartTime, actionsToSkip = actionsToSkip, initialSubFeeds = initialSubFeeds, dataObjectsState = dataObjectsState, stateStore = Some(stateStore))
  }

  /**
   * Start a simulation run.
   * This executes the DAG and returns all SubFeeds including the transformed DataFrames.
   * Only the exec phase is executed.
   * Actions and DataObjects needed have to be provided through implicit instanceRegistry parameter.
   * The Actions to execute are selected by appConfig.feedSel attribute.
   *
   * Note: this only works with DataFrameActions of the same SubFeed type, e.g. SparkSubFeed.
   *
   * @param appConfig application configuration
   * @param initialSubFeeds initial subfeeds for DataObjects at the beginning of the DAG
   * @param dataObjectsState state for incremental DataObjects
   * @param failOnMissingInputSubFeeds if true (default) all initial SubFeeds have to be provided in parameter `initialSubFeeds`. See also [[Environment.failSimulationOnMissingInputSubFeeds]].
   * @return tuple of list of final subfeeds and statistics (action count per RuntimeEventState)
   */
  def startSimulation(appConfig: SmartDataLakeBuilderConfig, initialSubFeeds: Seq[SparkSubFeed], dataObjectsState: Seq[DataObjectState] = Seq(), failOnMissingInputSubFeeds: Boolean = true)(implicit instanceRegistry: InstanceRegistry, session: SparkSession): (Seq[DataFrameSubFeed], Map[RuntimeEventState,Int]) = {
    Environment._failSimulationOnMissingInputSubFeeds = Some(failOnMissingInputSubFeeds)
    implicit val hadoopConf: Configuration = session.sparkContext.hadoopConfiguration
    val (subFeeds, stats) = exec(appConfig, SDLExecutionId.executionId1, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now, actionsToSkip = Map(), initialSubFeeds = initialSubFeeds, dataObjectsState = dataObjectsState, stateStore = None, stateListeners = Seq(), simulation = true, globalConfig = GlobalConfig())
    (subFeeds.map(_.asInstanceOf[SparkSubFeed]), stats)
  }

  /**
   * Starts a simulation run and registers all SDL first class objects that are defined in the config file which path is defined in parameter appConfig
   */
  def startSimulationWithConfigFile(appConfig: SmartDataLakeBuilderConfig, initialSubFeeds: Seq[SparkSubFeed], dataObjectsState: Seq[DataObjectState] = Seq())(session: SparkSession): (Seq[SubFeed], Map[RuntimeEventState, Int]) = {
    loadConfigIntoInstanceRegistry(appConfig, session)
    startSimulation(appConfig, initialSubFeeds, dataObjectsState)(this.instanceRegistry, session)
  }

  def loadConfigIntoInstanceRegistry(appConfig: SmartDataLakeBuilderConfig, session: SparkSession): Unit = {
    val config = ConfigLoader.loadConfigFromFilesystem(appConfig.configuration.get, session.sparkContext.hadoopConfiguration)
    ConfigParser.parse(config, this.instanceRegistry)
  }

  /**
   * Start run.
   * @return tuple of list of final subfeeds and statistics (action count per RuntimeEventState)
   */
  private[smartdatalake] def startRun(appConfig: SmartDataLakeBuilderConfig, executionId: SDLExecutionId = SDLExecutionId.executionId1, runStartTime: LocalDateTime = LocalDateTime.now, attemptStartTime: LocalDateTime = LocalDateTime.now, actionsToSkip: Map[ActionId, RuntimeInfo] = Map(), initialSubFeeds: Seq[SubFeed] = Seq(), dataObjectsState: Seq[DataObjectState] = Seq(), stateStore: Option[ActionDAGRunStateStore[_]] = None, simulation: Boolean = false)(implicit hadoopConf: Configuration): (Seq[SubFeed], Map[RuntimeEventState, Int]) = {
    AppUtil.setSdlbRunLoggerContext(appConfig, Some(executionId), Some(runStartTime))

    // validate application config
    appConfig.validate()

    // log start parameters
    logger.info(s"Starting run: runId=${executionId.runId} attemptId=${executionId.attemptId} feedSel=${appConfig.feedSel} appName=${appConfig.appName} streaming=${appConfig.streaming} test=${appConfig.test} givenPartitionValues=${appConfig.getPartitionValues.map(x => "(" + x.mkString(",") + ")").getOrElse("None")}")
    logger.debug(s"Environment: " + sys.env.map(x => x._1 + "=" + x._2).mkString(" "))
    logger.debug(s"System properties: " + sys.props.toMap.map(x => x._1 + "=" + x._2).mkString(" "))

    // load config
    val config: Config = appConfig.configuration match {
      case Some(configuration) => ConfigLoader.loadConfigFromFilesystem(configuration.map(_.trim), hadoopConf)
      case None => ConfigLoader.loadConfigFromClasspath
    }
    require(config.hasPath("actions"), s"No configuration parsed or it does not have a section called actions")
    require(config.hasPath("dataObjects"), s"No configuration parsed or it does not have a section called dataObjects")

    // parse global config
    val globalConfig = GlobalConfig.from(config)
    Environment._globalConfig = globalConfig

    // parse config objects
    ConfigParser.parse(config, instanceRegistry) // share instance registry for custom code
    // Attention: if JVM is shared between different SDL jobs (e.g. Databricks cluster), this overrides values from earlier jobs!
    Environment._instanceRegistry = instanceRegistry

    val snapshotListener = new SnapshotStatusInfoListener()
    val incrementalListener = new IncrementalStatusInfoListener()
    val statusInfoListeners =
      if (Environment._globalConfig.statusInfo.isDefined)
        Seq(snapshotListener, incrementalListener)
      else Nil

    val stateListeners =
      globalConfig.stateListeners.map(_.listener) ++ Environment._additionalStateListeners ++ statusInfoListeners

    if (Environment._globalConfig.statusInfo.isDefined) {
      StatusInfoServer.start(snapshotListener, incrementalListener, Environment._globalConfig.statusInfo.get)
    }
    exec(appConfig, executionId, runStartTime, attemptStartTime, actionsToSkip, initialSubFeeds, dataObjectsState, stateStore, stateListeners, simulation, globalConfig)(instanceRegistry)
  }

  private[smartdatalake] def exec(appConfig: SmartDataLakeBuilderConfig, executionId: SDLExecutionId, runStartTime: LocalDateTime, attemptStartTime: LocalDateTime, actionsToSkip: Map[ActionId, RuntimeInfo], initialSubFeeds: Seq[SubFeed], dataObjectsState: Seq[DataObjectState], stateStore: Option[ActionDAGRunStateStore[_]], stateListeners: Seq[StateListener], simulation: Boolean, globalConfig: GlobalConfig)(implicit instanceRegistry: InstanceRegistry): (Seq[SubFeed], Map[RuntimeEventState, Int]) = {
    AppUtil.setSdlbRunLoggerContext(appConfig, Some(executionId), Some(runStartTime))

    // select actions by feedSel
    val actionsSelected = AppUtil.filterActionList(appConfig.feedSel, instanceRegistry.getActions.toSet).toSeq
    require(actionsSelected.nonEmpty, s"No action matched the given feed selector: ${appConfig.feedSel}. At least one action needs to be selected.")
    logger.info(s"selected actions ${actionsSelected.map(_.id).mkString(", ")}")
    if (appConfig.test.contains(TestMode.Config)) { // stop here if only config check
      logger.info(s"${appConfig.test.get}-Test successfull")
      return (Seq(), Map())
    }

    // filter actions to skip
    val actionIdsSelected = actionsSelected.map(_.id)
    val actionIdsToSkip = actionsToSkip.keys.toSeq
    val missingActionsToSkip = actionIdsToSkip.filterNot(id => actionIdsSelected.contains(id))
    if (missingActionsToSkip.nonEmpty) logger.warn(s"actions to skip ${missingActionsToSkip.mkString(" ,")} not found in selected actions")
    val actionIdsSkipped = actionIdsSelected.filter(id => actionIdsToSkip.contains(id))
    val actionsToExec = actionsSelected.filterNot(action => actionIdsToSkip.contains(action.id))
    if (actionsToExec.isEmpty) logger.warn(s"No actions to execute. All selected actions are skipped: ${actionIdsSkipped.mkString(", ")}")
    else logger.info(s"actions to execute ${actionsToExec.map(_.id).mkString(", ")}" + (if (actionIdsSkipped.nonEmpty) s"; actions skipped ${actionIdsSkipped.mkString(", ")}" else ""))

    // create and execute DAG
    logger.info(s"starting application ${appConfig.appName} runId=${executionId.runId} attemptId=${executionId.attemptId}")
    val serializableHadoopConf = new SerializableHadoopConfiguration(globalConfig.getHadoopConfiguration)
    val context = ActionPipelineContext(appConfig.feedSel, appConfig.appName, executionId, instanceRegistry, referenceTimestamp = Some(LocalDateTime.now), appConfig, runStartTime, attemptStartTime, simulation, actionsSelected = actionIdsSelected, actionsSkipped = actionIdsSkipped, serializableHadoopConf = serializableHadoopConf, globalConfig = globalConfig)
    val actionDAGRun = ActionDAGRun(actionsToExec, actionsToSkip, appConfig.getPartitionValues.getOrElse(Seq()), appConfig.parallelism, initialSubFeeds, dataObjectsState, stateStore, stateListeners)(context)
    val finalSubFeeds = try {
      if (simulation) {
        // check action type
        val (dataFrameActions, otherActions) = actionsToExec.partition(_.isInstanceOf[DataFrameActionImpl])
        require(otherActions.isEmpty, s"Simulation needs all selected actions to be instances of DataFrameActionImpl. This is not the case for ${otherActions.map(_.id).mkString(", ")}")
        // check subFeedType
        val actionBySubFeedType = dataFrameActions.map(_.asInstanceOf[DataFrameActionImpl]).groupBy(_.subFeedType)
        require(actionBySubFeedType.size == 1, s"Simulation needs all selected actions to be instances of DataFrameActionImpl of the same subFeedType, e.g. SparkSubFeed. There are ${actionBySubFeedType.map{case (subFeedType, actions) => s"${actions.size} of ${actions.head.subFeedType.typeSymbol.name} (${actions.map(_.id).mkString(",")})"}.mkString(" and ")}")
        actionDAGRun.init(context)
      } else {
        actionDAGRun.prepare(context)
        actionDAGRun.init(context)
        if (appConfig.isDryRun) { // stop here if only dry-run
          logger.info(s"${appConfig.test.get}-Test successful")
          return (Seq(), Map())
        }
        execActionDAG(actionDAGRun, actionsSelected, context)
      }
    } catch {
      case ex: DAGException if ex.severity >= ExceptionSeverity.SKIPPED =>
        // don't fail on not severe exceptions like having no data to process
        logger.warn(s"At least one action is ${ex.severity}")
        Seq()
      case ex: Throwable if Environment.simplifyFinalExceptionLog =>
        // simplify stacktrace of exceptions thrown... filter all entries once an entry appears from monix (the parallel execution framework SDL uses)
        throw LogUtil.simplifyStackTrace(ex)
    }

    // return result statistics as string
    (finalSubFeeds, actionDAGRun.getStatistics)
  }
  private[smartdatalake] def agentExec(appConfig: SmartDataLakeBuilderConfig, phase: ExecutionPhase, executionId: SDLExecutionId = SDLExecutionId.executionId1, runStartTime: LocalDateTime = LocalDateTime.now(), attemptStartTime: LocalDateTime = LocalDateTime.now(), initialSubFeeds: Seq[SubFeed] = Seq(), dataObjectsState: Seq[DataObjectState]= Seq(), stateStore: Option[ActionDAGRunStateStore[_]] = None, stateListeners: Seq[StateListener] = Seq(), simulation: Boolean = false, globalConfig: GlobalConfig = GlobalConfig(enableHive = false))(implicit instanceRegistry: InstanceRegistry): Seq[SubFeed] = {
    // create and execute DAG
    val actionsToExecute = instanceRegistry.getActions
    logger.info(s"starting agentExecution ${appConfig.appName} runId=${executionId.runId} attemptId=${executionId.attemptId}")
    val serializableHadoopConf = new SerializableHadoopConfiguration(globalConfig.getHadoopConfiguration)
    val context = ActionPipelineContext(appConfig.feedSel, appConfig.appName, executionId, instanceRegistry, referenceTimestamp = Some(LocalDateTime.now), appConfig, runStartTime, attemptStartTime, simulation, actionsSelected = actionsToExecute.map(_.id), actionsSkipped = Nil, serializableHadoopConf = serializableHadoopConf, globalConfig = globalConfig)
    val actionDAGRun = ActionDAGRun(actionsToExecute, Map(), appConfig.getPartitionValues.getOrElse(Seq()), appConfig.parallelism, initialSubFeeds, dataObjectsState, stateStore, stateListeners)(context)

    phase match {
      case Prepare =>
        actionDAGRun.prepare(context)
        Nil
      case Init =>
        actionDAGRun.prepare(context)
        actionDAGRun.init(context)
      case Exec =>
        actionDAGRun.prepare(context)
        actionDAGRun.init(context)
        actionDAGRun.exec(context)
    }
  }

  /**
   * Execute one action DAG iteration and call recursion if streaming mode
   * Must be implemented with tail recursion to avoid stack overflow error for long running streaming jobs.
   */
  @tailrec
  final def execActionDAG(actionDAGRun: ActionDAGRun, actionsSelected: Seq[Action], context: ActionPipelineContext, lastStartTime: Option[LocalDateTime] = None): Seq[SubFeed] = {
    AppUtil.setSdlbRunLoggerContext(context)

    // handle skipped actions for next execution of streaming mode
    val nextExec: Option[(ActionDAGRun, ActionPipelineContext, Option[LocalDateTime])] = if (context.appConfig.streaming && lastStartTime.nonEmpty && context.actionsSkipped.nonEmpty) {
      // we have to recreate the action DAG if there are skipped actions in the original DAG
      val newContext = context.copy(actionsSkipped = Seq())
      val newDag = ActionDAGRun(actionsSelected, Map[ActionId, RuntimeInfo](), context.appConfig.getPartitionValues.getOrElse(Seq()), context.appConfig.parallelism, actionDAGRun.initialSubFeeds, actionDAGRun.initialDataObjectsState, actionDAGRun.stateStore, actionDAGRun.stateListeners)(newContext)
      Some(newDag, newContext, None)
    } else {

      // wait for trigger interval
      lastStartTime.foreach { t =>
        val nextStartTime = t.plusSeconds(context.globalConfig.synchronousStreamingTriggerIntervalSec)
        val waitTimeSec = Duration.between(LocalDateTime.now, nextStartTime).getSeconds
        if (waitTimeSec > 0) {
          logger.info(s"sleeping $waitTimeSec seconds for synchronous streaming trigger interval")
          Thread.sleep(waitTimeSec * 1000)
        }
      }

      // execute DAG
      val startTime = LocalDateTime.now
      var finalRunState: ActionDAGRunState = null
      var subFeeds = try {
        actionDAGRun.exec(context)
      } finally {
        finalRunState = actionDAGRun.saveState(ExecutionPhase.Exec, changedActionId = None, isFinal = true)(context)
      }

      // Iterate execution in streaming mode
      if (context.appConfig.streaming) {
        if (actionsSelected.exists(!_.isAsynchronous)) {
          // if there are synchronous actions, we re-execute the dag
          if (!Environment.stopStreamingGracefully) {
            // increment runId only if not all actions are skipped
            val newContext = if (!finalRunState.isSkipped) {
              context.incrementRunId
            } else {
              logger.info(s"As all actions of run_id ${context.executionId.runId} are skipped, run_id is not incremented for next execution")
              context
            }
            // remove spark caches so that new data is read in next iteration
            //TODO: in the future it might be interesting to keep some DataFrames cached for performance reason...
            if (context.hasSparkSession) context.sparkSession.sqlContext.clearCache()
            // iterate execution
            // note that this re-executes also asynchronous actions - they have to handle by themself that they are already started
            Some(actionDAGRun.copy(executionId = newContext.executionId), newContext, Some(startTime))
          } else {
            if (actionsSelected.exists(_.isAsynchronous)) {
              if (context.hasSparkSession) {
                // stop active streaming queries
                context.sparkSession.streams.active.foreach(_.stop())
                // if there were exceptions, throw first one
                context.sparkSession.streams.awaitAnyTermination() // using awaitAnyTermination is the easiest way to throw exception of first streaming query terminated
              }
            }
            // otherwise everything went smooth
            logger.info("Stopped streaming gracefully")
            None
          }
        } else {
          // if there are no synchronous actions, we wait for termination of asynchronous streaming queries (if we don't wait, the main process will end and kill the streaming query threads...)
          if (!Environment.stopStreamingGracefully) {
            if (context.hasSparkSession) {
              context.sparkSession.streams.awaitAnyTermination()
              context.sparkSession.streams.active.foreach(_.stop()) // stopping other streaming queries gracefully
            }
            actionDAGRun.saveState(ExecutionPhase.Exec, changedActionId = None, isFinal = true)(context) // notify about this asynchronous iteration
          } else {
            if (context.hasSparkSession) {
              context.sparkSession.streams.active.foreach(_.stop())
            }
            logger.info("Stopped streaming gracefully")
          }
          None
        }
      } else None
    }
    // execute tail recursion
    if (nextExec.isDefined) execActionDAG(nextExec.get._1, actionsSelected, nextExec.get._2, nextExec.get._3)
    else Seq()
  }
}
