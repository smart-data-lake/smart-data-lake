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

import java.io.File
import java.time.LocalDateTime

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ActionObjectId
import io.smartdatalake.config.{ConfigLoader, ConfigParser, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{LogUtil, MemoryUtils, SmartDataLakeLogger}
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{ResultRuntimeInfo, RuntimeEventState, SparkAction}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
 * This case class represents a default configuration for the App.
 * It is populated by parsing command-line arguments.
 * It also specifies default values.
 *
 * @param feedSel         Regex pattern to select the feed to execute.
 * @param applicationName Application name.
 * @param configuration   A configuration file or a directory containing configuration files.
 * @param master          The Spark master URL passed to SparkContext when in local mode.
 * @param deployMode      The Spark deploy mode passed to SparkContext when in local mode.
 * @param username        Kerberos user name (`username`@`kerberosDomain`) for local mode.
 * @param kerberosDomain  Kerberos domain (`username`@`kerberosDomain`) for local mode.
 * @param keytabPath      Path to Kerberos keytab file for local mode.
 * @param test            Run in test mode:
 *                        - "config": validate configuration
 *                        - "dry-run": execute "prepare" and "init" phase to check environment
 */
case class SmartDataLakeBuilderConfig(feedSel: String = null,
                                      applicationName: Option[String] = None,
                                      configuration: Option[String] = None,
                                      master: Option[String] = None,
                                      deployMode: Option[String] = None,
                                      username: Option[String] = None,
                                      kerberosDomain: Option[String] = None,
                                      keytabPath: Option[File] = None,
                                      partitionValues: Option[Seq[PartitionValues]] = None,
                                      multiPartitionValues: Option[Seq[PartitionValues]] = None,
                                      parallelism: Int = 1,
                                      statePath: Option[String] = None,
                                      overrideJars: Option[Seq[String]] = None,
                                      test: Option[TestMode.Value] = None
                                ) {
  def validate(): Unit = {
    assert(!applicationName.exists(_.contains({HadoopFileActionDAGRunStateStore.fileNamePartSeparator})), s"Application name must not contain character '${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}' ($applicationName)")
    assert(!master.contains("yarn") || deployMode.nonEmpty, "spark deploy-mode must be set if spark master=yarn")
    assert(partitionValues.isEmpty || multiPartitionValues.isEmpty, "partitionValues and multiPartitionValues cannot be defined at the same time")
    assert(statePath.isEmpty || applicationName.isDefined, "application name must be defined if state path is set")
  }
  def getPartitionValues: Option[Seq[PartitionValues]] = partitionValues.orElse(multiPartitionValues)
  val appName: String = applicationName.getOrElse(feedSel)
}
object TestMode extends Enumeration {
  type TestMode = Value

  /**
   * Test if config is valid.
   * Note that this only parses and validates the configuration. No attempts are made to check the environment (e.g. connection informations...).
   */
  val Config = Value("config")

  /**
   * Test the environment if connections can be initalized and spark lineage can be created.
   * Note that no changes are made to the environment if possible.
   * The test executes "prepare" and "init" phase, but not the "exec" phase of an SDLB run.
   */
  val DryRun = Value("dry-run")
}

/**
 * Abstract Smart Data Lake Command Line Application.
 */
abstract class SmartDataLakeBuilder extends SmartDataLakeLogger {

  // read version from package manifest (not defined if project is executed in IntellJ)
  val appVersion: String = Option(getClass.getPackage.getImplementationVersion).getOrElse("develop")
  val appType: String = getClass.getSimpleName.replaceAll("\\$$","") // remove $ from object name and use it as appType

  /**
   * Create a new SDL configuration.
   *
   * Could be used in the future to set default values.
   *
   * @return a new, initialized [[SmartDataLakeBuilderConfig]].
   */
  def initConfigFromEnvironment: SmartDataLakeBuilderConfig = SmartDataLakeBuilderConfig()

  /**
   * InstanceRegistry instance
   */
  val instanceRegistry: InstanceRegistry = new InstanceRegistry()

  /**
   * The Parser defines how to extract the options from the command line args.
   * Subclasses SmartDataLakeBuilder can define additional options to be extracted.
   */
  protected val parser: OptionParser[SmartDataLakeBuilderConfig] = new OptionParser[SmartDataLakeBuilderConfig](appType) {
    override def showUsageOnError = true

    head(appType, appVersion)
    opt[String]('f', "feed-sel")
      .required
      .action( (arg, config) => config.copy(feedSel = arg) )
      .text("Regex pattern to select the feed to execute.")
    opt[String]('n', "name")
      .action( (arg, config) => config.copy(applicationName = Some(arg)) )
      .text("Optional name of the application. If not specified feed-sel is used.")
    opt[String]('c', "config")
      .action( (arg, config) => config.copy(configuration = Some(arg)) )
      .text("One or multiple configuration files or directories containing configuration files, separated by comma.")
    opt[String]("partition-values")
      .action((arg, config) => config.copy(partitionValues = Some(PartitionValues.parseSingleColArg(arg))))
      .text(s"Partition values to process in format ${PartitionValues.singleColFormat}.")
    opt[String]("multi-partition-values")
      .action((arg, config) => config.copy(partitionValues = Some(PartitionValues.parseMultiColArg(arg))))
      .text(s"Multi partition values to process in format ${PartitionValues.multiColFormat}.")
    opt[Int]("parallelism")
      .action((arg, config) => config.copy(parallelism = arg))
      .text(s"Parallelism for DAG run.")
    opt[String]("state-path")
      .action((arg, config) => config.copy(statePath = Some(arg)))
      .text(s"Path to save run state files. Must be set to enable recovery in case of failures.")
    opt[String]("override-jars")
      .action((arg, config) => config.copy(overrideJars = Some(arg.split(','))))
      .text("Comma separated list of jars for child-first class loader. The jars must be present in classpath.")
    opt[String]("test")
      .action((arg, config) => config.copy(test = Some(TestMode.withName(arg))))
      .text("Run in test mode: config -> validate configuration, dry-run -> execute prepare- and init-phase only to check environment and spark lineage")
    help("help").text("Display the help text.")
    version("version").text("Display version information.")
  }


  /**
   * Parses the supplied (command line) arguments.
   *
   * This method parses command line arguments and creates the corresponding [[SmartDataLakeBuilderConfig]]
   *
   * @param args an Array of command line arguments.
   * @param config a configuration initialized with default values.
   * @return a new configuration with default values overwritten from the supplied command line arguments.
   */
  def parseCommandLineArguments(args: Array[String], config: SmartDataLakeBuilderConfig): Option[SmartDataLakeBuilderConfig] = {
    parser.parse(args, config)
  }

  /**
   * Run the application with the provided configuration.
   *
   * @param appConfig Application configuration (parsed from command line).
   */
  def run(appConfig: SmartDataLakeBuilderConfig): Map[RuntimeEventState,Int] = try {
    if (appConfig.statePath.isDefined) {
      assert(appConfig.applicationName.nonEmpty, "Application name must be defined if statePath is set")
      // check if latest run succeeded
      val appName = appConfig.applicationName.get
      val stateStore = HadoopFileActionDAGRunStateStore(appConfig.statePath.get, appName)
      val latestRunId = stateStore.getLatestRunId
      if (latestRunId.isDefined) {
        val latestStateFile = stateStore.getLatestState(latestRunId)
        val latestRunState = stateStore.recoverRunState(latestStateFile)
        if (latestRunState.isFailed) {
          // start recovery
          recoverRun(appConfig, stateStore, latestRunState)._2
        } else {
          startRun(appConfig, runId = latestRunState.runId+1, stateStore = Some(stateStore))._2
        }
      } else {
        startRun(appConfig, stateStore = Some(stateStore))._2
      }
    } else startRun(appConfig)._2
  } finally {
    // make sure memory logger timer task is stopped
    MemoryUtils.stopMemoryLogger()
  }

  /**
   * Recover previous failed run.
   */
  private def recoverRun(appConfig: SmartDataLakeBuilderConfig, stateStore: ActionDAGRunStateStore[_ <: StateId], runState: ActionDAGRunState): (Seq[SubFeed], Map[RuntimeEventState,Int]) = {
    logger.info(s"recovering application ${appConfig.applicationName.get} runId=${runState.runId} lastAttemptId=${runState.attemptId}")
    // skip all succeeded actions
    val actionsToSkip = runState.actionsState
      .filter { case (id,info) => info.state==RuntimeEventState.SUCCEEDED }
    val actionIdsToSkip = actionsToSkip
      .map { case (id,info) => id }.toSeq
    val initialSubFeeds = actionsToSkip.flatMap(_._2.results.map(_.subFeed)).toSeq
    // start run, increase attempt counter
    startRun(runState.appConfig, runState.runId, runState.attemptId+1, runState.runStartTime, actionIdsToSkip = actionIdsToSkip, initialSubFeeds = initialSubFeeds, stateStore = Some(stateStore))
  }

  /**
   * Start a simulation run.
   * This executes the DAG and returns all subfeeds including the transformed DataFrames.
   * Only prepare and init are executed.
   * All initial subfeeds must be provided as input.
   *
   * Note: this only works with SparkActions for now
   * @param appConfig application configuration
   * @param initialSubFeeds initial subfeeds for DataObjects at the beginning of the DAG
   * @return tuple of list of final subfeeds and statistics (action count per RuntimeEventState)
   */
  def startSimulation(appConfig: SmartDataLakeBuilderConfig, initialSubFeeds: Seq[SparkSubFeed])(implicit instanceRegistry: InstanceRegistry, session: SparkSession): (Seq[SparkSubFeed], Map[RuntimeEventState,Int]) = {
    val (subFeeds, stats) = exec(appConfig, runId = 1, attemptId = 1, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now, actionIdsToSkip = Seq(), initialSubFeeds = initialSubFeeds, stateStore = None, stateListeners = Seq(), simulation = true)
    (subFeeds.map(_.asInstanceOf[SparkSubFeed]), stats)
  }

  /**
   * Start run.
   * @return tuple of list of final subfeeds and statistics (action count per RuntimeEventState)
   */
  private def startRun(appConfig: SmartDataLakeBuilderConfig, runId: Int = 1, attemptId: Int = 1, runStartTime: LocalDateTime = LocalDateTime.now, attemptStartTime: LocalDateTime = LocalDateTime.now, actionIdsToSkip: Seq[ActionObjectId] = Seq(), initialSubFeeds: Seq[SubFeed] = Seq(), stateStore: Option[ActionDAGRunStateStore[_]] = None, simulation: Boolean = false) : (Seq[SubFeed], Map[RuntimeEventState,Int]) = {

    // validate application config
    appConfig.validate()

    // log start parameters
    logger.info(s"Starting run: runId=$runId attemptId=$attemptId feedSel=${appConfig.feedSel} appName=${appConfig.appName} test=${appConfig.test} givenPartitionValues=${appConfig.getPartitionValues.map(x => "("+x.mkString(",")+")").getOrElse("None")}")
    logger.debug(s"Environment: " + sys.env.map(x => x._1 + "=" + x._2).mkString(" "))
    logger.debug(s"System properties: " + sys.props.toMap.map(x => x._1 + "=" + x._2).mkString(" "))

    // load config
    val config: Config = appConfig.configuration match {
      case Some(configuration) => ConfigLoader.loadConfigFromFilesystem(configuration.split(',').toSeq)
      case None => ConfigLoader.loadConfigFromClasspath
    }
    require(config.hasPath("actions"), s"No configuration parsed or it does not have a section called actions")
    require(config.hasPath("dataObjects"), s"No configuration parsed or it does not have a section called dataObjects")

    // parse config objects
    Environment._instanceRegistry = ConfigParser.parse(config, instanceRegistry) // share instance registry for custom code
    Environment._globalConfig = GlobalConfig.from(config)
    val stateListeners = Environment._globalConfig.stateListeners.map(_.listener)

    // create Spark Session
    val session: SparkSession = Environment._globalConfig.createSparkSession(appConfig.appName, appConfig.master, appConfig.deployMode)
    LogUtil.setLogLevel(session.sparkContext)

    exec(appConfig, runId, attemptId, runStartTime, attemptStartTime, actionIdsToSkip, initialSubFeeds, stateStore, stateListeners, simulation)(Environment._instanceRegistry, session)
  }

  private def exec(appConfig: SmartDataLakeBuilderConfig, runId: Int, attemptId: Int, runStartTime: LocalDateTime, attemptStartTime: LocalDateTime, actionIdsToSkip: Seq[ActionObjectId], initialSubFeeds: Seq[SubFeed], stateStore: Option[ActionDAGRunStateStore[_]], stateListeners: Seq[StateListener], simulation: Boolean)(implicit instanceRegistry: InstanceRegistry, session: SparkSession) : (Seq[SubFeed], Map[RuntimeEventState,Int]) = {

    // select actions by feedSel
    val actionsSelected = instanceRegistry.getActions.filter(_.metadata.flatMap(_.feed).exists(_.matches(appConfig.feedSel)))
    require(actionsSelected.nonEmpty, s"No action matched the given feed selector: ${appConfig.feedSel}. At least one action needs to be selected.")
    logger.info(s"selected actions ${actionsSelected.map(_.id).mkString(", ")}")
    if (appConfig.test.contains(TestMode.Config)) { // stop here if only config check
      logger.info(s"${appConfig.test.get}-Test successfull")
      return (Seq(), Map())
    }

    // filter actions to skip
    val actionIdsSelected = actionsSelected.map(_.id)
    val missingActionsToSkip = actionIdsToSkip.filterNot( id => actionIdsSelected.contains(id))
    if (missingActionsToSkip.nonEmpty) logger.warn(s"actions to skip ${missingActionsToSkip.mkString(" ,")} not found in selected actions")
    val actionIdsSkipped = actionIdsSelected.filter( id => actionIdsToSkip.contains(id))
    val actionsToExec = actionsSelected.filterNot( action => actionIdsToSkip.contains(action.id))
    require(actionsToExec.nonEmpty, s"No actions to execute. All selected actions are skipped (${actionIdsSkipped.mkString(", ")})")
    logger.info(s"actions to execute ${actionsToExec.map(_.id).mkString(", ")}" + (if (actionIdsSkipped.nonEmpty) s", actions skipped ${actionIdsSkipped.mkString(", ")}" else ""))

    // create and execute DAG
    logger.info(s"starting application ${appConfig.appName} runId=$runId attemptId=$attemptId")
    implicit val context: ActionPipelineContext = ActionPipelineContext(appConfig.feedSel, appConfig.appName, runId, attemptId, instanceRegistry, referenceTimestamp = Some(LocalDateTime.now), appConfig, runStartTime, attemptStartTime, simulation)
    val actionDAGRun = ActionDAGRun(actionsToExec, runId, attemptId, appConfig.getPartitionValues.getOrElse(Seq()), appConfig.parallelism, initialSubFeeds, stateStore, stateListeners)
    val finalSubFeeds = try {
      if (simulation) {
        require(actionsToExec.forall(_.isInstanceOf[SparkAction]), s"Simulation needs all selected actions to be instances of SparkAction. This is not the case for ${actionsToExec.filterNot(_.isInstanceOf[SparkAction]).map(_.id).mkString(", ")}")
        actionDAGRun.init
      } else {
        actionDAGRun.prepare
        actionDAGRun.init
        if (appConfig.test.contains(TestMode.DryRun)) { // stop here if only dry-run
          logger.info(s"${appConfig.test.get}-Test successfull")
          return (Seq(), Map())
        }
        val subFeeds = actionDAGRun.exec
        actionDAGRun.saveState(true)
        subFeeds
      }
    } catch {
      // don't fail an not severe exceptions like having no data to process
      case ex: DAGException if (ex.severity == ExceptionSeverity.SKIPPED) =>
        logger.warn(s"dag run is skipped because of ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        Seq()
    }

    // return result statistics as string
    (finalSubFeeds, actionDAGRun.getStatistics)
  }
}
