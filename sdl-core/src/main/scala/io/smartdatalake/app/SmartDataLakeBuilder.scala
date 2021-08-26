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
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{ConfigLoader, ConfigParser, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.dag.{DAGException, ExceptionSeverity}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{LogUtil, MemoryUtils, SmartDataLakeLogger}
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{Action, RuntimeInfo, SDLExecutionId, SparkAction}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import java.io.File
import java.time.{Duration, LocalDateTime}

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
case class SmartDataLakeBuilderConfig(feedSel: String = null,
                                      applicationName: Option[String] = None,
                                      configuration: Option[Seq[String]] = None,
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
                                      test: Option[TestMode.Value] = None,
                                      streaming: Boolean = false
                                ) {
  def validate(): Unit = {
    assert(!applicationName.exists(_.contains({HadoopFileActionDAGRunStateStore.fileNamePartSeparator})), s"Application name must not contain character '${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}' ($applicationName)")
    assert(!master.contains("yarn") || deployMode.nonEmpty, "spark deploy-mode must be set if spark master=yarn")
    assert(partitionValues.isEmpty || multiPartitionValues.isEmpty, "partitionValues and multiPartitionValues cannot be defined at the same time")
    assert(statePath.isEmpty || applicationName.isDefined, "application name must be defined if state path is set")
    assert(!streaming || statePath.isDefined, "state path must be set if streaming is enabled")
  }
  def getPartitionValues: Option[Seq[PartitionValues]] = partitionValues.orElse(multiPartitionValues)
  val appName: String = applicationName.getOrElse(feedSel)
  def isDryRun: Boolean = test.contains(TestMode.DryRun)
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
    override def showUsageOnError = Some(true)

    head(appType, appVersion)
    opt[String]('f', "feed-sel")
      .required
      .action( (arg, config) => config.copy(feedSel = arg) )
      .valueName("<operation?><prefix:?><regex>[,<operation?><prefix:?><regex>...]")
      .text("""Select actions to execute by one or multiple expressions separated by comma (,). Results from multiple expressions are combined from left to right.
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
    opt[String]('n', "name")
      .action( (arg, config) => config.copy(applicationName = Some(arg)) )
      .text("Optional name of the application. If not specified feed-sel is used.")
    opt[Seq[String]]('c', "config")
      .action( (arg, config) => config.copy(configuration = Some(arg)) )
      .valueName("<file1>[,<file2>...]")
      .text("One or multiple configuration files or directories containing configuration files, separated by comma. Entries must be valid Hadoop URIs or a special URI with scheme \"cp\" which is treated as classpath entry.")
    opt[String]("partition-values")
      .action((arg, config) => config.copy(partitionValues = Some(PartitionValues.parseSingleColArg(arg))))
      .valueName(PartitionValues.singleColFormat)
      .text(s"Partition values to process for one single partition column.")
    opt[String]("multi-partition-values")
      .action((arg, config) => config.copy(partitionValues = Some(PartitionValues.parseMultiColArg(arg))))
      .valueName(PartitionValues.multiColFormat)
      .text(s"Partition values to process for multiple partitoin columns.")
    opt[Unit]('s', "streaming")
      .action((_, config) => config.copy(streaming = true))
      .text(s"Enable streaming mode for continuous processing.")
    opt[Int]("parallelism")
      .action((arg, config) => config.copy(parallelism = arg))
      .valueName("<int>")
      .text(s"Parallelism for DAG run.")
    opt[String]("state-path")
      .action((arg, config) => config.copy(statePath = Some(arg)))
      .valueName("<path>")
      .text(s"Path to save run state files. Must be set to enable recovery in case of failures.")
    opt[Seq[String]]("override-jars")
      .action((arg, config) => config.copy(overrideJars = Some(arg)))
      .valueName("<jar1>[,<jar2>...]")
      .text("Comma separated list of jar filenames for child-first class loader. The jars must be present in classpath.")
    opt[String]("test")
      .action((arg, config) => config.copy(test = Some(TestMode.withName(arg))))
      .valueName("<config|dry-run>")
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
    // invoke SDLPlugin if configured
    Environment.sdlPlugin.foreach(_.startup())
    // handle state if defined
    if (appConfig.statePath.isDefined && !appConfig.isDryRun) {
      assert(appConfig.applicationName.nonEmpty, "Application name must be defined if statePath is set")
      // check if latest run succeeded
      val appName = appConfig.applicationName.get
      val stateStore = HadoopFileActionDAGRunStateStore(appConfig.statePath.get, appName)
      val latestRunId = stateStore.getLatestRunId
      if (latestRunId.isDefined) {
        val latestStateId = stateStore.getLatestStateId(latestRunId)
          .getOrElse(throw new IllegalStateException(s"State for last runId=$latestRunId not found"))
        val latestRunState = stateStore.recoverRunState(latestStateId)
        if (latestRunState.isFailed) {
          // start recovery
          assert(appConfig == latestRunState.appConfig, s"There is a failed run to be recovered. Either you clean-up this state fail or the command line parameters given must match the parameters of the run to be recovered (${latestRunState.appConfig}")
          recoverRun(appConfig, stateStore, latestRunState)._2
        } else {
          val nextExecutionId = SDLExecutionId(latestRunState.runId + 1)
          startRun(appConfig, executionId = nextExecutionId, dataObjectsState = latestRunState.getDataObjectsState, stateStore = Some(stateStore))._2
        }
      } else {
        startRun(appConfig, stateStore = Some(stateStore))._2
      }
    } else startRun(appConfig)._2
  } finally {
    // make sure memory logger timer task is stopped
    MemoryUtils.stopMemoryLogger()
    // invoke SDLPlugin if configured
    Environment.sdlPlugin.foreach(_.shutdown())
  }

  /**
   * Recover previous failed run.
   */
  private[smartdatalake] def recoverRun[S<:StateId](appConfig: SmartDataLakeBuilderConfig, stateStore: ActionDAGRunStateStore[S], runState: ActionDAGRunState): (Seq[SubFeed], Map[RuntimeEventState,Int]) = {
    logger.info(s"recovering application ${appConfig.applicationName.get} runId=${runState.runId} lastAttemptId=${runState.attemptId}")
    // skip all succeeded actions
    val actionsToSkip = runState.actionsState
      .filter { case (id,info) => info.hasCompleted }
    val initialSubFeeds = actionsToSkip.flatMap(_._2.results.map(_.subFeed)).toSeq
    // get latest DataObject state and overwrite with current DataObject state
    val lastStateId = stateStore.getLatestStateId(Some(runState.runId-1))
    val lastRunState = lastStateId.map(stateStore.recoverRunState)
    val dataObjectsState = (lastRunState.map(_.getDataObjectsState.map(_.getEntry).toMap).getOrElse(Map()) ++ runState.getDataObjectsState.map(_.getEntry).toMap).values.toSeq
    // start run, increase attempt counter
    val recoveryExecutionId = SDLExecutionId(runState.runId, runState.attemptId + 1)
    startRun(runState.appConfig, recoveryExecutionId, runState.runStartTime, actionsToSkip = actionsToSkip, initialSubFeeds = initialSubFeeds, dataObjectsState = dataObjectsState, stateStore = Some(stateStore))
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
   * @param dataObjectsState state for incremental DataObjects
   * @return tuple of list of final subfeeds and statistics (action count per RuntimeEventState)
   */
  def startSimulation(appConfig: SmartDataLakeBuilderConfig, initialSubFeeds: Seq[SparkSubFeed], dataObjectsState: Seq[DataObjectState] = Seq())(implicit instanceRegistry: InstanceRegistry, session: SparkSession): (Seq[SparkSubFeed], Map[RuntimeEventState,Int]) = {
    val (subFeeds, stats) = exec(appConfig, SDLExecutionId.executionId1, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now, actionsToSkip = Map(), initialSubFeeds = initialSubFeeds, dataObjectsState = dataObjectsState, stateStore = None, stateListeners = Seq(), simulation = true)
    (subFeeds.map(_.asInstanceOf[SparkSubFeed]), stats)
  }

  /**
   * Start run.
   * @return tuple of list of final subfeeds and statistics (action count per RuntimeEventState)
   */
  private[smartdatalake] def startRun(appConfig: SmartDataLakeBuilderConfig, executionId: SDLExecutionId = SDLExecutionId.executionId1, runStartTime: LocalDateTime = LocalDateTime.now, attemptStartTime: LocalDateTime = LocalDateTime.now, actionsToSkip: Map[ActionId, RuntimeInfo] = Map(), initialSubFeeds: Seq[SubFeed] = Seq(), dataObjectsState: Seq[DataObjectState] = Seq(), stateStore: Option[ActionDAGRunStateStore[_]] = None, simulation: Boolean = false) : (Seq[SubFeed], Map[RuntimeEventState,Int]) = {

    // validate application config
    appConfig.validate()

    // log start parameters
    logger.info(s"Starting run: runId=${executionId.runId} attemptId=${executionId.attemptId} feedSel=${appConfig.feedSel} appName=${appConfig.appName} streaming=${appConfig.streaming} test=${appConfig.test} givenPartitionValues=${appConfig.getPartitionValues.map(x => "("+x.mkString(",")+")").getOrElse("None")}")
    logger.debug(s"Environment: " + sys.env.map(x => x._1 + "=" + x._2).mkString(" "))
    logger.debug(s"System properties: " + sys.props.toMap.map(x => x._1 + "=" + x._2).mkString(" "))

    // load config
    val config: Config = appConfig.configuration match {
      case Some(configuration) => ConfigLoader.loadConfigFromFilesystem(configuration.map(_.trim))
      case None => ConfigLoader.loadConfigFromClasspath
    }
    require(config.hasPath("actions"), s"No configuration parsed or it does not have a section called actions")
    require(config.hasPath("dataObjects"), s"No configuration parsed or it does not have a section called dataObjects")

    // parse config objects
    Environment._instanceRegistry = ConfigParser.parse(config, instanceRegistry) // share instance registry for custom code
    Environment._globalConfig = GlobalConfig.from(config)
    val stateListeners = Environment._globalConfig.stateListeners.map(_.listener) ++ Environment._additionalStateListeners

    // create Spark Session
    val session: SparkSession = Environment._globalConfig.createSparkSession(appConfig.appName, appConfig.master, appConfig.deployMode)
    LogUtil.setLogLevel(session.sparkContext)

    exec(appConfig, executionId, runStartTime, attemptStartTime, actionsToSkip, initialSubFeeds, dataObjectsState, stateStore, stateListeners, simulation)(Environment._instanceRegistry, session)
  }

  private[smartdatalake] def exec(appConfig: SmartDataLakeBuilderConfig, executionId: SDLExecutionId, runStartTime: LocalDateTime, attemptStartTime: LocalDateTime, actionsToSkip: Map[ActionId, RuntimeInfo], initialSubFeeds: Seq[SubFeed], dataObjectsState: Seq[DataObjectState], stateStore: Option[ActionDAGRunStateStore[_]], stateListeners: Seq[StateListener], simulation: Boolean)(implicit instanceRegistry: InstanceRegistry, session: SparkSession) : (Seq[SubFeed], Map[RuntimeEventState,Int]) = {

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
    val missingActionsToSkip = actionIdsToSkip.filterNot( id => actionIdsSelected.contains(id))
    if (missingActionsToSkip.nonEmpty) logger.warn(s"actions to skip ${missingActionsToSkip.mkString(" ,")} not found in selected actions")
    val actionIdsSkipped = actionIdsSelected.filter( id => actionIdsToSkip.contains(id))
    val actionsToExec = actionsSelected.filterNot( action => actionIdsToSkip.contains(action.id))
    require(actionsToExec.nonEmpty, s"No actions to execute. All selected actions are skipped (${actionIdsSkipped.mkString(", ")})")
    logger.info(s"actions to execute ${actionsToExec.map(_.id).mkString(", ")}" + (if (actionIdsSkipped.nonEmpty) s"; actions skipped ${actionIdsSkipped.mkString(", ")}" else ""))

    // create and execute DAG
    logger.info(s"starting application ${appConfig.appName} runId=${executionId.runId} attemptId=${executionId.attemptId}")
    val context: ActionPipelineContext = ActionPipelineContext(appConfig.feedSel, appConfig.appName, executionId, instanceRegistry, referenceTimestamp = Some(LocalDateTime.now), appConfig, runStartTime, attemptStartTime, simulation, actionsSelected = actionIdsSelected, actionsSkipped = actionIdsSkipped)
    val actionDAGRun = ActionDAGRun(actionsToExec, actionsToSkip, appConfig.getPartitionValues.getOrElse(Seq()), appConfig.parallelism, initialSubFeeds, dataObjectsState, stateStore, stateListeners)(session,context)
    val finalSubFeeds = try {
      if (simulation) {
        require(actionsToExec.forall(_.isInstanceOf[SparkAction]), s"Simulation needs all selected actions to be instances of SparkAction. This is not the case for ${actionsToExec.filterNot(_.isInstanceOf[SparkAction]).map(_.id).mkString(", ")}")
        actionDAGRun.init(session,context)
      } else {
        actionDAGRun.prepare(session,context)
        actionDAGRun.init(session,context)
        if (appConfig.isDryRun) { // stop here if only dry-run
          logger.info(s"${appConfig.test.get}-Test successful")
          return (Seq(), Map())
        }
        execActionDAG(actionDAGRun, actionsSelected, context)
      }
    } catch {
      // don't fail on not severe exceptions like having no data to process
      case ex: DAGException if ex.severity >= ExceptionSeverity.SKIPPED =>
        logger.warn(s"At least one action is ${ex.severity}")
        Seq()
    }

    // return result statistics as string
    (finalSubFeeds, actionDAGRun.getStatistics)
  }

  def execActionDAG(actionDAGRun: ActionDAGRun, actionsSelected: Seq[Action], context: ActionPipelineContext, lastStartTime: Option[LocalDateTime] = None)(implicit session: SparkSession): Seq[SubFeed] = {

    // handle skipped actions for next execution of streaming mode
    if (context.appConfig.streaming && lastStartTime.nonEmpty && context.actionsSkipped.nonEmpty) {
      // we have to recreate the action DAG if there are skipped actions in the original DAG
      val newContext = context.copy(actionsSkipped = Seq())
      val newDag = ActionDAGRun(actionsSelected, Map[ActionId, RuntimeInfo](), context.appConfig.getPartitionValues.getOrElse(Seq()), context.appConfig.parallelism, actionDAGRun.initialSubFeeds, actionDAGRun.initialDataObjectsState, actionDAGRun.stateStore, actionDAGRun.stateListeners)(session, newContext)
      execActionDAG(newDag, actionsSelected, newContext)
    }

    // wait for trigger interval
    lastStartTime.foreach { t =>
      val nextStartTime = t.plusSeconds(Environment.globalConfig.synchronousStreamingTriggerIntervalSec)
      val waitTimeSec = Duration.between(LocalDateTime.now, nextStartTime).getSeconds
      if (waitTimeSec>0) {
        logger.info(s"sleeping $waitTimeSec seconds for synchronous streaming trigger interval")
        Thread.sleep(waitTimeSec * 1000)
      }
    }

    // execute DAG
    val startTime = LocalDateTime.now
    var subFeeds = try {
      actionDAGRun.exec(session, context)
    } finally {
      actionDAGRun.saveState(true)(session, context)
    }

    // Iterate execution in streaming mode
    if (context.appConfig.streaming) {
      if (actionsSelected.exists(!_.isAsynchronous)) {
        // if there are synchronous actions, we re-execute the dag
        if (!Environment.stopStreamingGracefully) {
          // increment runId
          val newContext = context.incrementRunId
          // reset execution result before new execution
          actionsSelected.foreach(_.resetExecutionResult())
          // remove spark caches so that new data is read in next iteration
          //TODO: in the future it might be interesting to keep some DataFrames cached for performance reason...
          session.sqlContext.clearCache()
          // iterate execution
          // note that this re-executes also asynchronous actions - they have to handle by themself that they are already started
          subFeeds = execActionDAG(actionDAGRun.copy(executionId = newContext.executionId), actionsSelected, newContext, Some(startTime))
        } else {
          if (actionsSelected.exists(_.isAsynchronous)) {
            // stop active streaming queries
            session.streams.active.foreach(_.stop)
            // if there were exceptions, throw first one
            session.streams.awaitAnyTermination() // using awaitAnyTermination is the easiest way to throw exception of first streaming query terminated
          }
          // otherwise everything went smooth
          logger.info("Stopped streaming gracefully")
        }
      } else {
        // if there are no synchronous actions, we wait for termination of asynchronous streaming queries (if we don't wait, the main process will end and kill the streaming query threads...)
        if (!Environment.stopStreamingGracefully) {
          session.streams.awaitAnyTermination()
          session.streams.active.foreach(_.stop) // stopping other streaming queries gracefully
          actionDAGRun.saveState(true)(session, context) // notify about this asynchronous iteration
        } else {
          session.streams.active.foreach(_.stop)
          logger.info("Stopped streaming gracefully")
        }
      }
    }
    subFeeds
  }
}
