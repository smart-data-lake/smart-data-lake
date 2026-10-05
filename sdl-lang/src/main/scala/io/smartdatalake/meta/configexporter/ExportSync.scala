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
package io.smartdatalake.meta.configexporter

import io.smartdatalake.app.{BackendClient, GlobalConfig}
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.exporter.ExportType.ExportType
import io.smartdatalake.config.exporter.{ExportType, ExportWriter, StateRunId, StateSyncEndpoint}
import io.smartdatalake.config.{ConfigLoader, ConfigurationException}
import io.smartdatalake.util.misc.{ProductUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.ActionDAGRunState
import org.apache.commons.lang3.NotImplementedException
import org.apache.hadoop.conf.Configuration
import scopt.OptionParser

import scala.jdk.CollectionConverters._

/**
 * Which versions [[ExportSync]] transfers from source to target.
 */
object SyncVersions extends Enumeration {
  type SyncVersions = Value
  /** only the latest version of the source, if it is newer than the latest version of the target */
  val Latest: Value = Value("latest")
  /** all versions of the source which are newer than the latest version of the target */
  val Newer: Value = Value("newer")
  /** all versions of the source which are missing in the target */
  val All: Value = Value("all")
}

/**
 * @param types       document types per DataObject to synchronize, see [[ExportType]]
 * @param withState   if true, the state of workflow runs is synchronized as well
 * @param stateSource state directory to read the state from. Reading the state from 'uiBackend' is not supported.
 * @param stateTarget state directory or 'uiBackend' to write the state to. Defaults to `target` if it is 'uiBackend'.
 */
case class ExportSyncConfig(configPaths: Seq[String] = null,
                            source: String = null,
                            target: String = null,
                            types: Seq[ExportType] = ExportType.values.toSeq,
                            withState: Boolean = false,
                            stateSource: Option[String] = None,
                            stateTarget: Option[String] = None,
                            versions: SyncVersions.Value = SyncVersions.Newer,
                            includeRegex: String = ".*",
                            excludeRegex: Option[String] = None,
                            applicationRegex: String = ".*",
                            dryRun: Boolean = false,
                            stopOnError: Boolean = true
                           ) {
  def getStateSource: String = stateSource
    .getOrElse(throw ConfigurationException("stateSource must be given to synchronize state"))

  def getStateTarget: String = stateTarget.orElse(Option(target).filter(ExportSync.isUiBackend))
    .getOrElse(throw ConfigurationException("stateTarget must be given to synchronize state if target is not 'uiBackend'"))
}

/**
 * Synchronizes the documents exported per DataObject (schema, stats, lineage) and the state of workflow runs from a
 * source to a target, transferring the versions which are newer in the source.
 * Source and target can be any [[ExportWriter]] uri, e.g. upload from 'localfile:./viz/schema' to 'uiBackend',
 * or download from 'uiBackend' to 'localfile:./viz/schema'.
 */
object ExportSync extends SmartDataLakeLogger {

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType

  private val stateType = "state"

  protected val parser: OptionParser[ExportSyncConfig] = new OptionParser[ExportSyncConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('c', "config")
      .required()
      .action((value, c) => c.copy(configPaths = value.split(',').toIndexedSeq))
      .text("One or multiple configuration files or directories containing configuration files for SDLB, separated by comma. They define the DataObjects to synchronize and the global.uiBackend configuration.")
    opt[String]("source")
      .required()
      .action((value, c) => c.copy(source = value))
      .text("Source URI to read exported documents from. Can be 'localfile:./xyz', a Hadoop path, 'uiBackend' or any http/https URL. 'uiBackend' will use global.uiBackend configuration to download from UI backend.")
    opt[String]('t', "target")
      .required()
      .action((value, c) => c.copy(target = value))
      .text("Target URI to write exported documents to. Can be 'localfile:./xyz', a Hadoop path, 'uiBackend' or any http/https URL. 'uiBackend' will use global.uiBackend configuration to upload to UI backend.")
    opt[String]("types")
      .validate(value => {
        val unknown = value.split(',').map(_.trim).filterNot(t => t == stateType || ExportType.values.exists(_.toString == t))
        if (unknown.isEmpty) success else failure(s"Unknown types ${unknown.mkString(", ")}")
      })
      .action((value, c) => {
        val types = value.split(',').map(_.trim).toSeq
        c.copy(types = ExportType.values.toSeq.filter(t => types.contains(t.toString)), withState = types.contains(stateType))
      })
      .text(s"Types to synchronize, separated by comma. Possible values are ${(ExportType.values.toSeq.map(_.toString) :+ stateType).mkString(", ")}. Default: ${ExportType.values.mkString(",")}")
    opt[String]("stateSource")
      .action((value, c) => c.copy(stateSource = Some(value)))
      .text("State directory as given to SmartDataLakeBuilder with --state-path to read the state of workflow runs from. Reading the state from 'uiBackend' is not supported, as the UI backend stores it in a different format.")
    opt[String]("stateTarget")
      .action((value, c) => c.copy(stateTarget = Some(value)))
      .text("State directory as given to SmartDataLakeBuilder with --state-path, or 'uiBackend', to write the state of workflow runs to. Default: target if it is 'uiBackend'")
    opt[String]("versions")
      .validate(value => if (SyncVersions.values.exists(_.toString == value)) success else failure(s"Unknown versions mode $value"))
      .action((value, c) => c.copy(versions = SyncVersions.withName(value)))
      .text("Versions to transfer: 'latest' transfers only the latest version of the source if it is newer than the target, 'newer' all versions newer than the latest version of the target, 'all' all versions missing in the target. Default: newer")
    opt[String]('i', "includeRegex")
      .action((value, c) => c.copy(includeRegex = value))
      .text("Regular expression used to include DataObjects, matching DataObject ids. Default: .*")
    opt[String]('e', "excludeRegex")
      .action((value, c) => c.copy(excludeRegex = Some(value)))
      .text("Regular expression used to exclude DataObjects, matching DataObject ids. `excludeRegex` is applied after `includeRegex`. Default: no excludes")
    opt[String]('a', "applicationRegex")
      .action((value, c) => c.copy(applicationRegex = value))
      .text("Regular expression used to include applications when synchronizing state, matching application names. Default: .*")
    opt[Unit]("dryRun")
      .action((_, c) => c.copy(dryRun = true))
      .text("Only log which versions would be transferred, without writing them.")
    opt[String]('s', "stopOnError")
      .action((value, c) => c.copy(stopOnError = value.toBoolean))
      .text("If true, synchronization is stopped as soon as there is an error. Otherwise errors are logged, synchronization continues and fails at the end. Default: true")
    help("help").text("Synchronize exported DataObject schemas, statistics, column lineage and the state of workflow runs from a source to a target, e.g. to upload them to or download them from the UI backend. Only versions newer in the source are transferred.")
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, ExportSyncConfig()) match {
      case Some(syncConfig) =>
        logger.info(s"starting with configuration ${ProductUtil.formatObj(syncConfig)}")
        val result = sync(syncConfig)
        if (result.failed > 0) throw new IllegalStateException(s"Synchronization of ${result.failed} documents failed, see log for details")
      case None =>
        logAndThrowException(s"Aborting $appType after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  case class SyncResult(transferred: Int = 0, upToDate: Int = 0, failed: Int = 0) {
    def +(other: SyncResult): SyncResult = SyncResult(transferred + other.transferred, upToDate + other.upToDate, failed + other.failed)
  }

  def sync(config: ExportSyncConfig): SyncResult = {
    require(config.source != config.target, s"source and target must be different, but both are ${config.source}")
    val sdlConfig = ConfigLoader.loadConfigFromFilesystem(config.configPaths, new Configuration())
    val hadoopConf = GlobalConfig.from(sdlConfig).getHadoopConfiguration
    // create BackendClient only once, as it might need to authenticate
    lazy val backendClient = BackendClient(config.configPaths)
    def getBackendClient(uris: String*) = if (uris.exists(isUiBackend)) Some(backendClient) else None

    // documents per DataObject
    val exportResults = if (config.types.nonEmpty) {
      val dataObjectIds = if (sdlConfig.hasPath("dataObjects")) sdlConfig.getObject("dataObjects").keySet.asScala.toSeq.sorted.map(DataObjectId(_)) else Seq()
      val selectedIds = dataObjectIds
        .filter(id => id.id.matches(config.includeRegex) && (config.excludeRegex.isEmpty || !id.id.matches(config.excludeRegex.get)))
      logger.info(s"Synchronizing ${config.types.mkString(", ")} of ${selectedIds.size} DataObjects from ${config.source} to ${config.target}")
      val backendClientOpt = getBackendClient(config.source, config.target)
      val source = ExportWriter(config.source, config.configPaths, backendClientOpt, Some(hadoopConf))
      val target = ExportWriter(config.target, config.configPaths, backendClientOpt, Some(hadoopConf))
      config.types.map(tpe => tpe.toString -> selectedIds.map(id => syncDocument(tpe, id, source, target, config)).fold(SyncResult())(_ + _))
    } else Seq()

    // state of workflow runs
    val stateResults = if (config.withState) {
      val (stateSourceUri, stateTargetUri) = (config.getStateSource, config.getStateTarget)
      require(stateSourceUri != stateTargetUri, s"stateSource and stateTarget must be different, but both are $stateSourceUri")
      // the UI backend converts the state on upload, so it can not be written back to a state directory
      if (isUiBackend(stateSourceUri)) throw ConfigurationException("Downloading state from the UI backend is not supported, as the UI backend stores it in a different format than SDLB")
      val backendClientOpt = getBackendClient(stateSourceUri, stateTargetUri)
      val stateSource = StateSyncEndpoint(stateSourceUri, config.configPaths, backendClientOpt, Some(hadoopConf))
      val stateTarget = StateSyncEndpoint(stateTargetUri, config.configPaths, backendClientOpt, Some(hadoopConf))
      val applications = stateSource.listApplications().filter(_.matches(config.applicationRegex))
      logger.info(s"Synchronizing state of ${applications.size} applications from $stateSourceUri to $stateTargetUri")
      Seq(stateType -> applications.map(app => syncState(app, stateSource, stateTarget, config)).fold(SyncResult())(_ + _))
    } else Seq()

    val results = exportResults ++ stateResults
    results.foreach { case (tpe, r) => logger.info(s"$tpe: transferred=${r.transferred} upToDate=${r.upToDate} failed=${r.failed}${if (config.dryRun) " (dry run)" else ""}") }
    results.map(_._2).fold(SyncResult())(_ + _)
  }

  private def syncDocument(tpe: ExportType, dataObjectId: DataObjectId, source: ExportWriter, target: ExportWriter, config: ExportSyncConfig): SyncResult = {
    handleErrors(s"$tpe of ${dataObjectId.id}", config) {
      val versions = selectVersions(source.listVersions(tpe, dataObjectId), target.listVersions(tpe, dataObjectId), config.versions)
      // a target which keeps only the latest version needs only the latest version
      val versionsToWrite = if (target.keepsHistory) versions else versions.maxOption.toSeq
      var latestTargetDocument = if (versionsToWrite.nonEmpty) target.readLatest(tpe, dataObjectId) else None
      val transferred = versionsToWrite.map { version =>
        val document = source.readVersion(tpe, dataObjectId, version)
          .getOrElse(throw new IllegalStateException(s"version $version of $tpe for ${dataObjectId.id} listed but not found in ${config.source}"))
        // skip identical content, e.g. if the version of the target is the modification time of an unversioned file
        if (latestTargetDocument.map(normalize).contains(normalize(document))) {
          logger.info(s"$tpe of ${dataObjectId.id} version $version is identical to latest version of target, skipping")
          false
        } else {
          logger.info(s"${if (config.dryRun) "Would transfer" else "Transferring"} $tpe of ${dataObjectId.id} version $version")
          if (!config.dryRun) target.writeVersion(tpe, document, dataObjectId, version)
          latestTargetDocument = Some(document)
          true
        }
      }.count(identity)
      SyncResult(transferred = transferred, upToDate = if (transferred == 0) 1 else 0)
    }
  }

  private def syncState(application: String, source: StateSyncEndpoint, target: StateSyncEndpoint, config: ExportSyncConfig): SyncResult = {
    handleErrors(s"state of application $application", config) {
      // only versions 'all' needs all runs of the target, otherwise its latest run is enough
      val targetRuns = if (config.versions == SyncVersions.All) target.listRuns(application) else target.latestRun(application).toSeq
      val runs = selectVersions(source.listRuns(application), targetRuns, config.versions)
      val transferred = runs.map { run =>
        val stateJson = source.readState(application, run)
          .getOrElse(throw new IllegalStateException(s"state of application $application runId=${run.runId} attemptId=${run.attemptId} listed but not found in ${config.getStateSource}"))
        // the state of a run in progress is transferred once it is final
        if (!ActionDAGRunState.fromJson(stateJson).isFinal) {
          logger.info(s"state of application $application runId=${run.runId} attemptId=${run.attemptId} is not final, skipping")
          false
        } else {
          logger.info(s"${if (config.dryRun) "Would transfer" else "Transferring"} state of application $application runId=${run.runId} attemptId=${run.attemptId}")
          if (!config.dryRun) target.writeState(stateJson)
          true
        }
      }.count(identity)
      SyncResult(transferred = transferred, upToDate = if (transferred == 0) 1 else 0)
    }
  }

  /**
   * Select the versions to transfer from source to target, in ascending order.
   */
  private[configexporter] def selectVersions[T: Ordering](sourceVersions: Seq[T], targetVersions: Seq[T], mode: SyncVersions.Value): Seq[T] = {
    val ordering = implicitly[Ordering[T]]
    val latestTarget = targetVersions.maxOption
    def isNewer(v: T) = latestTarget.forall(ordering.gt(v, _))
    val selected = mode match {
      case SyncVersions.Latest => sourceVersions.maxOption.filter(isNewer).toSeq
      case SyncVersions.Newer => sourceVersions.filter(isNewer)
      case SyncVersions.All => sourceVersions.diff(targetVersions)
    }
    selected.distinct.sorted
  }

  private def handleErrors(context: String, config: ExportSyncConfig)(fn: => SyncResult): SyncResult = {
    try fn
    catch {
      // a source or target not supporting to read documents is a configuration error
      case ex: NotImplementedException => throw ConfigurationException(s"Cannot synchronize $context: ${ex.getMessage}", throwable = ex)
      case ex: Exception if !config.stopOnError =>
        logger.warn(s"Synchronizing $context failed: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        SyncResult(failed = 1)
    }
  }

  private def normalize(document: String): String = document.replace("\r\n", "\n").trim

  private[configexporter] def isUiBackend(uri: String): Boolean = uri.takeWhile(_ != ':').equalsIgnoreCase("uiBackend")
}
