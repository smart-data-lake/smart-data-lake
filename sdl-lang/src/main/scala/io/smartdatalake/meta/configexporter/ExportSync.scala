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

import com.typesafe.config.{Config, ConfigRenderOptions, ConfigValue, ConfigValueFactory, ConfigValueType}
import configs.ConfigObject
import io.smartdatalake.app.{BackendClient, BuildVersionInfo, GlobalConfig}
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.exporter.ExportType.ExportType
import io.smartdatalake.config.exporter.{ExportType, ExportWriter, FileDescriptor, StateRunId, StateSyncEndpoint, UploadDefaults}
import io.smartdatalake.config.{ConfigLoader, ConfigParser, ConfigurationException}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.misc.HoconUtil.{getConfigValue, updateConfigValue}
import io.smartdatalake.util.misc.StringUtil.strToLowerCamelCase
import io.smartdatalake.util.misc.{CustomCodeUtil, HoconUtil, ProductUtil, ScaladocUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.ActionDAGRunState
import io.smartdatalake.workflow.action.generic.customlogic.CustomTransformMethodDef
import org.apache.commons.lang3.NotImplementedException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scopt.OptionParser

import scala.jdk.CollectionConverters._
import scala.util.Using

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
 * @param schemaSource source uri to read the documents exported per DataObject from (schema, stats, lineage).
 *                     Defaults to global.dataObjectsSchemaSource.
 * @param target       target uri to write the documents to
 * @param types        types to synchronize, see [[ExportType]]
 * @param stateSource  state directory to read the state from. Reading the state from 'uiBackend' is not supported.
 * @param stateTarget  state directory or 'uiBackend' to write the state to. Defaults to `target` if it is 'uiBackend'.
 * @param updateSearchIndex rebuild the search index of the target after synchronizing configuration, descriptions or
 *                     schemas, if the target has one. The state does not need it, as it is indexed on upload.
 * @param updateLineageIndex rebuild the column lineage index of the target if column lineage was transferred, if the
 *                     target has one. Otherwise the UI backend rebuilds it on the first read.
 */
case class ExportSyncConfig(configPaths: Seq[String] = null,
                            schemaSource: Option[String] = None,
                            target: String = null,
                            types: Seq[ExportType] = ExportSync.defaultTypes,
                            stateSource: Option[String] = None,
                            stateTarget: Option[String] = None,
                            versions: SyncVersions.Value = SyncVersions.Newer,
                            includeRegex: String = ".*",
                            excludeRegex: Option[String] = None,
                            applicationRegex: String = ".*",
                            dryRun: Boolean = false,
                            stopOnError: Boolean = true,
                            updateSearchIndex: Boolean = true,
                            updateLineageIndex: Boolean = true
                           ) {
  def getSchemaSource(globalConfig: GlobalConfig): String = schemaSource.orElse(globalConfig.dataObjectsSchemaSource)
    .getOrElse(throw ConfigurationException(s"schemaSource must be given to synchronize ${ExportType.dataObjectTypes.mkString(", ")} if global.dataObjectsSchemaSource is not set"))

  def getStateSource: String = stateSource
    .getOrElse(throw ConfigurationException("stateSource must be given to synchronize state"))

  def getStateTarget: String = stateTarget.orElse(Option(target).filter(ExportSync.isUiBackend))
    .getOrElse(throw ConfigurationException("stateTarget must be given to synchronize state if target is not 'uiBackend'"))
}

/**
 * Synchronizes the exported documents to a target:
 * - the resolved configuration as Json document, enriched with the origin of the configuration objects and the
 *   documentation of custom classes, created from the SDLB configuration
 * - the markdown description files of global.descriptionPath
 * - the documents exported per DataObject (schema, stats, lineage), transferring the versions which are newer in
 *   the schema source
 * - the state of workflow runs, transferring the runs which are newer in the state source
 * - finally the search index of the target is rebuilt over configuration, descriptions and schemas, and the column
 *   lineage index if column lineage was transferred
 *
 * Schema source and target can be any [[ExportWriter]] uri, e.g. upload from 'localfile:./viz/schema' to
 * 'uiBackend', or download from 'uiBackend' to 'localfile:./viz/schema'. If the target is a directory, the
 * configuration is written to file [[ExportWriter.configFilename]] in that directory.
 */
object ExportSync extends SmartDataLakeLogger {

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType

  /**
   * Types synchronized by default. Descriptions and state need additional configuration and must be selected
   * explicitly.
   */
  val defaultTypes: Seq[ExportType] = ExportType.ConfigJson +: ExportType.dataObjectTypes

  /**
   * Types the search index of the UI backend is built from.
   */
  val searchIndexTypes: Seq[ExportType] = Seq(ExportType.ConfigJson, ExportType.Description, ExportType.Schema)

  /**
   * Transformer types implemented as Java/Scala class which support a dynamic transform method. For these the
   * parameters of the transform method are exported to be shown in the UI.
   * They all hold the class name in configuration parameter [[CLASS_NAME_PARAMETER]].
   * Note that they are listed by name, as not all corresponding modules are on the classpath of the exporter.
   */
  private[configexporter] val customTransformerTypes: Set[String] = Set(
    "ScalaClassSparkDfsTransformer",
    "ScalaClassSparkDfTransformer",
    "ScalaClassGenericDfsTransformer",
    "ScalaClassGenericDfTransformer",
    "ScalaClassSnowparkDfsTransformer",
    "ScalaClassSnowparkDfTransformer"
  )

  /**
   * Name of the configuration parameter holding the class name of the transformer types listed in
   * [[customTransformerTypes]].
   */
  private[configexporter] val CLASS_NAME_PARAMETER = "className"

  protected val parser: OptionParser[ExportSyncConfig] = new OptionParser[ExportSyncConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('c', "config")
      .required()
      .action((value, c) => c.copy(configPaths = value.split(',').toIndexedSeq))
      .text("One or multiple configuration files or directories containing configuration files for SDLB, separated by comma. They are exported as configuration Json document, and define the DataObjects to synchronize, global.descriptionPath, global.dataObjectsSchemaSource and the global.uiBackend configuration.")
    opt[String]("schemaSource")
      .action((value, c) => c.copy(schemaSource = Some(value)))
      .text(s"Source URI to read the documents exported per DataObject (${ExportType.dataObjectTypes.mkString(", ")}) from. Can be 'localfile:./xyz', a Hadoop path, 'uiBackend' or any http/https URL. 'uiBackend' will use global.uiBackend configuration to download from UI backend. Default: global.dataObjectsSchemaSource")
    opt[String]('t', "target")
      .required()
      .action((value, c) => c.copy(target = value))
      .text(s"Target URI to write exported documents to. Can be 'localfile:./xyz', a Hadoop path, 'uiBackend' or any http/https URL. 'uiBackend' will use global.uiBackend configuration to upload to UI backend. 'localfile:' and Hadoop paths are directories, the configuration is written to file ${ExportWriter.configFilename} in it.")
    opt[String]("types")
      .validate(value => {
        val unknown = value.split(',').map(_.trim).filterNot(t => ExportType.values.exists(_.toString == t))
        if (unknown.isEmpty) success else failure(s"Unknown types ${unknown.mkString(", ")}")
      })
      .action((value, c) => {
        val types = value.split(',').map(_.trim).toSeq
        c.copy(types = ExportType.values.toSeq.filter(t => types.contains(t.toString)))
      })
      .text(s"Types to synchronize, separated by comma. Possible values are ${ExportType.values.mkString(", ")}. '${ExportType.Description}' uploads the markdown files of global.descriptionPath, '${ExportType.State}' needs --stateSource. Default: ${defaultTypes.mkString(",")}")
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
      .text("Only log which documents would be transferred, without writing them.")
    opt[String]('s', "stopOnError")
      .action((value, c) => c.copy(stopOnError = value.toBoolean))
      .text("If true, synchronization is stopped as soon as there is an error. Otherwise errors are logged, synchronization continues and fails at the end. Default: true")
    opt[Boolean]("updateSearchIndex")
      .action((value, c) => c.copy(updateSearchIndex = value))
      .text(s"Rebuild the search index of the target after synchronizing ${searchIndexTypes.mkString(", ")}. Supported by 'uiBackend', which does not rebuild an unchanged index. Default: true")
    opt[Boolean]("updateLineageIndex")
      .action((value, c) => c.copy(updateLineageIndex = value))
      .text(s"Rebuild the column lineage index of the target if ${ExportType.Lineage} was transferred. Supported by 'uiBackend', which otherwise rebuilds it on the first read. Default: true")
    help("help").text("Export the resolved configuration and the description files, and synchronize exported DataObject schemas, statistics, column lineage and the state of workflow runs from a source to a target, e.g. to upload them to or download them from the UI backend. Only versions newer in the source are transferred.")
  }

  /**
   * Use the following maven profile to add an export step to your build:
   * ```
   *     <profile>
   *         <id>export-config</id>
   *         <build>
   *             <plugins>
   *                 <plugin>
   *                     <groupId>org.codehaus.mojo</groupId>
   *                     <artifactId>exec-maven-plugin</artifactId>
   *                     <version>3.1.0</version>
   *                     <executions>
   *                         <execution>
   *                             <id>export-config</id>
   *                             <phase>package</phase>
   *                             <goals>
   * <!-- use exec instead of java, so that the exporter runs in a separate jvm from mvn -->
   *                                 <goal>exec</goal>
   *                             </goals>
   *                             <configuration>
   *                                 <executable>java</executable>
   *                                 <longClasspath>true</longClasspath>
   *                                 <arguments>
   *                                     <argument>-Dlog4j.configurationFile=log4j2.yml</argument>
   *                                     <argument>-classpath</argument>
   *                                     <classpath/>
   *                                     <argument>io.smartdatalake.meta.configexporter.ExportSync</argument>
   *                                     <argument>--config</argument>
   *                                     <argument>./config,./envConfig/dev.conf</argument>
   *                                     <argument>--types</argument>
   *                                     <argument>configJson</argument>
   *                                     <argument>--target</argument>
   *                                     <argument>localfile:./viz</argument>
   *                                 </arguments>
   *                                 <classpathScope>runtime</classpathScope>
   *                             </configuration>
   *                         </execution>
   *                     </executions>
   *                 </plugin>
   *             </plugins>
   *         </build>
   *     </profile>
   * ```
   *
   * The new profile can then be executed by the following maven command line: `mvn exec:exec@export-config -Pexport-config`
   */
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

  /**
   * @param missingInSource number of DataObjects without document in the source
   */
  case class SyncResult(transferred: Int = 0, upToDate: Int = 0, failed: Int = 0, missingInSource: Int = 0) {
    def +(other: SyncResult): SyncResult = SyncResult(transferred + other.transferred, upToDate + other.upToDate, failed + other.failed, missingInSource + other.missingInSource)
  }

  def sync(config: ExportSyncConfig): SyncResult = {
    val sdlConfig = ConfigLoader.loadConfigFromFilesystem(config.configPaths, new Configuration())
    val globalConfig = GlobalConfig.from(sdlConfig)
    implicit val hadoopConf: Configuration = globalConfig.getHadoopConfiguration
    // create BackendClient only once, as it might need to authenticate
    lazy val backendClient = BackendClient(config.configPaths)
    def getBackendClient(uris: String*) = if (uris.exists(isUiBackend)) Some(backendClient) else None
    lazy val target = ExportWriter(config.target, config.configPaths, getBackendClient(config.target), Some(hadoopConf))
    // configuration and descriptions are versioned with the version of the application, unless the target defines it
    val appVersion = if (!config.target.contains("version=")) Some(getAppVersion) else None

    // configuration
    val configResults = if (config.types.contains(ExportType.ConfigJson)) {
      val result = handleErrors(s"${ExportType.ConfigJson}", config) {
        val configJson = exportConfigJson(sdlConfig, config.configPaths)
        logger.info(s"${if (config.dryRun) "Would write" else "Writing"} configuration to ${config.target}")
        if (!config.dryRun) target.writeConfig(configJson, appVersion)
        SyncResult(transferred = 1)
      }
      Seq(ExportType.ConfigJson.toString -> result)
    } else Seq()

    // description files
    val descriptionResults = if (config.types.contains(ExportType.Description)) {
      val result = handleErrors(s"${ExportType.Description}", config) {
        val descriptionPath = globalConfig.descriptionPath
          .getOrElse(throw ConfigurationException("global.descriptionPath must be set to synchronize descriptions"))
        syncDescriptions(new Path(descriptionPath), target, appVersion, config)
      }
      Seq(ExportType.Description.toString -> result)
    } else Seq()

    // documents per DataObject
    val dataObjectTypes = config.types.filter(ExportType.dataObjectTypes.contains)
    val exportResults = if (dataObjectTypes.nonEmpty) {
      val sourceUri = config.getSchemaSource(globalConfig)
      require(sourceUri != config.target, s"schemaSource and target must be different, but both are $sourceUri")
      val dataObjectIds = if (sdlConfig.hasPath("dataObjects")) sdlConfig.getObject("dataObjects").keySet.asScala.toSeq.sorted.map(DataObjectId(_)) else Seq()
      val selectedIds = dataObjectIds
        .filter(id => id.id.matches(config.includeRegex) && (config.excludeRegex.isEmpty || !id.id.matches(config.excludeRegex.get)))
      logger.info(s"Synchronizing ${dataObjectTypes.mkString(", ")} of ${selectedIds.size} DataObjects from $sourceUri to ${config.target}")
      val source = ExportWriter(sourceUri, config.configPaths, getBackendClient(sourceUri), Some(hadoopConf))
      dataObjectTypes.map { tpe =>
        // list the versions of all DataObjects at once if supported, instead of a request per DataObject
        val sourceVersions = listAllVersions(tpe, source, latestOnly = config.versions == SyncVersions.Latest, config)
        val targetVersions = listAllVersions(tpe, target, latestOnly = config.versions != SyncVersions.All, config)
        val result = selectedIds.map(id => syncDocument(tpe, id, source, sourceUri, sourceVersions, target, targetVersions, config)).fold(SyncResult())(_ + _)
        if (selectedIds.nonEmpty && result.missingInSource == selectedIds.size) {
          // documents found in the source, which do not belong to a DataObject of the configuration
          val otherIds = sourceVersions.map(_.filter(_._2.nonEmpty).keys.map(_.id).toSeq.sorted).getOrElse(Seq())
          logger.warn(s"No $tpe found in $sourceUri for any of the ${selectedIds.size} DataObjects. Check that this is the right source and that its documents belong to the DataObjects of the configuration." +
            (if (otherIds.nonEmpty) s" Found $tpe of ${otherIds.size} other DataObjects, e.g. ${otherIds.take(5).mkString(", ")}" else ""))
        }
        tpe.toString -> result
      }
    } else Seq()

    // search index over configuration, descriptions and schemas
    // it is updated even if nothing was transferred, to retry an update which failed before. The UI backend does not
    // rebuild an unchanged index.
    val searchIndexResults = if (config.updateSearchIndex && config.types.exists(searchIndexTypes.contains)) {
      // the index is built per configuration version. If neither configuration nor descriptions are synchronized,
      // the target indexes its latest version.
      val indexVersion = if (config.types.exists(Seq(ExportType.ConfigJson, ExportType.Description).contains)) appVersion else None
      val result = handleErrors("search index", config) {
        if (config.dryRun) logger.info(s"Would update search index of ${config.target}")
        else if (!target.updateSearchIndex(indexVersion)) logger.info(s"${config.target} has no search index to update")
        SyncResult()
      }
      // only reported if it failed, as it is no transferred document
      if (result.failed > 0) Seq("searchIndex" -> result) else Seq()
    } else Seq()

    // column lineage index, only if lineage was transferred, as the update forces a rebuild
    val lineageTransferred = exportResults.exists { case (tpe, r) => tpe == ExportType.Lineage.toString && r.transferred > 0 }
    val lineageIndexResults = if (config.updateLineageIndex && lineageTransferred) {
      val result = handleErrors("column lineage index", config) {
        if (config.dryRun) logger.info(s"Would update column lineage index of ${config.target}")
        else if (!target.updateLineageIndex()) logger.info(s"${config.target} has no column lineage index to update")
        SyncResult()
      }
      // only reported if it failed, as it is no transferred document
      if (result.failed > 0) Seq("lineageIndex" -> result) else Seq()
    } else Seq()

    // state of workflow runs
    val stateResults = if (config.types.contains(ExportType.State)) {
      val (stateSourceUri, stateTargetUri) = (config.getStateSource, config.getStateTarget)
      require(stateSourceUri != stateTargetUri, s"stateSource and stateTarget must be different, but both are $stateSourceUri")
      // the UI backend converts the state on upload, so it can not be written back to a state directory
      if (isUiBackend(stateSourceUri)) throw ConfigurationException("Downloading state from the UI backend is not supported, as the UI backend stores it in a different format than SDLB")
      val backendClientOpt = getBackendClient(stateSourceUri, stateTargetUri)
      val stateSource = StateSyncEndpoint(stateSourceUri, config.configPaths, backendClientOpt, Some(hadoopConf))
      val stateTarget = StateSyncEndpoint(stateTargetUri, config.configPaths, backendClientOpt, Some(hadoopConf))
      val applications = stateSource.listApplications().filter(_.matches(config.applicationRegex))
      logger.info(s"Synchronizing state of ${applications.size} applications from $stateSourceUri to $stateTargetUri")
      Seq(ExportType.State.toString -> applications.map(app => syncState(app, stateSource, stateTarget, config)).fold(SyncResult())(_ + _))
    } else Seq()

    val results = configResults ++ descriptionResults ++ exportResults ++ searchIndexResults ++ lineageIndexResults ++ stateResults
    results.foreach { case (tpe, r) => logger.info(s"$tpe: transferred=${r.transferred} upToDate=${r.upToDate} missingInSource=${r.missingInSource} failed=${r.failed}${if (config.dryRun) " (dry run)" else ""}") }
    results.map(_._2).fold(SyncResult())(_ + _)
  }

  def getAppVersion: String = {
    BuildVersionInfo.appVersionInfo.map(_.version).getOrElse(UploadDefaults.versionDefault)
  }

  private def listAllVersions(tpe: ExportType, writer: ExportWriter, latestOnly: Boolean, config: ExportSyncConfig): Option[Map[DataObjectId, Seq[Long]]] = {
    try writer.listAllVersions(tpe, latestOnly)
    catch {
      case ex: Exception if !config.stopOnError =>
        logger.warn(s"Listing $tpe versions of all DataObjects failed, listing them per DataObject: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        None
    }
  }

  private def syncDocument(tpe: ExportType, dataObjectId: DataObjectId,
                           source: ExportWriter, sourceUri: String, sourceVersions: Option[Map[DataObjectId, Seq[Long]]],
                           target: ExportWriter, targetVersions: Option[Map[DataObjectId, Seq[Long]]],
                           config: ExportSyncConfig): SyncResult = {
    def getVersions(writer: ExportWriter, allVersions: Option[Map[DataObjectId, Seq[Long]]]) =
      allVersions.map(_.getOrElse(dataObjectId, Seq())).getOrElse(writer.listVersions(tpe, dataObjectId))
    handleErrors(s"$tpe of ${dataObjectId.id}", config) {
      val sourceDocumentVersions = getVersions(source, sourceVersions)
      if (sourceDocumentVersions.isEmpty) {
        logger.debug(s"No $tpe of ${dataObjectId.id} found in $sourceUri")
        SyncResult(missingInSource = 1)
      } else {
        val targetDocumentVersions = getVersions(target, targetVersions)
        val versions = selectVersions(sourceDocumentVersions, targetDocumentVersions, config.versions)
        // a target which keeps only the latest version needs only the latest version
        val versionsToWrite = if (target.keepsHistory) versions else versions.maxOption.toSeq
        var latestTargetDocument = if (versionsToWrite.nonEmpty) targetDocumentVersions.maxOption.flatMap(target.readVersion(tpe, dataObjectId, _)) else None
        val transferred = versionsToWrite.map { version =>
          val document = source.readVersion(tpe, dataObjectId, version)
            .getOrElse(throw new IllegalStateException(s"version $version of $tpe for ${dataObjectId.id} listed but not found in $sourceUri"))
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
      case ex: ConfigurationException => throw ex
      case ex: Exception if !config.stopOnError =>
        logger.warn(s"Synchronizing $context failed: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        SyncResult(failed = 1)
    }
  }

  /**
   * Upload the markdown description files, and delete the files on the target which no longer exist.
   */
  private def syncDescriptions(path: Path, target: ExportWriter, version: Option[String], config: ExportSyncConfig)(implicit hadoopConf: Configuration): SyncResult = {
    implicit val filesystem: FileSystem = Environment.fileSystemFactory.getFileSystem(path, hadoopConf)
    if (!filesystem.exists(path)) throw ConfigurationException(s"Description path $path not found")
    val existingFiles = try {
      target.listFiles(version).map(f => (f.name.dropWhile(_ == '/'), f)).toMap
    } catch {
      case _: NotImplementedException =>
        logger.info(s"Target ${target.getClass.getSimpleName} does not support listing existing files.")
        Map[String, FileDescriptor]()
    }
    logger.info(s"Searching description files in $path")
    val files = RemoteIteratorWrapper(filesystem.listFiles(path, true)).filterNot(_.isDirectory).toSeq
    val relativePaths = files.map { f =>
      val relativePath = f.getPath.toString.substring(f.getPath.toString.lastIndexOf(path.toString) + path.toString.length + 1)
      val existingFile = existingFiles.get(relativePath)
      if (existingFile.exists(e => e.size == f.getLen && e.lastModified.getTime > f.getModificationTime)) {
        logger.info(s"Description $relativePath already uploaded")
        (relativePath, false)
      } else {
        logger.info(s"${if (config.dryRun) "Would upload" else "Uploading"} description $relativePath")
        if (!config.dryRun) {
          val content = Using.resource(filesystem.open(f.getPath))(_.readAllBytes)
          target.writeFile(content, relativePath, version)
        }
        (relativePath, true)
      }
    }
    // delete no longer existing files
    val filesToDelete = existingFiles.keySet.diff(relativePaths.map(_._1).toSet)
    filesToDelete.foreach { f =>
      logger.info(s"${if (config.dryRun) "Would delete" else "Deleting"} description $f")
      if (!config.dryRun) target.deleteFile(f, version)
    }
    val transferred = relativePaths.count(_._2)
    SyncResult(transferred = transferred + filesToDelete.size, upToDate = relativePaths.size - transferred)
  }

  /**
   * Export the SDLB configuration as one Json document, everything resolved and with some metadata enrichments.
   */
  private[configexporter] def exportConfigJson(sdlConfig: Config, configPaths: Seq[String]): String = {
    // remove additional config paths introduced by system properties...
    val configKeysToRemove = sdlConfig.root.keySet().asScala.diff(Set(ConfigParser.CONFIG_SECTION_ACTIONS, ConfigParser.CONFIG_SECTION_CONNECTIONS, ConfigParser.CONFIG_SECTION_DATAOBJECTS, ConfigParser.CONFIG_SECTION_GLOBAL))
    var config = configKeysToRemove.foldLeft(sdlConfig)((config, key) => config.withoutPath(key))
    // enrich origin of first class config objects
    config = enrichOrigin(configPaths, config)
    // enrich optional source documentation for custom classes from scaladoc
    config = enrichCustomClassScalaDoc(config)
    // enrich optional custom transformer parameter info
    config = enrichCustomTransformerParameters(config)
    // render config as json
    config.root.render(ConfigRenderOptions.concise())
  }

  private def enrichCustomClassScalaDoc(config: Config): Config = {
    // we are looking for className and type attributes
    def searchCondition(key: String, value: ConfigValue) = {
      val lccKey = strToLowerCamelCase(key)
      // condition
      value.valueType == ConfigValueType.STRING &&
      (lccKey == "className" || lccKey == "type") &&
      value.unwrapped.asInstanceOf[String].split('.').length > 2 // check if configuration value is full class name, e.g. it has at least three name parts
    }
    val classConfigurationPaths = HoconUtil.findInConfigObject(config.root, searchCondition)
    // enrich config with scala doc for config paths found
    logger.info(s"Enriching source documentation for ${classConfigurationPaths.length} custom classes from scaladoc")
    classConfigurationPaths.foldLeft(config) {
      case (config, path) =>
        val className = getConfigValue(config.root(), path).unwrapped().asInstanceOf[String]
        try {
          val scalaDoc = ScaladocUtil.getClassScalaDoc(className).map(ScaladocUtil.formatScaladocWithTags(_))
          if (scalaDoc.isDefined) updateConfigValue(config.root(), path.init :+ "_sourceDoc", ConfigValueFactory.fromAnyRef(scalaDoc.get)).asInstanceOf[ConfigObject].toConfig
          else config
        } catch {
          case e: Exception =>
            logger.warn(s"${e.getClass.getSimpleName}: ${e.getMessage} for $className when enriching configuration path ${path.mkString(".")}")
            config
        }
    }
  }

  private def enrichOrigin(configPaths: Seq[String], config: Config): Config = {
    val descriptionRegex = """([^\s,]*):\s*([0-9]+)(-[0-9]+)?""".r.unanchored
    val configSectionsToEnrich = Set(ConfigParser.CONFIG_SECTION_ACTIONS, ConfigParser.CONFIG_SECTION_CONNECTIONS, ConfigParser.CONFIG_SECTION_DATAOBJECTS)
    val configBasePaths = configPaths.map(new Path(_)).map {
      case p if p.getName.endsWith(".conf") => p.getParent.toString // if it is a file, take its parent folder as base path
      case p => p.toString
    }
    configSectionsToEnrich.filter(config.hasPath).foldLeft(config) {
      case (config, sectionKey) =>
        config.getConfig(sectionKey).root.keySet().asScala.foldLeft(config) {
          case (config, objectKey) =>
            val objectConfig = config.getConfig(s"$sectionKey.$objectKey")
            // parse origins description, as we can not access the detailed private properties
            // note that currently endLineNumber is not filled by Hocon parser, even though it is foreseen in the code.
            objectConfig.origin.description() match {
              case descriptionRegex(path, lineNumber, endLineNumber) =>
                // relativize path
                val relativePath = configBasePaths.find(path.contains)
                  .map(configPath => path.split(configPath).last.dropWhile("\\/".contains(_))).getOrElse(path)
                // add to config
                val origin = Seq(
                  Some("path" -> relativePath),
                  Some("lineNumber" -> lineNumber.toInt),
                  Option(endLineNumber).map("endLineNumber" -> _.toInt)
                ).flatten.toMap
                config.withValue(s"$sectionKey.$objectKey._origin", ConfigValueFactory.fromMap(origin.asJava))
            }
        }
    }
  }

  private def enrichCustomTransformerParameters(config: Config): Config = {
    // we are looking for all transformers which are implemented as Java/Scala class and support a dynamic transform method
    def searchCondition(key: String, value: ConfigValue) = {
      // condition
      value.valueType == ConfigValueType.STRING &&
        key == "type" &&
        customTransformerTypes.contains(value.unwrapped.asInstanceOf[String])
    }
    val customTransformerConfigurationPaths = HoconUtil.findInConfigObject(config.root, searchCondition)
    // enrich config with custom transformer parameters if available
    logger.info(s"Enriching custom transformer parameter information for ${customTransformerConfigurationPaths.length} transformers")
    customTransformerConfigurationPaths.foldLeft(config) {
      case (config, path) =>
        val className = getConfigValue(config.root(), path.init :+ CLASS_NAME_PARAMETER).unwrapped().asInstanceOf[String]
        val classInstance = CustomCodeUtil.getClassInstanceByName[CustomTransformMethodDef](className)
        val parameters = classInstance.customTransformMethod.map(CustomCodeUtil.analyzeMethodParameters(None, _))
        if (parameters.isDefined) {
          val parametersValue = ConfigValueFactory.fromIterable(parameters.get.map(p => ConfigValueFactory.fromMap(p.toMap.asJava)).asJava)
          updateConfigValue(config.root(), path.init :+ "_parameters", parametersValue).asInstanceOf[ConfigObject].toConfig
        }
        else config
    }
  }

  private def normalize(document: String): String = document.replace("\r\n", "\n").trim

  private[configexporter] def isUiBackend(uri: String): Boolean = uri.takeWhile(_ != ':').equalsIgnoreCase("uiBackend")
}
