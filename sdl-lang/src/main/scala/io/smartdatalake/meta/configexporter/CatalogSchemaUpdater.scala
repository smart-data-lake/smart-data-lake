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

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.exporter.{ColumnDescriptionParser, ExportWriter}
import io.smartdatalake.config.{ConfigToolbox, ConfigurationException}
import io.smartdatalake.util.misc._
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CatalogMetadataApplier, CatalogMetadataChanges}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.hadoop.conf.Configuration
import scopt.OptionParser

import java.time.LocalDateTime

/**
 * What CatalogSchemaUpdater should do.
 */
object UpdaterMode extends Enumeration {
  type UpdaterMode = Value

  /**
   * Only report the changes which would be applied, without changing the catalog.
   */
  val Plan: UpdaterMode = Value("plan")

  /**
   * Create or update the tables in the catalog: missing tables, schema changes, comments,
   * primary and foreign keys.
   */
  val Apply: UpdaterMode = Value("apply")
}

case class CatalogSchemaUpdaterConfig(configPaths: Seq[String] = null,
                                      mode: UpdaterMode.Value = UpdaterMode.Plan,
                                      source: Option[String] = None,
                                      descriptionPath: Option[String] = None,
                                      includeRegex: String = ".*",
                                      excludeRegex: Option[String] = None,
                                      stopOnError: Boolean = true
                                     )

/**
 * Create and update the tables defined in the SDLB configuration and in the exported schemas in the catalog.
 *
 * This is the deployment time counterpart of an SDLB run with "--test dry-run-with-schema-export", which
 * exports the schemas this tool reads. Table metadata can only change when the configuration or the code
 * changes, so it is not written during a normal SDLB run.
 */
object CatalogSchemaUpdater extends SmartDataLakeLogger {

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType

  protected val parser: OptionParser[CatalogSchemaUpdaterConfig] = new OptionParser[CatalogSchemaUpdaterConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('c', "config")
      .required()
      .action((value, c) => c.copy(configPaths = value.split(',').toIndexedSeq))
      .text("One or multiple configuration files or directories containing configuration files for SDLB, separated by comma.")
    opt[String]("mode")
      .validate(value => if (UpdaterMode.values.exists(_.toString == value)) success
        else failure(s"mode must be one of ${UpdaterMode.values.mkString("|")}"))
      .action((value, c) => c.copy(mode = UpdaterMode.withName(value)))
      .valueName("<plan|apply>")
      .text("plan: report the changes 'apply' would make, without changing the catalog (default). " +
        "apply: create and update the tables of the configuration in the catalog, including schema changes, comments, primary and foreign keys.")
    opt[String]("source")
      .action((value, c) => c.copy(source = Some(value)))
      .text("Source URI to read exported schemas from. Defaults to global.dataObjectsSchemaSource.")
    opt[String]('d', "descriptionPath")
      .action((value, c) => c.copy(descriptionPath = Some(value)))
      .text("Path of the directory containing the Markdown description files of the DataObjects. Column descriptions defined there with @column are applied as column comments. Defaults to global.descriptionPath.")
    opt[String]('i', "includeRegex")
      .action((value, c) => c.copy(includeRegex = value))
      .text("Regular expression used to include DataObjects, matching DataObject ids. Default: .*")
    opt[String]('e', "excludeRegex")
      .action((value, c) => c.copy(excludeRegex = Some(value)))
      .text("Regular expression used to exclude DataObjects, matching DataObject ids. `excludeRegex` is applied after `includeRegex`. Default: no excludes")
    opt[String]('s', "stopOnError")
      .action((value, c) => c.copy(stopOnError = value.toBoolean))
      .text("If true, processing is stopped as soon as there is an error. Otherwise the error is logged and the next DataObject is processed. Default: true")
    help("help").text("Create and update the tables of the SDLB configuration in the catalog: missing tables, " +
      "schema changes, table and column comments, primary and foreign keys. The schemas are read from the files " +
      "exported by an SDLB run with '--test dry-run-with-schema-export'.")
  }

  def main(args: Array[String]): Unit = {
    // Parse all command line arguments
    parser.parse(args, CatalogSchemaUpdaterConfig()) match {
      case Some(updaterConfig) =>

        logger.info(s"starting with configuration ${ProductUtil.formatObj(updaterConfig)}")
        updateCatalog(updaterConfig)

      case None =>
        logAndThrowException(s"Aborting $appType after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  /**
   * Create and update the tables defined in the configuration and in the exported schemas in the catalog,
   * or report the changes which would be applied in mode plan.
   *
   * The schemas are read from `source`, which defaults to `global.dataObjectsSchemaSource`. They are
   * created by an SDLB dry-run using "--test dry-run-with-schema-export", so that the schema and the column
   * comments are available even if the tables do not exist yet in the environment where the dry-run is executed.
   */
  def updateCatalog(config: CatalogSchemaUpdaterConfig): Unit = {

    val isPlan = config.mode == UpdaterMode.Plan

    // get DataObjects
    val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(config.configPaths)
    implicit val hadoopConf: Configuration = globalConfig.getHadoopConfiguration
    val startTime = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext("feedTest", "appTest", SDLExecutionId.executionId1, registry, SmartDataLakeBuilderConfig(appType, Some(appType)), runStartTime = startTime, attemptStartTime = startTime, phase = ExecutionPhase.Init, globalConfig = globalConfig)
    val dataObjects = registry.getDataObjects
      .filter(d => d.id.id.matches(config.includeRegex) && (config.excludeRegex.isEmpty || !d.id.id.matches(config.excludeRegex.get)))

    // schemas exported by a previous dry-run, used to create and evolve the tables and to get the column comments
    val source = config.source.orElse(globalConfig.dataObjectsSchemaSource)
    val schemaWriter = source.map(ExportWriter.apply(_, config.configPaths, globalConfig.uiBackend.map(_.client), Some(hadoopConf)))
    if (schemaWriter.isEmpty) logger.warn("Neither --source nor global.dataObjectsSchemaSource is defined," +
      " no tables will be created and no schema changes and column comments will be applied")
    def readSchema(dataObjectId: DataObjectId): Option[GenericSchema] =
      schemaWriter.flatMap(_.readLatestSchema(dataObjectId)).map(ExportWriter.parseSchema(_)._1)

    // column descriptions from the Markdown description files override the exported schema comments.
    // Note that schemas exported with global.descriptionPath already contain them.
    // The description of an array element, e.g. "addresses.[]", has no column to be set on in the catalog, it would
    // override the comment of the array column itself.
    val columnDescriptions = config.descriptionPath.orElse(globalConfig.descriptionPath)
      .map(path => ColumnDescriptionParser.parse(path)).getOrElse(Map())
      .map { case (dataObjectId, descriptions) =>
        dataObjectId -> descriptions.filterNot(_._1.endsWith(".[]"))
          .map { case (name, d) => ColumnDescriptionParser.toColumnPath(name) -> d }
      }

    val applier = new CatalogMetadataApplier(readSchema, columnDescriptions)
    logger.info(s"${if (isPlan) "Planning" else "Applying"} catalog metadata for ${dataObjects.size} DataObjects")

    def onError(dataObject: DataObject)(ex: Exception): Option[Nothing] = {
      logger.error(s"(${dataObject.id}) ${ex.getClass.getSimpleName}: ${ex.getMessage}")
      if (config.stopOnError) throw ex else None
    }

    // plan all DataObjects
    val plans = dataObjects.flatMap { dataObject =>
      try {
        applier.plan(dataObject).filterNot(_.isEmpty).map(changes => (dataObject, changes))
      } catch {
        case ex: Exception => onError(dataObject)(ex)
      }
    }

    // apply in two phases: the tables including their primary keys first, then the foreign keys referencing
    // them, see CanHandleForeignKeys.
    def applyPhase(describe: CatalogMetadataChanges => Seq[String],
                   apply: (DataObject, CatalogMetadataChanges) => Unit): Seq[DataObjectId] = {
      plans.filter { case (_, changes) => describe(changes).nonEmpty }.flatMap { case (dataObject, changes) =>
        try {
          logger.info(s"(${dataObject.id}) ${if (isPlan) "would apply" else "applying"}:\n  ${describe(changes).mkString("\n  ")}")
          if (!isPlan) apply(dataObject, changes)
          Some(dataObject.id)
        } catch {
          case ex: Exception => onError(dataObject)(ex)
        }
      }
    }
    val changedTables = applyPhase(_.describeTableChanges, applier.applyTableChanges)
    val changedForeignKeys = applyPhase(_.describeForeignKeys, applier.applyForeignKeys)

    val changed = (changedTables ++ changedForeignKeys).distinct
    if (changed.isEmpty) logger.info("Catalog metadata is up to date, nothing to apply")
    else logger.info(s"${if (isPlan) "Would change" else "Changed"} catalog metadata of ${changed.size} DataObjects: ${changed.map(_.id).mkString(", ")}")
  }
}
