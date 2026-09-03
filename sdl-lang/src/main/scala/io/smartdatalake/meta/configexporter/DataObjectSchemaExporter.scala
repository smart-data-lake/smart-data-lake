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
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataobject.generic.{CatalogMetadataApplier, CatalogMetadataChanges}
import org.apache.hadoop.conf.Configuration
import io.smartdatalake.config.exporter.ExportWriter
import io.smartdatalake.config.exporter.ExportWriter.formatSchema
import io.smartdatalake.config.{ConfigToolbox, ConfigurationException}
import io.smartdatalake.util.misc._
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.CanCreateDataFrame
import io.smartdatalake.workflow.dataobject.spark.SparkFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import scopt.OptionParser

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

/**
 * What DataObjectSchemaExporter should do.
 */
object ExporterMode extends Enumeration {
  type ExporterMode = Value

  /**
   * Read the schemas and statistics of the DataObjects and write them to the configured targets.
   */
  val Export: ExporterMode = Value("export")

  /**
   * Read the desired tables from the configuration and the exported schema files, and create or update them
   * in the catalog: missing tables, schema changes, comments, primary and foreign keys.
   * This is the deployment time counterpart of "--test dry-run-with-schema-export".
   */
  val Apply: ExporterMode = Value("apply")

  /**
   * Like [[Apply]], but only report the changes which would be applied, without changing the catalog.
   */
  val Plan: ExporterMode = Value("plan")
}

case class DataObjectSchemaExporterConfig(configPaths: Seq[String] = null,
                                          mode: ExporterMode.Value = ExporterMode.Export,
                                          targets: Seq[String] = Seq("./schema"),
                                          source: Option[String] = None,
                                          descriptionPath: Option[String] = None,
                                          includeRegex: String = ".*",
                                          excludeRegex: Option[String] = None,
                                          withStats: Boolean = true,
                                          updateStats: Boolean = true,
                                          preferredSubFeedType: Option[String] = None,
                                          stopOnError: Boolean = true,
                                          master: String = "local[2]"
                                         )

object DataObjectSchemaExporter extends SmartDataLakeLogger {

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  protected val parser: OptionParser[DataObjectSchemaExporterConfig] = new OptionParser[DataObjectSchemaExporterConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('c', "config")
      .required()
      .action((value, c) => c.copy(configPaths = value.split(',').toIndexedSeq))
      .text("One or multiple configuration files or directories containing configuration files for SDLB, separated by comma.")
    opt[String]("mode")
      .action((value, c) => c.copy(mode = ExporterMode.withName(value)))
      .valueName("<export|apply|plan>")
      .text("export: read schemas and statistics of the DataObjects and write them to the target (default). " +
        "apply: create and update the tables of the configuration in the catalog, including schema changes, comments, primary and foreign keys. " +
        "plan: report the changes 'apply' would make, without changing the catalog.")
    opt[String]("source")
      .action((value, c) => c.copy(source = Some(value)))
      .text("Source URI to read exported schemas from in mode apply/plan. Defaults to global.dataObjectsSchemaSource.")
    opt[String]('d', "descriptionPath")
      .action((value, c) => c.copy(descriptionPath = Some(value)))
      .text("Path of the directory containing the Markdown description files of the DataObjects. Column descriptions defined there with @column are applied as column comments in mode apply/plan.")
    opt[String]('p', "exportPath")
      .action((value, c) => c.copy(targets = Seq(value)))
      .text("Deprecated: Use target instead. Path to export schema and statistics to.")
    opt[String]('t', "target")
      .action((value, c) => c.copy(targets = value.split(",").map(_.trim).toSeq))
      .text("Target URI to export configuration to. Can be './xyz.json', 'uiBackend', or any http/https URL. 'uiBackend will use global.uiBackend configuration to upload to UI backend. Default: ./schema")
    opt[String]('i', "includeRegex")
      .action((value, c) => c.copy(includeRegex = value))
      .text("Regular expression used to include DataObjects in export, matching DataObject ids. Default: .*")
    opt[String]('e', "excludeRegex")
      .action((value, c) => c.copy(excludeRegex = Some(value)))
      .text("Regular expression used to exclude DataObjects from export, matching DataObject ids. `excludeRegex` is applied after `includeRegex`. Default: no excludes")
    opt[String]('w', "withStats")
      .action((value, c) => c.copy(withStats = value.toBoolean))
      .text("If true, DataObject statistics are exported, otherwise not. Default: true")
    opt[String]('u', "updateStats")
      .action((value, c) => c.copy(updateStats = value.toBoolean))
      .text("If true, more costly operations to update statistics such as \"analyze table\" are executed before returning statistics. Default: true")
    opt[String]("preferredSubFeedType")
      .action((value, c) => c.copy(preferredSubFeedType = Some(value)))
      .text("If a DataObjects implements multiple subFeedTypes, e.g. Spark and Snowpark, the schema is exported for the first subFeedType defined in the DataObject.getSubFeedSupportedTypes. This can be overridden by giving a preferred subFeedType. Possible values are subclasses of DataFrameSubFeed, e.g. SparkSubFeed and SnowparkSubFeed.")
    opt[String]('s', "stopOnError")
      .action((value, c) => c.copy(stopOnError = value.toBoolean))
      .text("If true, export is stopped as soon as there is an error. Otherwise the error is written into the export content. Default: true")
    opt[String]('m', "master")
      .action((value, c) => c.copy(master = value))
      .text("Spark session master configuration. As schemas might be inferred by Spark, there might be a need to tune this for some DataObjects. Default: local[2]")
    help("help").text("Export DataObject schemas and statistics as Json documents which can be used by the visualizer. Each Json document is identified by its type (schema or stats), the DataObject Id and the timestamp of creation.")
  }

  /**
   * Takes as input an SDL Config and exports the schema of all DataObjects for the visualizer.
   */
  def main(args: Array[String]): Unit = {
    // Parse all command line arguments
    parser.parse(args, DataObjectSchemaExporterConfig()) match {
      case Some(exporterConfig) =>

        logger.info(s"starting with configuration ${ProductUtil.formatObj(exporterConfig)}")
        exporterConfig.mode match {
          // export data object schemas and statistics to json format
          case ExporterMode.Export => exportSchemaAndStats(exporterConfig)
          // write table metadata to the catalog, or report what would be written
          case ExporterMode.Apply | ExporterMode.Plan => applyCatalogMetadata(exporterConfig)
        }

      case None =>
        logAndThrowException(s"Aborting $appType after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  def exportSchemaAndStats(config: DataObjectSchemaExporterConfig): Unit = {

    // get DataObjects
    val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(config.configPaths)
    val hadoopConf = globalConfig.getHadoopConfiguration
    val startTime = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext("feedTest", "appTest", SDLExecutionId.executionId1, registry, SmartDataLakeBuilderConfig("DataObjectSchemaExporter", Some("DataObjectSchemaExporter")), runStartTime = startTime, attemptStartTime = startTime, phase = ExecutionPhase.Init, globalConfig = globalConfig)
    val dataObjects = registry.getDataObjects
      .filter(d => d.id.id.matches(config.includeRegex) && (config.excludeRegex.isEmpty || !d.id.id.matches(config.excludeRegex.get)))
    logger.info(s"Writing ${dataObjects.size} DataObject schemas and stats to target ${config.targets.mkString(",")}")

    // create document writer depending on target uri scheme
    val writers = config.targets.map(ExportWriter.apply(_, config.configPaths))

    // get and write Schemas
    val atLeastOneSchemaSuccessful = dataObjects.map { dataObject =>
      logger.info(s"get schema for ${dataObject.id} (${dataObject.getClass.getSimpleName})")
      val exportedSchema = dataObject match {
        case dataObject: SparkFileDataObject =>
          val schema = Try(dataObject.getSchema)
          val info = schema match {
            case Success(Some(s)) => None
            case Success(None) => Some(s"${dataObject.id} of type ${dataObject.getClass.getSimpleName} did not return a schema")
            case Failure(ex) => Some(s"${ex.getClass.getSimpleName}: ${ex.getMessage}")
          }
          Some((schema.toOption.flatten, info, schema.isSuccess, schema.failed.toOption))
        case dataObject: CanCreateDataFrame =>
          // prefer given subFeedType if defined, otherwise take first subFeedType defined by the DataObject
          val subFeedType = dataObject.getSubFeedSupportedTypes.find(tpe => config.preferredSubFeedType.contains(tpe.typeSymbol.name.toTermName.toString))
            .getOrElse(dataObject.getSubFeedSupportedTypes.head)
          val schema = Try(dataObject.getDataFrame(Seq(), subFeedType).schema)
          val info = schema.failed.toOption.map(ex => s"${ex.getClass.getSimpleName}: ${ex.getMessage}")
          Some((schema.toOption, info, schema.isSuccess, schema.failed.toOption))
        case _ => None
      }
      // log errors, then throw first exception
      exportedSchema.flatMap(_._2).foreach {
        info => logger.warn(s"Could not get schema for ${dataObject.id}: $info")
      }
      if (config.stopOnError) exportedSchema.flatMap(_._4).foreach(throw _)
      // write schemas
      exportedSchema.foreach {
        case (schema, info, _, _) =>
          writers.foreach(_.writeSchema(formatSchema(schema, info), dataObject.id, getCurrentVersion))
      }
      // return true if no exception
      exportedSchema.forall(_._3)
    }.reduceOption(_ || _).getOrElse(false)
    require(atLeastOneSchemaSuccessful, "Schema export failed for all DataObjects!")

    // get and write Stats
    if (config.withStats) {
      dataObjects.foreach { dataObject =>
        try {
          logger.info(s"get statistics for ${dataObject.id}")
          val stats = dataObject.getStats(config.updateStats)
          val contentStr = Serialization.writePretty(stats)
          writers.foreach(_.writeStats(contentStr, dataObject.id, getCurrentVersion))
        } catch {
          case ex: Exception =>
            logger.warn(s"${ex.getClass.getSimpleName}: ${ex.getMessage}")
        }
      }
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
  def applyCatalogMetadata(config: DataObjectSchemaExporterConfig): Unit = {

    val isPlan = config.mode == ExporterMode.Plan

    // get DataObjects
    val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(config.configPaths)
    implicit val hadoopConf: Configuration = globalConfig.getHadoopConfiguration
    val startTime = LocalDateTime.now()
    implicit val context: ActionPipelineContext = ActionPipelineContext("feedTest", "appTest", SDLExecutionId.executionId1, registry, SmartDataLakeBuilderConfig("DataObjectSchemaExporter", Some("DataObjectSchemaExporter")), runStartTime = startTime, attemptStartTime = startTime, phase = ExecutionPhase.Init, globalConfig = globalConfig)
    val dataObjects = registry.getDataObjects
      .filter(d => d.id.id.matches(config.includeRegex) && (config.excludeRegex.isEmpty || !d.id.id.matches(config.excludeRegex.get)))

    // schemas exported by a previous dry-run, used to create and evolve the tables and to get the column comments
    val source = config.source.orElse(globalConfig.dataObjectsSchemaSource)
    val schemaWriter = source.map(ExportWriter.apply(_, config.configPaths, globalConfig.uiBackend.map(_.client), Some(hadoopConf)))
    if (schemaWriter.isEmpty) logger.warn("Neither --source nor global.dataObjectsSchemaSource is defined," +
      " no tables will be created and no schema changes and column comments will be applied")
    def readSchema(dataObjectId: DataObjectId): Option[GenericSchema] =
      schemaWriter.flatMap(_.readLatestSchema(dataObjectId)).map(ExportWriter.parseSchema(_)._1)

    // column descriptions from the Markdown description files override the exported schema comments
    val columnDescriptions = config.descriptionPath.map(path => ColumnDescriptionParser.parse(path)).getOrElse(Map())
      .map { case (dataObjectId, descriptions) =>
        dataObjectId -> descriptions.map { case (name, d) => ColumnDescriptionParser.toColumnPath(name) -> d }
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

  private[configexporter] def getCurrentVersion = System.currentTimeMillis() / 1000
}
