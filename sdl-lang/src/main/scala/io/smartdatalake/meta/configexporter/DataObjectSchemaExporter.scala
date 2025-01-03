package io.smartdatalake.meta.configexporter

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.{ConfigToolbox, ConfigurationException}
import io.smartdatalake.util.misc._
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, SparkFileDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods.pretty
import org.json4s.jackson.Serialization
import org.json4s.{Formats, JObject, NoTypeHints}
import scopt.OptionParser

import java.time.LocalDateTime
import scala.collection.compat._
import scala.util.{Failure, Success, Try}

case class DataObjectSchemaExporterConfig(configPaths: Seq[String] = null,
                                          target: String = "file:./schema",
                                          includeRegex: String = ".*",
                                          excludeRegex: Option[String] = None,
                                          updateStats: Boolean = true,
                                          master: String = "local[2]"
                                         )

object DataObjectSchemaExporter extends SmartDataLakeLogger {

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  protected val parser: OptionParser[DataObjectSchemaExporterConfig] = new OptionParser[DataObjectSchemaExporterConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('c', "config")
      .required()
      .action((value, c) => c.copy(configPaths = value.split(',')))
      .text("One or multiple configuration files or directories containing configuration files for SDLB, separated by comma.")
    opt[String]('p', "exportPath")
      .action((value, c) => c.copy(target = "file:"+value))
      .text("Deprecated: Use target instead. Path to export schema and statistics to.")
    opt[String]('t', "target")
      .action((value, c) => c.copy(target = value))
      .text("Target URI to export configuration to. Can be 'file:./xyz.json', 'uiBackend', or any http/https URL. 'uiBackend will use global.uiBackend configuration to upload to UI backend. Default: file:./exportedConfig.json")
    opt[String]('i', "includeRegex")
      .action((value, c) => c.copy(includeRegex = value))
      .text("Regular expression used to include DataObjects in export, matching DataObject ids. Default: .*")
    opt[String]('e', "excludeRegex")
      .action((value, c) => c.copy(excludeRegex = Some(value)))
      .text("Regular expression used to exclude DataObjects from export, matching DataObject ids. `excludeRegex` is applied after `includeRegex`. Default: no excludes")
    opt[String]('u', "updateStats")
      .action((value, c) => c.copy(updateStats = value.toBoolean))
      .text("If true, more costly operations to update statistics such as \"analyze table\" are executed before returning statistics. Default: true")
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

        // export data object schemas and statistics to json format
        logger.info(s"starting with configuration ${ProductUtil.formatObj(exporterConfig)}")
        exportSchemaAndStats(exporterConfig)

      case None =>
        logAndThrowException(s"Aborting $appType after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  def exportSchemaAndStats(config: DataObjectSchemaExporterConfig): Unit = {

    // get DataObjects
    val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(config.configPaths)
    val hadoopConf = globalConfig.getHadoopConfiguration
    implicit val context: ActionPipelineContext = ActionPipelineContext("feedTest", "appTest", SDLExecutionId.executionId1, registry, Some(LocalDateTime.now()), SmartDataLakeBuilderConfig("DataObjectSchemaExporter", Some("DataObjectSchemaExporter"), master=Some(config.master)), phase = ExecutionPhase.Init, serializableHadoopConf = new SerializableHadoopConfiguration(hadoopConf), globalConfig = globalConfig)
    val dataObjects = registry.getDataObjects
      .filter(d => d.id.id.matches(config.includeRegex) && (config.excludeRegex.isEmpty || !d.id.id.matches(config.excludeRegex.get)))
    logger.info(s"Writing ${dataObjects.size} DataObject schemas and stats to target ${config.target}")

    // create document writer depending on target uri scheme
    val writer = ExportWriter.apply(config.target, config.configPaths)

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
          Some((schema.toOption.flatten, info, schema.isSuccess))
        case dataObject: CanCreateDataFrame =>
          val schema = Try(dataObject.getDataFrame(Seq(), dataObject.getSubFeedSupportedTypes.head).schema)
          val info = schema.failed.toOption.map(ex => s"${ex.getClass.getSimpleName}: ${ex.getMessage}")
          Some((schema.toOption, info, schema.isSuccess))
        case _ => None
      }
      exportedSchema.foreach {
        case (schema, info, _) =>
          info.foreach(logger.warn)
          writer.writeSchema(formatSchema(schema, info), dataObject.id, getCurrentVersion)
      }
      // return true if no exception
      exportedSchema.forall(_._3)
    }.maxOption
    require(!atLeastOneSchemaSuccessful.contains(false), "Schema export failed for all DataObjects!")

    // get and write Stats
    dataObjects.foreach { dataObject =>
      try {
        logger.info(s"get statistics for ${dataObject.id}")
        val stats = dataObject.getStats(config.updateStats)
        val contentStr = Serialization.writePretty(stats)
        writer.writeStats(contentStr, dataObject.id, getCurrentVersion)
      } catch {
        case ex: Exception =>
          logger.warn(s"${ex.getClass.getSimpleName}: ${ex.getMessage}")
      }
    }
  }

  private[configexporter] def formatSchema(schema: Option[GenericSchema], info: Option[String]): String = {
    val contentJson = JObject(Seq(info.toSeq.map("info" -> JString(_)), schema.toSeq.map("schema" -> _.toJson)).flatten:_*)
    pretty(contentJson)
  }

  private[configexporter] def getCurrentVersion = System.currentTimeMillis() / 1000
}
