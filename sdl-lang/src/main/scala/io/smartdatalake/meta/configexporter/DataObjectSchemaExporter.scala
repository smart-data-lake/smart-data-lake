package io.smartdatalake.meta.configexporter

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigToolbox, ConfigurationException}
import io.smartdatalake.util.misc.{SerializableHadoopConfiguration, SmartDataLakeLogger}
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.dataobject.CanCreateDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.json4s.jackson.JsonMethods.pretty
import scopt.OptionParser

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.LocalDateTime

case class DataObjectSchemaExporterConfig(configPaths: Seq[String] = null,
                                          exportPath: String = "./schema",
                                          includeRegex: String = ".*",
                                          excludeRegex: Option[String] = None,
                                          descriptionPath: Option[String] = None,
                                          master: String = "local[2]"
                                         )

object DataObjectSchemaExporter extends SmartDataLakeLogger {

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType

  protected val parser: OptionParser[DataObjectSchemaExporterConfig] = new OptionParser[DataObjectSchemaExporterConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('c', "config")
      .required()
      .action((value, c) => c.copy(configPaths = value.split(',')))
      .text("One or multiple configuration files or directories containing configuration files for SDLB, separated by comma.")
    opt[String]('p', "exportPath")
      .optional()
      .action((value, c) => c.copy(exportPath = value))
      .text("Path to export configuration to. Default: ./schema")
    opt[String]('i', "includeRegex")
      .optional()
      .action((value, c) => c.copy(includeRegex = value))
      .text("Regular expression used to include DataObjects in export, matching DataObject ids. Default: .*")
    opt[String]('e', "excludeRegex")
      .optional()
      .action((value, c) => c.copy(excludeRegex = Some(value)))
      .text("Regular expression used to exclude DataObjects from export, matching DataObject ids. `excludeRegex` is applied after `includeRegex`. Default: no excludes")
    opt[String]('m', "master")
      .optional()
      .action((value, c) => c.copy(excludeRegex = Some(value)))
      .text("Spark session master configuration. As schemas might be inferred by Spark, there might be a need to tune this for some DataObjects. Default: local[2]")
    help("help").text("Export DataObject schemas as Json-files which can be used by the visualizer. A Json file with the DataObject Id as name is created in `exportPath` for every DataObject to be exported.")
  }

  /**
   * Takes as input a SDL Config and exports the schema of all DataObjects for the visualizer.
   */
  def main(args: Array[String]): Unit = {
    val exporterConfig = DataObjectSchemaExporterConfig()
    // Parse all command line arguments
    parser.parse(args, exporterConfig) match {
      case Some(exporterConfig) =>

        // export data object schemas to json format
        val schemas = exportSchemas(exporterConfig)

        // write file
        logger.info(s"Writing ${schemas.size} data object schemas to json files in path ${exporterConfig.exportPath}")
        val path = Paths.get(exporterConfig.exportPath)
        Files.createDirectories(path)
        schemas.foreach{
          case (dataObjectId, jsonStr) =>
            val file = path.resolve(s"${dataObjectId.id}.${System.currentTimeMillis() / 1000}.json")
            logger.debug(s"Writing file $file")
            Files.write(file, jsonStr.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        }

      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  def exportSchemas(config: DataObjectSchemaExporterConfig): Seq[(DataObjectId,String)] = {
    val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(config.configPaths)
    val hadoopConf = globalConfig.getHadoopConfiguration
    implicit val context: ActionPipelineContext = ActionPipelineContext("feedTest", "appTest", SDLExecutionId.executionId1, registry, Some(LocalDateTime.now()), SmartDataLakeBuilderConfig("DataObjectSchemaExporter", Some("DataObjectSchemaExporter"), master=Some(config.master)), phase = ExecutionPhase.Init, serializableHadoopConf = new SerializableHadoopConfiguration(hadoopConf), globalConfig = globalConfig)
    registry.getDataObjects
      .filter(d => d.id.id.matches(config.includeRegex) && (config.excludeRegex.isEmpty || !d.id.id.matches(config.excludeRegex.get)))
      .collect {
        case dataObject: CanCreateDataFrame =>
          // get schema
          val df = dataObject.getDataFrame(Seq(), dataObject.getSubFeedSupportedTypes.head)
          val schemaJson = df.schema.toJson
          (dataObject.id, pretty(schemaJson))
    }
  }
}
