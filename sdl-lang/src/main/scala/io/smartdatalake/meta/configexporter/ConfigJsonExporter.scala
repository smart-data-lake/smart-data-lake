package io.smartdatalake.meta.configexporter

import com.typesafe.config.{ConfigRenderOptions, ConfigValueFactory}
import io.smartdatalake.config.{ConfigLoader, ConfigParser, ConfigurationException}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.hadoop.conf.Configuration
import scopt.OptionParser

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.JavaConverters._

case class ConfigJsonExporterConfig(configPaths: Seq[String] = null, filename: String = "exportedConfig.json", enrichOrigin: Boolean = true)

object ConfigJsonExporter extends SmartDataLakeLogger {

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType

  protected val parser: OptionParser[ConfigJsonExporterConfig] = new OptionParser[ConfigJsonExporterConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('c', "config")
      .required()
      .action((value, c) => c.copy(configPaths = value.split(',')))
      .text("One or multiple configuration files or directories containing configuration files for SDLB, separated by comma.")
    opt[String]('f', "filename")
      .optional()
      .action((value, c) => c.copy(filename = value))
      .text("File to export configuration to. Default: exportedConfig.json")
    opt[Boolean]("enrichOrigin")
      .optional()
      .action((value, c) => c.copy(enrichOrigin = value))
      .text("Whether to add an additional property 'origin' including source filename and line number to first class configuration objects.")
    help("help").text("Display the help text.")
  }

  /**
   * Takes as input a SDL Config and exports it as one json document, everything resolved.
   * Additionally a separate file with the mapping of first class config objects to source code origin is created.
   */
  def main(args: Array[String]): Unit = {
    val exporterConfig = ConfigJsonExporterConfig()
    // Parse all command line arguments
    parser.parse(args, exporterConfig) match {
      case Some(exporterConfig) =>

        // create json
        val configAsJson = exportConfigJson(exporterConfig)

        // write file
        logger.info(s"Writing config json to file ${exporterConfig.filename}")
        Files.write(Paths.get(exporterConfig.filename), configAsJson.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  def exportConfigJson(exporterConfig: ConfigJsonExporterConfig): String = {
    val defaultHadoopConf: Configuration = new Configuration()
    val sdlConfig = ConfigLoader.loadConfigFromFilesystem(exporterConfig.configPaths, defaultHadoopConf)
    // remove additional config paths introduced by system properties...
    val configKeysToRemove = sdlConfig.root.keySet().asScala.diff(Set(ConfigParser.CONFIG_SECTION_ACTIONS, ConfigParser.CONFIG_SECTION_CONNECTIONS, ConfigParser.CONFIG_SECTION_DATAOBJECTS, ConfigParser.CONFIG_SECTION_GLOBAL))
    val reducedSdlConfig = configKeysToRemove.foldLeft(sdlConfig)((config,key) => config.withoutPath(key))
    // enrich origin of first class config objects
    val descriptionRegex = "(.*): ([0-9]+)(-[0-9]+)?".r
    val configSectionsToEnrich =  Set(ConfigParser.CONFIG_SECTION_ACTIONS, ConfigParser.CONFIG_SECTION_CONNECTIONS, ConfigParser.CONFIG_SECTION_DATAOBJECTS)
    val enrichedSdlConfig = configSectionsToEnrich.foldLeft(reducedSdlConfig){
      case (config,sectionKey) =>
        config.getConfig(sectionKey).root.keySet().asScala.foldLeft(config) {
          case (config,objectKey) =>
            val objectConfig = config.getConfig(s"$sectionKey.$objectKey")
            // parse origins description, as we can not access the detailed private properties
            // note that currently endLineNumber is not filled by Hocon parser, even though it is foreseen in the code.
            val (path, lineNumber, endLineNumber) = objectConfig.origin.description match {
              case descriptionRegex(path, lineNumber, endLineNumber) =>
                (path, lineNumber.toInt, Option(endLineNumber).map(_.toInt))
            }
            val config1 = config
              .withValue(s"$sectionKey.$objectKey.origin.path", ConfigValueFactory.fromAnyRef(path))
              .withValue(s"$sectionKey.$objectKey.origin.lineNumber", ConfigValueFactory.fromAnyRef(lineNumber))
            if (endLineNumber.isDefined) config1.withValue(s"$sectionKey.$objectKey.origin.endLineNumber", ConfigValueFactory.fromAnyRef(endLineNumber.get))
            else config1
        }
    }
    // render config as json
    val sdlConfigToExport = if (exporterConfig.enrichOrigin) enrichedSdlConfig else reducedSdlConfig
    sdlConfigToExport.root.render(ConfigRenderOptions.concise())
  }
}
