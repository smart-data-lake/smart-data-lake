package io.smartdatalake.meta.atlas

import io.smartdatalake.config.{ConfigLoader, ConfigToolbox, ConfigurationException, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.WebserviceException
import org.apache.hadoop.conf.Configuration
import scopt.OptionParser


case class AtlasExporterConfig(
                                atlasExporterConfigPaths: String = null,
                                sdlConfigPaths: String = null,
                                exportTypeDefs: Boolean = false,
                                exportEntities: Boolean = false
                              )


object AtlasExporter extends SmartDataLakeLogger {

  // read version from package manifest (not defined if project is executed in IntellJ)
  val appVersion: String = Option(getClass.getPackage.getImplementationVersion).getOrElse("develop")
  val appType: String = getClass.getSimpleName.replaceAll("\\$$","") // remove $ from object name and use it as appType

  /**
   * InstanceRegistry instance
   */
  val instanceRegistry: InstanceRegistry = new InstanceRegistry()

  /**
   * The Parser defines how to extract the options from the command line args.
   * Subclasses SmartDataLakeBuilder can define additional options to be extracted.
   */
  protected val parser: OptionParser[AtlasExporterConfig] = new OptionParser[AtlasExporterConfig]("AtlasExporter") {
    override def showUsageOnError: Option[Boolean] = Some(true)

    opt[String]('a', "atlasExporterConfig")
      .required()
      .action((config, c) => c.copy(atlasExporterConfigPaths = config))
      .text("One or multiple configuration files or directories containing configuration files for the atlas exporter," +
        "separated by comma.")
    opt[String]('s', "sdlConfig")
      .required()
      .action((config, c) => c.copy(sdlConfigPaths = config))
      .text("One or multiple configuration files or directories containing configuration files for the sdlBuilder, separated by comma.")
    opt[Unit]("exportTypeDefs")
      .action((_, c) => c.copy(exportTypeDefs = true))
      .text("export type definitions")
    opt[Unit]("exportEntities")
      .action((_, c) => c.copy(exportEntities = true))
      .text("export entities")
    help("help").text("Display the help text.")
  }


  /**
   * Parses the supplied (command line) arguments.
   *
   * This method parses command line arguments and creates the corresponding [[AtlasExporterConfig]]
   *
   * @param args an Array of command line arguments.
   * @param config a configuration initialized with default values.
   * @return a new configuration with default values overwritten from the supplied command line arguments.
   */
  def parseCommandLineArguments(args: Array[String], config: AtlasExporterConfig): Option[AtlasExporterConfig] = {
    parser.parse(args, config)
  }
  def main(args: Array[String]): Unit = {
    logger.info(s"Starting Atlas Exporter")

    val config = AtlasExporterConfig()

    // Parse all command line arguments
    parseCommandLineArguments(args, config) match {
      case Some(config) =>
        try {
          implicit val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(config.sdlConfigPaths.split(','))
          val atlasConfig = AtlasConfig(ConfigLoader.loadConfigFromFilesystem(config.atlasExporterConfigPaths.split(','), new Configuration()))
          if (config.exportEntities && config.exportTypeDefs) {
            logger.warn("Both flags for exportTypeDefs and exportEntities set." +
              "It is recommended to first only export typeDefs, and only exportEntities once the typeDefs are visible in atlas")
          }
          if (config.exportTypeDefs) {
            logger.info("Exporting Typedefs")
            AtlasTypeUtil(atlasConfig).export
          }
          if (config.exportEntities) {
            logger.info("Exporting Entities")
            AtlasEntitiesUtil(atlasConfig).export
          }
        } catch {
          case ex: WebserviceException =>
            //logger.error(s"Webservice call failed with ${ex.responseCode} ${ex.responseBody}")
            throw ex
        }
      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}
