package io.smartdatalake.meta.atlas

import io.smartdatalake.config.{ConfigLoader, ConfigToolbox, ConfigurationException, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.WebserviceException
import org.apache.hadoop.conf.Configuration
import scopt.OptionParser


case class AtlasCleanerConfig(
                                atlasExporterConfigPaths: String = null,
                                deleteTypeDefs: Boolean = false,
                                deleteEntities: Boolean = false
                              )


object AtlasCleaner extends SmartDataLakeLogger {

  // read version from package manifest (not defined if project is executed in IntellJ)
  val appVersion: String = Option(getClass.getPackage.getImplementationVersion).getOrElse("develop")
  val appType: String = getClass.getSimpleName.replaceAll("\\$$","") // remove $ from object name and use it as appType

  /**
   * The Parser defines how to extract the options from the command line args.
   * Subclasses SmartDataLakeBuilder can define additional options to be extracted.
   */
  protected val parser: OptionParser[AtlasCleanerConfig] = new OptionParser[AtlasCleanerConfig]("AtlasCleaner") {
    override def showUsageOnError: Option[Boolean] = Some(true)

    opt[String]('a', "atlasCleanerConfig")
      .required()
      .action((config, c) => c.copy(atlasExporterConfigPaths = config))
      .text("One or multiple configuration files or directories containing configuration files for the atlas cleaner, separated by comma.")
    opt[Unit]("deleteTypeDefs")
      .action((_, c) => c.copy(deleteTypeDefs = true))
      .text("delete type definitions for configured prefix")
    opt[Unit]("deleteEntities")
      .action((_, c) => c.copy(deleteEntities = true))
      .text("delete entities for type definitions with configured prefix")
    help("help").text("Display the help text.")
  }


  /**
   * Parses the supplied (command line) arguments.
   *
   * This method parses command line arguments and creates the corresponding [[AtlasCleanerConfig]]
   *
   * @param args an Array of command line arguments.
   * @param config a configuration initialized with default values.
   * @return a new configuration with default values overwritten from the supplied command line arguments.
   */
  def parseCommandLineArguments(args: Array[String], config: AtlasCleanerConfig): Option[AtlasCleanerConfig] = {
    parser.parse(args, config)
  }

  def main(args: Array[String]): Unit = {
    logger.info(s"Starting Atlas Cleaner")

    val config = AtlasCleanerConfig()

    // Parse all command line arguments
    parseCommandLineArguments(args, config) match {
      case Some(config) =>
        try {
          val atlasConfig = AtlasConfig(ConfigLoader.loadConfigFromFilesystem(config.atlasExporterConfigPaths.split(','), new Configuration()))
          if (config.deleteEntities) {
            logger.info("Deleting Entities for type definitions with prefix ${atlasConfig.getTypeDefPrefix}")
            // TODO
            //AtlasEntitiesUtil(atlasConfig).clean
          }
          if (config.deleteTypeDefs) {
            logger.info(s"Deleting Typedefs with prefix ${atlasConfig.getTypeDefPrefix}")
            // TODO
            //AtlasTypeUtil(atlasConfig).clean
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
