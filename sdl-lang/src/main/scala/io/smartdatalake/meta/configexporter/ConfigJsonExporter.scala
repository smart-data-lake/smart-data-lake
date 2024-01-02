package io.smartdatalake.meta.configexporter

import com.typesafe.config.{Config, ConfigList, ConfigRenderOptions, ConfigValue, ConfigValueFactory, ConfigValueType}
import configs.ConfigObject
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigLoader, ConfigParser, ConfigurationException}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.meta.ScaladocUtil
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.misc.HoconUtil.{getConfigValue, updateConfigValue}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.DataFrameUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scopt.OptionParser

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.jdk.CollectionConverters._

case class ConfigJsonExporterConfig(configPaths: Seq[String] = null, filename: String = "exportedConfig.json", enrichOrigin: Boolean = true, descriptionPath: Option[String] = None)

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
    opt[String]('d', "descriptionPath")
      .optional()
      .action((value, c) => c.copy(descriptionPath = Some(value)))
      .text("Path to markdown files that contain column descriptions of DataObjects. If set, these descriptions are exported to the visualizer.")
    help("help").text("Display the help text.")
  }

  /**
   * Takes as input an SDL Config and exports it as one json document, everything resolved.
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
        val path = Paths.get(exporterConfig.filename)
        Files.createDirectories(path.getParent)
        Files.write(path, configAsJson.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  def exportConfigJson(exporterConfig: ConfigJsonExporterConfig): String = {
    implicit val defaultHadoopConf: Configuration = new Configuration()
    var sdlConfig = ConfigLoader.loadConfigFromFilesystem(exporterConfig.configPaths, defaultHadoopConf)
    // remove additional config paths introduced by system properties...
    val configKeysToRemove = sdlConfig.root.keySet().asScala.diff(Set(ConfigParser.CONFIG_SECTION_ACTIONS, ConfigParser.CONFIG_SECTION_CONNECTIONS, ConfigParser.CONFIG_SECTION_DATAOBJECTS, ConfigParser.CONFIG_SECTION_GLOBAL))
    sdlConfig = configKeysToRemove.foldLeft(sdlConfig)((config,key) => config.withoutPath(key))
    // enrich origin of first class config objects
    if (exporterConfig.enrichOrigin) sdlConfig = enrichOrigin(exporterConfig.configPaths, sdlConfig)
    // enrich optional column description from description files
    exporterConfig.descriptionPath.foreach(path => sdlConfig = enrichColumnDescription(path, sdlConfig))
    // enrich optional source documentation for custom classes from scaladoc
    sdlConfig = enrichCustomClassScalaDoc(sdlConfig)
    // render config as json
    sdlConfig.root.render(ConfigRenderOptions.concise())
  }

  private def enrichCustomClassScalaDoc(config: Config): Config = {
    def findClassConfigurationsHandleEntry(key: String, value: ConfigValue, path: Seq[String]): Seq[Seq[String]] = {
      value.valueType match {
        // recursion for objects and lists
        case ConfigValueType.OBJECT =>
          findClassConfigurationsInConfigObject(value.asInstanceOf[ConfigObject], path)
        case ConfigValueType.LIST =>
          val elements = value.asInstanceOf[ConfigList].asScala.zipWithIndex
          elements.flatMap {
            case (value, idx) => findClassConfigurationsHandleEntry(idx.toString, value, path :+ s"[$idx]")
          }.toSeq
        // we are looking for className and type attributes
        case ConfigValueType.STRING if Seq("className", "type").contains(DataFrameUtil.strToLowerCamelCase(key)) =>
          // check if configuration value is full class name, e.g. it has at least three name parts
          if (value.unwrapped.asInstanceOf[String].split('.').length > 2)
            Seq(path) // found! return configuration path
          else Seq()
        // default -> nothing found
        case _ => Seq()
      }
    }
    def findClassConfigurationsInConfigObject(obj: ConfigObject, path: Seq[String]): Seq[Seq[String]] = {
      obj.entrySet.asScala.toSeq.flatMap(e => findClassConfigurationsHandleEntry(e.getKey, e.getValue, path :+ e.getKey))
    }
    val classConfigurationPaths = findClassConfigurationsInConfigObject(config.root, Seq())
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

  private def enrichColumnDescription(descriptionPath: String, config: Config)(implicit hadoopConf: Configuration): Config = {
    val columnDescriptionRegex = """\s*@column\s+["`']?([^\s"`']+)["`']?\s+(.*)""".r.anchored
    var enrichedConfig = config
    val hadoopPath = new Path(descriptionPath, ConfigParser.CONFIG_SECTION_DATAOBJECTS)
    implicit val filesystem: FileSystem = Environment.fileSystemFactory.getFileSystem(hadoopPath, hadoopConf)

    logger.info(s"Searching DataObject description files in $hadoopPath")
    RemoteIteratorWrapper(filesystem.listStatusIterator(hadoopPath)).filterNot(_.isDirectory)
      .filter(_.getPath.getName.endsWith(".md")).toSeq // only markdown files
      .foreach { p =>
        val dataObjectId = p.getPath.getName.split('.').head
        val dataObjectPath = s"${ConfigParser.CONFIG_SECTION_DATAOBJECTS}.$dataObjectId"
        if (enrichedConfig.hasPath(dataObjectPath)) {
          val descriptions = HdfsUtil.readHadoopFile(p.getPath).linesIterator.foldLeft((Seq[(String, String)](), false)) {
            // if new column description tag, add new column description
            case ((descriptions, _), columnDescriptionRegex(name, description)) =>
              (descriptions :+ (name, description.trim), true)
            // if new header tag and column description open, close column description
            case ((descriptions, true), line) if line.startsWith("#") =>
              (descriptions, false)
            // if last column description open, add line to last column description text
            case ((descriptions, true), line) =>
              val (lastName, lastDesc) = descriptions.last
              (descriptions.init :+ (lastName, (lastDesc + System.lineSeparator() + line.trim).trim), true)
            // if last column description closed, ignore line
            case ((descriptions, false), _) =>
              (descriptions, false)
          }._1.filter(_._2.nonEmpty).toMap
          if (descriptions.nonEmpty) {
            logger.info(s"(${DataObjectId(dataObjectId)}) Merging ${descriptions.size} column descriptions")
            enrichedConfig = enrichedConfig.withValue(s"$dataObjectPath._columnDescriptions", ConfigValueFactory.fromMap(descriptions.asJava))
          }
        } else {
          logger.error(s"(${DataObjectId(dataObjectId)}) Markdown file found, but DataObject does not exist in configuration")
        }
      }

    // return
    enrichedConfig
  }
}
