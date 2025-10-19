package io.smartdatalake.meta.configexporter

import com.typesafe.config._
import configs.ConfigObject
import io.smartdatalake.app.BuildVersionInfo
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.exporter.{ExportWriter, FileDescriptor, UploadDefaults}
import io.smartdatalake.config.{ConfigLoader, ConfigParser, ConfigurationException}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.misc.HoconUtil.{getConfigValue, updateConfigValue}
import io.smartdatalake.util.misc.{CustomCodeUtil, HoconUtil, ScaladocUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.action.spark.customlogic.{CustomTransformMethodDef, CustomTransformMethodWrapper}
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfsTransformer
import org.apache.commons.lang.NotImplementedException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scopt.OptionParser

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using

case class ConfigJsonExporterConfig(configPaths: Seq[String] = null, targets: Seq[String] = Seq("file:./exportedConfig.json"), enrichOrigin: Boolean = true, descriptionPath: Option[String] = None, uploadDescriptions: Boolean = false)

object ConfigJsonExporter extends SmartDataLakeLogger {

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType

  protected val parser: OptionParser[ConfigJsonExporterConfig] = new OptionParser[ConfigJsonExporterConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('c', "config")
      .required()
      .action((value, c) => c.copy(configPaths = value.split(',')))
      .text("One or multiple configuration files or directories containing configuration files for SDLB, separated by comma.")
    opt[String]('f', "filename")
      .action((value, c) => c.copy(targets = Seq("file:" + value)))
      .text("Deprecated: Use target instead. File to export configuration to.")
    opt[String]('t', "target")
      .action((value, c) => c.copy(targets = value.split(",").map(_.trim).toSeq))
      .text("Target URI to export configuration to. Can be 'file:./xyz.json', 'uiBackend', or any http/https URL. 'uiBackend will use global.uiBackend configuration to upload to UI backend. Default: file:./exportedConfig.json")
    opt[Boolean]("enrichOrigin")
      .action((value, c) => c.copy(enrichOrigin = value))
      .text("Whether to add an additional property 'origin' including source filename and line number to first class configuration objects.")
    opt[String]('d', "descriptionPath")
      .action((value, c) => c.copy(descriptionPath = Some(value)))
      .text("Path to markdown files that contain column descriptions of DataObjects. If set, the exported config is enriched with the column descriptions.")
    opt[Unit]("uploadDescriptions")
      .action((_, c) => c.copy(uploadDescriptions = true))
      .text("Upload description markdown files to visualizer backend.")
    help("help").text("Export resolved configuration as Json document which can be used by the visualizer.")
  }

  /**
   * Takes as input an SDL Config and exports it as one json document, everything resolved and with some metadata enrichment's.
   *
   * Use the following maven profile to add an additional export step to your build:
   * ```
   *     <profile>
   *         <id>export-config</id>
   *         <build>
   *             <plugins>
   *                 <!-- parse and export configuration -->
   *                 <plugin>
   *                     <groupId>org.codehaus.mojo</groupId>
   *                     <artifactId>exec-maven-plugin</artifactId>
   *                     <version>3.1.0</version>
   *                     <executions>
   *                         <execution>
   *                             <id>parse-export-config</id>
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
   *                                     <argument>
   *                                         io.smartdatalake.meta.configexporter.ConfigJsonExporter
   *                                     </argument>
   *                                     <argument>--config</argument>
   * <argument>./config,./envConfig/dev.conf</argument>
   * <argument>--target</argument>
   * <argument>file:./viz/exportedConfig.json</argument>
   *                                     <argument>--descriptionPath</argument>
   *                                     <argument>./description</argument>
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
   *
   * The new profile can then be executed by the following maven command line: `mvn exec:exec@parse-export-config -Dorg.slf4j.simpleLogger.log.org.apache=ERROR -Pexport-config`
   */
  def main(args: Array[String]): Unit = {
    // Parse all command line arguments
    parser.parse(args, ConfigJsonExporterConfig()) match {
      case Some(config) =>

        // create json
        implicit val hadoopConf: Configuration = new Configuration()
        val configAsJson = exportConfigJson(config)

        // create document writer depending on target uri scheme
        val writers = config.targets.map(ExportWriter.apply(_, config.configPaths))

        writers.zipWithIndex.foreach { case (writer, i) =>

          // write export config
          val version = if (!config.targets(i).contains("version=")) Some(getAppVersion()) else None
          writer.writeConfig(configAsJson, version)

          // write descriptions
          if (config.uploadDescriptions) uploadDescriptions(config, writer, version)
        }


      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  def getAppVersion(): String = {
    BuildVersionInfo.appVersionInfo.map(_.version).getOrElse(UploadDefaults.versionDefault)
  }

  def exportConfigJson(config: ConfigJsonExporterConfig)(implicit hadoopConf: Configuration): String = {
    var sdlConfig = ConfigLoader.loadConfigFromFilesystem(config.configPaths, hadoopConf)
    // remove additional config paths introduced by system properties...
    val configKeysToRemove = sdlConfig.root.keySet().asScala.diff(Set(ConfigParser.CONFIG_SECTION_ACTIONS, ConfigParser.CONFIG_SECTION_CONNECTIONS, ConfigParser.CONFIG_SECTION_DATAOBJECTS, ConfigParser.CONFIG_SECTION_GLOBAL))
    sdlConfig = configKeysToRemove.foldLeft(sdlConfig)((config, key) => config.withoutPath(key))
    // enrich origin of first class config objects
    if (config.enrichOrigin) sdlConfig = enrichOrigin(config.configPaths, sdlConfig)
    // enrich optional column description from description files
    config.descriptionPath.foreach(path => sdlConfig = enrichColumnDescription(path, sdlConfig))
    // enrich optional source documentation for custom classes from scaladoc
    sdlConfig = enrichCustomClassScalaDoc(sdlConfig)
    // enrich optional custom transformer parameter info
    sdlConfig = enrichCustomTransformerParameters(sdlConfig)
    // render config as json
    sdlConfig.root.render(ConfigRenderOptions.concise())
  }

  def uploadDescriptions(config: ConfigJsonExporterConfig, writer: ExportWriter, version: Option[String])(implicit hadoopConf: Configuration): Unit = {
    require(config.descriptionPath.nonEmpty, "descriptionPath must be set if uploadDescriptions=true")
    implicit val filesystem: FileSystem = Environment.fileSystemFactory.getFileSystem(new Path(config.descriptionPath.get), hadoopConf)
    val path = new Path(config.descriptionPath.get)
    if (filesystem.exists(path)) {
      val existingFiles = try {
        writer.listFiles(version).map(f => (f.name.dropWhile(_ == '/'), f)).toMap
      } catch {
        case _: NotImplementedException =>
          logger.info(s"Target ${writer.getClass.getSimpleName} does not support listing existing files.")
          Map[String, FileDescriptor]()
      }
      val filesToDelete = mutable.Set(existingFiles.keySet.toSeq: _*)
      logger.info(s"Searching description files in $path")
      RemoteIteratorWrapper(filesystem.listFiles(path, true))
        .filterNot(_.isDirectory)
        .foreach { p =>
          val relativePath = p.getPath.toString.substring(p.getPath.toString.lastIndexOf(path.toString) + path.toString.length + 1)
          // initialize delete list
          val existingFile = existingFiles.get(relativePath)
          if (existingFile.exists(f => f.size == p.getLen && f.lastModified.getTime > p.getModificationTime)) {
            logger.info(s"File $relativePath already uploaded")
          } else {
            val content = Using.resource(filesystem.open(p.getPath)) {
              is => is.readAllBytes
            }
            writer.writeFile(content, relativePath, version)
          }
          // remove from delete list
          filesToDelete -= relativePath
        }
      // delete no longer existing files
      filesToDelete.foreach(f => writer.deleteFile(f, version))
    } else throw new IllegalArgumentException(s"Description path $path not found!")
  }

  private def enrichCustomClassScalaDoc(config: Config): Config = {
    // we are looking for className and type attributes
    def searchCondition(key: String, value: ConfigValue) = {
      val lccKey = DataFrameUtil.strToLowerCamelCase(key)
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

  private def enrichCustomTransformerParameters(config: Config)(implicit hadoopConf: Configuration): Config = {
    // we are looking for type = ScalaClassSparkDfsTransformer (for now)
    def searchCondition(key: String, value: ConfigValue) = {
      // condition
      value.valueType == ConfigValueType.STRING &&
        key == "type" &&
        value.unwrapped.asInstanceOf[String] == classOf[ScalaClassSparkDfsTransformer].getSimpleName
    }
    val customTransformerConfigurationPaths = HoconUtil.findInConfigObject(config.root, searchCondition)
    // enrich config with ScalaClassSparkDfsTransformer parameters if available
    logger.info(s"Enriching custom transformer parameter information for ${customTransformerConfigurationPaths.length} transformers")
    customTransformerConfigurationPaths.foldLeft(config) {
      case (config, path) =>
        val className = getConfigValue(config.root(), path.init :+ "className").unwrapped().asInstanceOf[String]
        val classInstance = CustomCodeUtil.getClassInstanceByName[CustomTransformMethodDef](className)
        val wrapper = classInstance.customTransformMethod.map(new CustomTransformMethodWrapper(_))
        val parameters = wrapper.map(_.getParameterInfo())
        if (parameters.isDefined) {
          val parametersValue = ConfigValueFactory.fromIterable(parameters.get.map(p => ConfigValueFactory.fromMap(p.toMap.asJava)).asJava)
          updateConfigValue(config.root(), path.init :+ "_parameters", parametersValue).asInstanceOf[ConfigObject].toConfig
        }
        else config
    }
  }

}
