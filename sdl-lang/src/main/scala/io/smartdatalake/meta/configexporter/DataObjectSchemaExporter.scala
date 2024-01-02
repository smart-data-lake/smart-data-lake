package io.smartdatalake.meta.configexporter

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigToolbox, ConfigurationException}
import io.smartdatalake.util.misc.{ProductUtil, SerializableHadoopConfiguration, SmartDataLakeLogger}
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, SparkFileDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods.pretty
import org.json4s.jackson.Serialization
import org.json4s.{Formats, JObject, NoTypeHints}
import scopt.OptionParser

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.LocalDateTime
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}
import scala.collection.compat._

case class DataObjectSchemaExporterConfig(configPaths: Seq[String] = null,
                                          exportPath: String = "./schema",
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
    opt[String]('u', "updateStats")
      .optional()
      .action((value, c) => c.copy(updateStats = value.toBoolean))
      .text("If true, more costly operations to update statistics such as \"analyze table\" are executed before returning statistics. Default: true")
    opt[String]('m', "master")
      .optional()
      .action((value, c) => c.copy(master = value))
      .text("Spark session master configuration. As schemas might be inferred by Spark, there might be a need to tune this for some DataObjects. Default: local[2]")
    help("help").text("Export DataObject schemas as Json-files which can be used by the visualizer. A Json file with the DataObject Id as name is created in `exportPath` for every DataObject to be exported.")
  }

  /**
   * Takes as input an SDL Config and exports the schema of all DataObjects for the visualizer.
   */
  def main(args: Array[String]): Unit = {
    val exporterConfig = DataObjectSchemaExporterConfig()
    // Parse all command line arguments
    parser.parse(args, exporterConfig) match {
      case Some(exporterConfig) =>

        // export data object schemas to json format
        logger.info(s"starting with configuration ${ProductUtil.formatObj(exporterConfig)}")
        exportSchemas(exporterConfig)

      case None =>
        logAndThrowException(s"Aborting $appType after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  def exportSchemas(config: DataObjectSchemaExporterConfig): Unit = {

    // get DataObjects
    val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(config.configPaths)
    val hadoopConf = globalConfig.getHadoopConfiguration
    val path = Paths.get(config.exportPath)
    Files.createDirectories(path)
    implicit val context: ActionPipelineContext = ActionPipelineContext("feedTest", "appTest", SDLExecutionId.executionId1, registry, Some(LocalDateTime.now()), SmartDataLakeBuilderConfig("DataObjectSchemaExporter", Some("DataObjectSchemaExporter"), master=Some(config.master)), phase = ExecutionPhase.Init, serializableHadoopConf = new SerializableHadoopConfiguration(hadoopConf), globalConfig = globalConfig)
    val dataObjects = registry.getDataObjects
      .filter(d => d.id.id.matches(config.includeRegex) && (config.excludeRegex.isEmpty || !d.id.id.matches(config.excludeRegex.get)))
    logger.info(s"Writing ${dataObjects.size} data object schemas to json files in path ${config.exportPath}")

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
          writeSchemaIfChanged(dataObject.id, schema, info, path)
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
        writeStatsIfChanged(dataObject.id, stats, path)
      } catch {
        case ex: Exception =>
          logger.warn(s"${ex.getClass.getSimpleName}: ${ex.getMessage}")
      }
    }
  }

  def writeSchemaIfChanged(dataObjectId: DataObjectId, newSchema: Option[GenericSchema], info: Option[String], path: Path): Unit = {
    assert(newSchema.isDefined || info.isDefined)
    val newContentJson = JObject(Seq(info.toSeq.map("info" -> JString(_)), newSchema.toSeq.map("schema" -> _.toJson)).flatten:_*)
    val newContentStr =  pretty(newContentJson)
    val indexFile = getIndexPath(dataObjectId, "schema", path)
    val (newFilename, newFile) = getDataPath(dataObjectId, "schema", path)
    val latestSchema = getLatestData(dataObjectId, "schema", path)
    if (!latestSchema.contains(newContentStr)) {
      logger.info(s"Writing schema file $newFile and updating index")
      Files.write(newFile, newContentStr.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      Files.write(indexFile, (newFilename + System.lineSeparator).getBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    }
  }

  def writeStatsIfChanged(dataObjectId: DataObjectId, stats: Map[String,Any], path: Path): Unit = {
    val statsStr = Serialization.writePretty(stats)
    val indexFile = getIndexPath(dataObjectId, "stats", path)
    val (newFilename, newFile) = getDataPath(dataObjectId, "stats", path)
    val latestStats = getLatestData(dataObjectId, "stats", path).map(Serialization.read[Map[String, Any]])
    if (!latestStats.contains(stats)) {
      logger.info(s"Writing stats file $newFile and updating index")
      Files.write(newFile, statsStr.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      Files.write(indexFile, (newFilename + System.lineSeparator).getBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    }
  }

  private[configexporter] def getLatestData(dataObjectId: DataObjectId, tpe: String, path: Path): Option[String] ={
    val lastIndexEntry = readIndex(dataObjectId, tpe, path).lastOption
    val latestFile = lastIndexEntry.map(path.resolve).map(_.toFile)
    latestFile.map(readFile)
  }

  private[configexporter] def readIndex(dataObjectId: DataObjectId, tpe: String, path: Path): Seq[String] = {
    val indexFile = getIndexPath(dataObjectId, tpe, path)
    Using(Source.fromFile(indexFile.toFile)) {
      _.getLines().filter(_.trim.nonEmpty).toVector
    }.getOrElse(Seq())
  }

  private def getIndexPath(dataObjectId: DataObjectId, tpe: String, path: Path) = {
    path.resolve(s"${dataObjectId.id}.$tpe.index")
  }

  private def getDataPath(dataObjectId: DataObjectId, tpe: String, path: Path) = {
    val filename = s"${dataObjectId.id}.$tpe.${System.currentTimeMillis() / 1000}.json"
    val file = path.resolve(filename)
    (filename, file)
  }

  private def readFile(file: File): String = {
    Using(Source.fromFile(file)) {
      _.getLines().mkString(System.lineSeparator)
    }.get
  }
}
