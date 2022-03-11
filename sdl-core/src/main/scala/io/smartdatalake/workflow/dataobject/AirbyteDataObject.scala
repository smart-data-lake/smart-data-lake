/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.dataobject

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.networknt.schema.{JsonSchema, JsonSchemaFactory, SpecVersion, ValidationMessage}
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.json.JsonUtils._
import io.smartdatalake.util.json.SchemaConverter
import io.smartdatalake.util.misc.{DataFrameUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.action.script.{CmdScript, DockerRunScript, ParsableScriptDef}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.Formats
import org.json4s.jackson.JsonMethods.compact

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}

/**
 *
 * Limitations: Connectors have only access to locally mounted directories
 *
 * @param id DataObject identifier
 * @param config Configuration for the source
 * @param streamName The stream name to read. Must match an entry of the catalog of the source.
 * @param incrementalCursorFields Some sources need a specification of the cursor field for incremental mode
 * @param cmd command to launch airbyte connector. Normally this is of type [[DockerRunScript]].
 */
case class AirbyteDataObject(override val id: DataObjectId,
                             config: Config,
                             streamName: String,
                             cmd: ParsableScriptDef,
                             incrementalCursorFields: Seq[String] = Seq(),
                             override val schemaMin: Option[StructType] = None,
                             override val metadata: Option[DataObjectMetadata] = None)
  extends DataObject with CanCreateDataFrame with CanCreateIncrementalOutput with SchemaValidation with SmartDataLakeLogger {

  // these variables will be initialized in prepare phase
  private var spec: Option[AirbyteConnectorSpecification] = None
  private var catalog: Option[AirbyteCatalog] = None
  private var configuredStream: Option[ConfiguredAirbyteStream] = None
  private var schema: Option[StructType] = None
  private var state: Option[String] = None

  // the json de/serialization definition to use
  implicit val jsonFormats: Formats = AirbyteMessage.formats

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    // spec: get connector specification
    spec = Some(findMsg[AirbyteConnectorSpecification](launchConnector(Operation.spec)))

    val configJson = configToJsonString(config)

    val validationMsgs = JsonValidator.validateJsonSchema(jsonToString(spec.get.connectionSpecification), configJson)
    if (validationMsgs.nonEmpty) throw AirbyteConnectorException(s"($id) Connector config is invalid: ${validationMsgs.map(_.getMessage).mkString(", ")}"+System.lineSeparator()+"Config specification is:"+System.lineSeparator()+configJson)
    // check: checks connection
    val status = findMsg[AirbyteConnectionStatus](launchConnector(Operation.check, Some(config)))
    if (status.status != AirbyteConnectionStatus.Status.SUCCEEDED) throw AirbyteConnectorException(s"($id) Connection check failed: ${status.message}")
    // discover: check streamName exists, get schema, prepare catalog for read
    catalog = Some(findMsg[AirbyteCatalog](launchConnector(Operation.discover, Some(config))))
    val stream = catalog.get.streams.find(_.name == streamName)
    if (stream.isEmpty) throw AirbyteConnectorException(s"($id) Stream '$streamName' not found in catalog. Available streams are: ${catalog.get.streams.map(_.name).mkString(", ")}")
    configuredStream = Some(
      ConfiguredAirbyteStream(stream.get, SyncModeEnum.full_refresh, if (incrementalCursorFields.nonEmpty) Some(incrementalCursorFields) else None, DestinationSyncModeEnum.append, primary_key = None)
    )
    schema = Some(SchemaConverter.convert(jsonToString(configuredStream.get.stream.json_schema)))
    logger.info(s"($id) got schema: ${schema.get.simpleString}")
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    assert(configuredStream.nonEmpty, s"($id) prepare must be called before getDataFrame")
    implicit val session = context.sparkSession
    import session.implicits._

    val df = context.phase match {
      // init phase -> return empty dataframe
      case ExecutionPhase.Init =>
        DataFrameUtil.getEmptyDataFrame(schema.get)
      // exec phase -> read: return data
      case ExecutionPhase.Exec =>
        val configuredCatalog = ConfiguredAirbyteCatalog(List(configuredStream.get))
        // TODO:
        //  For now we read everything on the driver, this could be a memory problem.
        //  But the question is how to split into multiple launchConnector runs, or limit the number of rows per batch?
        //  Maybe we could do something similar to spark.sql.files.maxPartitionBytes and create multiple partitions on the driver?
        val data = launchConnector(Operation.read, Some(config), Some(configuredCatalog), state)
          .flatMap {
            case x: AirbyteRecordMessage => Some(x)
            case x: AirbyteStateMessage =>
              state = Some(compact(x.data))
              None
            case _ => None
          }
        val dfRaw = data.map(r => jsonToString(r.data)).toSeq.toDF("json")
        dfRaw.withColumn("parsed", from_json($"json", schema.get))
          .select($"parsed.*")
    }

    validateSchemaMin(df, "read")
    df
  }

  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {
    assert(configuredStream.nonEmpty, s"($id) prepare must be called before setState")
    assert(spec.exists(_.supportsIncremental), s"${id} Connector does not support incremental output")
    assert(configuredStream.exists(_.stream.supported_sync_modes.contains(SyncModeEnum.incremental)), s"${id} Stream '$streamName' does not support incremental output")
    this.state = state
    if (state.isDefined) configuredStream = configuredStream.map(_.copy(sync_mode = SyncModeEnum.incremental))
    else configuredStream = configuredStream.map(_.copy(sync_mode = SyncModeEnum.full_refresh))
  }

  override def getState: Option[String] = {
    assert(configuredStream.nonEmpty, s"($id) prepare must be called before getState")
    assert(spec.exists(_.supportsIncremental), s"${id} Connector does not support incremental output")
    assert(configuredStream.exists(_.sync_mode == SyncModeEnum.incremental), s"${id} Stream configuration must be set to SyncMode.Incremental by calling setState before")
    state
  }

  final val configFilename = "config.json"
  final val catalogFilename = "catalog.json"
  final val stateFilename = "state.json"
  final val containerConfigDir = "/mnt/config"

  private def launchConnector(op: Operation.Operation, config: Option[Config] = None, catalog: Option[ConfiguredAirbyteCatalog] = None, state: Option[String] = None)(implicit context: ActionPipelineContext): Iterator[AirbyteMessage] = {
    val errors = collection.mutable.Buffer[String]()
    try {

      // convert config and catalog
      val configJson = config.map(configToJsonString)
      val catalogJson = catalog.map(caseClassToJsonString)

      // write interface files
      val tempPath = Files.createTempDirectory(id.id)
      configJson.map(s => Files.write(tempPath.resolve(configFilename), s.getBytes(StandardCharsets.UTF_8)))
      catalogJson.map(s => Files.write(tempPath.resolve(catalogFilename), s.getBytes(StandardCharsets.UTF_8)))
      state.map(s => Files.write(tempPath.resolve(stateFilename), s.getBytes(StandardCharsets.UTF_8)))

      // prepare parameters
      val (parameters) = cmd match {
        case dockerCmd: DockerRunScript =>
          val dockerParams = Seq("run", "--rm", "-v", s"${dockerCmd.preparePath(tempPath.toString)}:$containerConfigDir")
          val runParams = Seq(
            Some(Seq(op)),
            configJson.map(_ => Seq("--config", s"$containerConfigDir/$configFilename")),
            catalogJson.map(_ => Seq("--catalog", s"$containerConfigDir/$catalogFilename")),
            state.map(_ => Seq("--state", s"$containerConfigDir/$stateFilename"))
          ).flatten.flatten
          dockerParams.zipWithIndex.map{ case (p,i) => (f"dockerParam$i%02d", p)} ++ runParams.zipWithIndex.map{ case (p,i) => (f"runParam$i%02d", p)}
        case scriptCmd: CmdScript =>
          val params = Seq(
            Some(Seq(op)),
            configJson.map(_ => Seq("--config", s"${scriptCmd.preparePath(tempPath.resolve(configFilename).toString)}")),
            catalogJson.map(_ => Seq("--catalog", s"${scriptCmd.preparePath(tempPath.resolve(catalogFilename).toString)}")),
            state.map(_ => Seq("--state", s"${scriptCmd.preparePath(tempPath.resolve(stateFilename).toString)}"))
          ).flatten.flatten
          params.zipWithIndex.map{ case (p,i) => (f"param$i%02d", p)}
      }

      //launch process
      val linesStream = cmd.execStdOutStream(id, Seq(), parameters.toMap, errors)

      // parse result
      AirbyteMessage.parseOutput(linesStream, errors).toIterator

    } catch {
      case ex: Exception => throw AirbyteConnectorException(s"($id) Could not launch connector: ${errors.mkString(", ")}, ${ex.getMessage}", ex)
    }
  }

  private def findMsg[T <: AirbyteMessage : ClassTag](msgs: Iterator[AirbyteMessage]): T = {
    val cls = classTag[T]
    val msg = msgs.collectFirst{ case x: T => x }
    msg.getOrElse(throw AirbyteConnectorException(s"($id) Expected message of type '${cls.runtimeClass.getSimpleName}' not found in connector output"))
  }

  override def factory: FromConfigFactory[DataObject] = AirbyteDataObject
}

object AirbyteDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AirbyteDataObject = {
    extract[AirbyteDataObject](config)
  }
}

private object Operation extends Enumeration {
  type Operation = String
  val spec = "spec"
  val check = "check"
  val discover = "discover"
  val read = "read"
}

case class AirbyteConnectorException(msg: String, cause: Throwable = null) extends Exception(msg, cause)

object JsonValidator {
  private val validator = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
  private val parser = new ObjectMapper().registerModule(new JavaTimeModule)
  parser.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  def getSchemaValidator(schema: String): JsonSchema = {
    validator.getSchema(schema)
  }
  def validateJsonSchema(schema: String, data: String): Seq[ValidationMessage] = {
    getSchemaValidator(schema).validate(parser.readTree(data)).asScala.toSeq
  }
  def validateJsonSchema(schema: JsonSchema, data: String): Seq[ValidationMessage] = {
    schema.validate(parser.readTree(data)).asScala.toSeq
  }
}

