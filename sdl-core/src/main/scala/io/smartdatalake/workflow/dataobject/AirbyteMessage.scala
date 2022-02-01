package io.smartdatalake.workflow.dataobject

import io.smartdatalake.util.json.SchemaConverter
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.types.StructType
import org.json4s.ext.EnumNameSerializer
import org.json4s.{DefaultFormats, Formats, JObject}

import scala.collection.mutable

/**
 * Methods to parse Arbyte message stresam
 */
private[smartdatalake] object AirbyteMessage extends SmartDataLakeLogger {
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  implicit val formats: Formats = DefaultFormats +
    new EnumNameSerializer(AirbyteLogMessage.Level) +
    new EnumNameSerializer(AirbyteConnectionStatus.Status) +
    new EnumNameSerializer(SyncModeEnum) +
    new EnumNameSerializer(DestinationSyncModeEnum)

  object Type extends Enumeration {
    val RECORD, STATE, LOG, SPEC, CONNECTION_STATUS, CATALOG = Value
  }

  /**
   * Parse stream of json lines as AirbyteMessages
   * Non-Json lines will be logged with level INFO.
   * Non valid messages are logged as ERROR and added to error buffer.
   * AirbyteLogMessages are logged and filtered if filterLog=true. If log level is ERROR they are added to error buffer as well.
   * Invalid
   */
  def parseOutput(lines: Stream[String], errBuffer: mutable.Buffer[String], filterLog: Boolean = true): Stream[AirbyteMessage] = {
    lines
      // parse json
      .flatMap( line =>
        try {
          logger.debug(line)
          if (line.trim.startsWith("{")) Some(parse(line))
          else throw new IllegalStateException(s"non-json line found: $line")
        } catch {
          // If a source logs to stdout, we log this as info. Note that it could also be corrupt json data...
          case ex: Exception =>
            logger.info(s"non json line received: $line")
            None
        }
      )
      // convert message
      .flatMap( json =>
        try {
          Some(extractObject(json))
        } catch {
          case ex: Exception =>
            val logMsg = s"Json message conversion failed: ${ex.getMessage} data='$json''"
            logger.error(logMsg)
            errBuffer.append(logMsg)
            None
        }
      )
      // handle and filter out log messages
      .filter {
        case msg: AirbyteLogMessage =>
          internalLog(msg)
          if (Seq(AirbyteLogMessage.Level.ERROR, AirbyteLogMessage.Level.FATAL).contains(msg.level)) {
            errBuffer.append(msg.message)
          }
          !filterLog
        case _ =>
          true
      }
  }

  private def internalLog(msg: AirbyteLogMessage): Unit = {
    msg.level match {
      case AirbyteLogMessage.Level.FATAL => logger.error(msg.message)
      case AirbyteLogMessage.Level.ERROR => logger.error(msg.message)
      case AirbyteLogMessage.Level.WARN  => logger.warn(msg.message)
      case AirbyteLogMessage.Level.INFO  => logger.info(msg.message)
      case AirbyteLogMessage.Level.DEBUG => logger.debug(msg.message)
      case AirbyteLogMessage.Level.TRACE => logger.trace(msg.message)
    }
  }

  private def extractObject(json: JValue): AirbyteMessage = {
    // extract object
    val msgTypeField = (json \ "type")
    if (msgTypeField.toOption.isEmpty) throw AirbyteConnectorException("Could not find field 'type' in message")
    val msgType = Type.withName(msgTypeField.extract[String])
    msgType match {
      case Type.RECORD => (json \ "record").extract[AirbyteRecordMessage]
      case Type.STATE => (json \ "state").extract[AirbyteStateMessage]
      case Type.LOG => (json \ "log").extract[AirbyteLogMessage]
      case Type.SPEC => (json \ "spec").extract[AirbyteConnectorSpecification]
      case Type.CONNECTION_STATUS => (json \ "connectionStatus").extract[AirbyteConnectionStatus]
      case Type.CATALOG => (json \ "catalog").extract[AirbyteCatalog]
    }
  }
}

/**
 * Trait to unify all AirbyteMessage types
 */
private[smartdatalake] sealed trait AirbyteMessage

/**
 * Airbyte log message
 */
private[smartdatalake] case class AirbyteLogMessage (
                               level: AirbyteLogMessage.Level.Value,
                               message: String
                             ) extends AirbyteMessage
private[smartdatalake] object AirbyteLogMessage {
  object Level extends Enumeration {
    val FATAL, CRITICAL, ERROR, WARN, WARNING, INFO, DEBUG, TRACE = Value
  }
}

/**
 * Airbyte Connector specification message
 */
private[smartdatalake] case class AirbyteConnectorSpecification (
                                           documentationUrl: Option[String] = None,
                                           changelogUrl: Option[String] = None,
                                           connectionSpecification: JObject,
                                           supportsIncremental: Boolean = false,
                                           supportsNormalization: Boolean = false,
                                           supportsDBT: Boolean = false,
                                           supported_destination_sync_modes: Option[Seq[DestinationSyncModeEnum.Value]] = None,
                                           authSpecification: Option[JObject] = None
                                         ) extends AirbyteMessage

/**
 * Airbyte Connection status message
 */
private[smartdatalake] case class AirbyteConnectionStatus (
                                     status: AirbyteConnectionStatus.Status.Value,
                                     message: Option[String] = None
                                   ) extends AirbyteMessage
private[smartdatalake] object AirbyteConnectionStatus {
  object Status extends Enumeration {
    val SUCCEEDED, FAILED = Value
  }
}

/**
 * Airbyte catalog message
 */
private[smartdatalake] case class AirbyteCatalog (
                            streams: Seq[AirbyteStream]
                          ) extends AirbyteMessage

/**
 * Airbyte state message
 */
private[smartdatalake] case class AirbyteStateMessage (
                                 data: JObject
                               ) extends AirbyteMessage

/**
 * Airbyte data record message
 */
private[smartdatalake] case class AirbyteRecordMessage (
                                  stream: String,
                                  data: JObject,
                                  emitted_at: Long,
                                  namespace: Option[String] = None
                                ) extends AirbyteMessage


private[smartdatalake] case class AirbyteStream(
                                                 name: String,
                                                 json_schema: JObject,
                                                 supported_sync_modes: Seq[SyncModeEnum.Value],
                                                 source_defined_cursor: Boolean = false,
                                                 default_cursor_field: Option[Seq[String]] = None,
                                                 source_defined_primary_key: Option[Seq[Seq[String]]] = None,
                                                 namespace: Option[String] = None
                                               ) {
  def getSparkSchema: StructType = {
    import org.json4s.jackson.JsonMethods._
    implicit val formats: Formats = DefaultFormats
    SchemaConverter.convert(compact(json_schema \ "properties"))
  }
}

private[smartdatalake] case class ConfiguredAirbyteCatalog (
                                      streams: Seq[ConfiguredAirbyteStream]
                                    )

private[smartdatalake] case class ConfiguredAirbyteStream (
                              stream: AirbyteStream,
                              sync_mode: SyncModeEnum.Value = SyncModeEnum.full_refresh,
                              cursor_field: Option[Seq[String]] = None,
                              destination_sync_mode: DestinationSyncModeEnum.Value = DestinationSyncModeEnum.append,
                              primary_key: Option[Seq[String]] = None
                            )

private[smartdatalake] object SyncModeEnum extends Enumeration {
  val full_refresh, incremental = Value
}

private[smartdatalake] object DestinationSyncModeEnum extends Enumeration {
  val append, overwrite, append_dedup = Value
}