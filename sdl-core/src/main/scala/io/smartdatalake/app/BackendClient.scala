/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.app

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.exporter.{ExportWriter, FileDescriptor}
import io.smartdatalake.config.{ConfigLoader, ConfigurationException}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.SDLExecutionId
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.Json4sCompat
import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, Formats, JString}
import sttp.client3.multipart
import sttp.model.{MediaType, Method}

import java.sql.Timestamp
import java.time.OffsetDateTime


case class BackendClient(uploader: UploadService) extends ExportWriter with SmartDataLakeLogger {

  override def writeConfig(document: String, version: Option[String]): Unit = {
    upload(document, "config", additionalParams = Seq(version.map("version" -> _)).flatten.toMap)
  }

  override def writeSchema(document: String, dataObjectId: DataObjectId, tstamp: Long): Unit = {
    upload(document, s"dataobject/schema/${dataObjectId.id}", additionalParams = Map("tstamp" -> tstamp.toString))
  }

  override def writeStats(document: String, dataObjectId: DataObjectId, tstamp: Long): Unit = {
    upload(document, s"dataobject/stats/${dataObjectId.id}", additionalParams = Map("tstamp" -> tstamp.toString))
  }

  override def writeFile(content: Array[Byte], filename: String, version: Option[String]): Unit = {
    val additionalParams = Seq(version.map("version" -> _)).flatten.toMap
    logger.info(s"Uploading descriptions/$filename " + additionalParams.map { case (k, v) => s"$k=$v" }.mkString(" "))
    uploader.sendBytes(s"descriptions/$filename", multipartBody = Some(Seq(multipart("file", content).fileName(filename))), method = Method.POST, additionalParams = additionalParams, mediaType = MediaType.MultipartFormData)
  }

  override def deleteFile(filename: String, version: Option[String]): Unit = {
    val additionalParams = Seq(version.map("version" -> _)).flatten.toMap
    logger.info(s"Deleting descriptions/$filename " + additionalParams.map { case (k, v) => s"$k=$v" }.mkString(" "))
    uploader.sendBytes(s"descriptions/$filename", method = Method.DELETE, additionalParams = additionalParams)
  }

  override def listFiles(version: Option[String]): Seq[FileDescriptor] = {
    val additionalParams = Seq(version.map("version" -> _)).flatten.toMap
    logger.info(s"get descriptions " + additionalParams.map { case (k, v) => s"$k=$v" }.mkString(" "))
    val response = uploader.sendBytes("descriptions/list", method = Method.GET, additionalParams = additionalParams)
      .getOrElse(throw new IllegalStateException("Got empty response for 'descriptions/list'"))
    parseFileDescriptors(response)
  }

  override def readLatestSchema(dataObjectId: DataObjectId): Option[String] = {
    import scala.collection.compat._
    val tpe = "schema"
    val subPath = s"dataobject/$tpe/${dataObjectId.id}"
    val tstampsSubPath = s"$subPath/tstamps"
    val tstamps = download(tstampsSubPath)
      .map(JsonMethods.parse(_).extract[Seq[Long]])
    val lastTstamp = tstamps.flatMap(_.maxOption)
    lastTstamp.flatMap(tstamp => download(subPath, additionalParams = Map("tstamp" -> tstamp.toString)))
  }

  def writeState(stateJson: String): Unit = {
    upload(stateJson, "state", method = Method.POST)
  }

  def updateState(stateJson: String, applicationName: String, executionId: SDLExecutionId, changedActionId: ActionId): Unit = {
    val runParams = Map(
      "application" -> applicationName,
      "runId" -> executionId.runId.toString,
      "attemptId" -> executionId.attemptId.toString,
      "actionId" -> changedActionId.id
    )
    upload(stateJson, "state", method = Method.PATCH, additionalParams = runParams)
  }

  private def download(subPath: String, additionalParams: Map[String, String] = Map()): Option[String] = {
    logger.info(s"Downloading $subPath " + additionalParams.map { case (k, v) => s"$k=$v" }.mkString(" "))
    uploader.send(subPath, method = Method.GET, additionalParams = additionalParams)
  }

  private def upload(content: String, subPath: String, method: Method = Method.PUT, additionalParams: Map[String, String] = Map()): Unit = {
    logger.info(s"Uploading $subPath " + additionalParams.map { case (k, v) => s"$k=$v" }.mkString(" "))
    uploader.send(subPath, body = Some(content), method = method, additionalParams = additionalParams)
  }

  implicit private val formats: Formats = DefaultFormats + Json4sCompat.getCustomSerializer[Timestamp](_ => ( {
    case json: JString => Timestamp.from(OffsetDateTime.parse(json.s).toInstant)
  }, {
    case obj: Timestamp => JString(obj.toLocalDateTime.toString)
  }
  ))

  private def parseFileDescriptors(jsonStr: String): Seq[FileDescriptor] = {
    val json = JsonMethods.parse(jsonStr).camelizeKeys.transformField {
      case ("type", x) => ("mediaType", x)
    }
    json.extract[Seq[FileDescriptor]]
  }
}

object BackendClient {
  def apply(configPaths: Seq[String]): BackendClient = {
    implicit val hadoopConf: Configuration = new Configuration()
    val config = ConfigLoader.loadConfigFromFilesystem(configPaths, hadoopConf)
    val globalConfig = GlobalConfig.from(config)
    val uploader = globalConfig.uiBackend.map(_.getUploadService)
      .getOrElse(throw ConfigurationException("global.uiBackend configuration missing in SDLB configuration files"))
    BackendClient(uploader)
  }
}
