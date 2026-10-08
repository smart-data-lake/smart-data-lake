/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
import io.smartdatalake.config.exporter.ExportType.ExportType
import io.smartdatalake.config.exporter.{ExportWriter, FileDescriptor, StateRunId, StateSyncEndpoint}
import io.smartdatalake.config.{ConfigLoader, ConfigurationException}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.HttpRequestError
import io.smartdatalake.workflow.action.SDLExecutionId
import org.apache.commons.lang3.NotImplementedException
import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.JsonMethods
import org.json4s.{CustomSerializer, DefaultFormats, Formats, JString}
import sttp.client3.multipart
import sttp.model.{MediaType, Method}

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.annotation.tailrec


case class BackendClient(uploader: UploadService) extends ExportWriter with StateSyncEndpoint with SmartDataLakeLogger {

  override def writeConfig(document: String, version: Option[String]): Unit = {
    upload(document, "config", additionalParams = Seq(version.map("version" -> _)).flatten.toMap)
  }

  override def writeSchema(document: String, dataObjectId: DataObjectId, tstamp: Long): Unit = {
    upload(document, s"dataobject/schema/${dataObjectId.id}", additionalParams = Map("tstamp" -> tstamp.toString))
  }

  override def writeStats(document: String, dataObjectId: DataObjectId, tstamp: Long): Unit = {
    upload(document, s"dataobject/stats/${dataObjectId.id}", additionalParams = Map("tstamp" -> tstamp.toString))
  }

  override def writeLineage(document: String, dataObjectId: DataObjectId, tstamp: Long): Unit = {
    upload(document, s"dataobject/lineage/${dataObjectId.id}", additionalParams = Map("tstamp" -> tstamp.toString))
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

  override def listVersions(tpe: ExportType, dataObjectId: DataObjectId): Seq[Long] = {
    download(s"dataobject/$tpe/${dataObjectId.id}/tstamps")
      .map(JsonMethods.parse(_).extract[Seq[Long]])
      .getOrElse(Seq())
  }

  override def readVersion(tpe: ExportType, dataObjectId: DataObjectId, tstamp: Long): Option[String] = {
    download(s"dataobject/$tpe/${dataObjectId.id}", additionalParams = Map("tstamp" -> tstamp.toString))
      .map(ExportWriter.unwrapDownloadedDocument(tpe, _))
  }

  override def writeState(stateJson: String): Unit = {
    upload(stateJson, "state", method = Method.POST)
  }

  override def listApplications(): Seq[String] = {
    download("workflows")
      .map(JsonMethods.parse(_).extract[Seq[WorkflowSummary]].map(_.name))
      .getOrElse(Seq())
  }

  /**
   * The UI backend returns the runs newest first, in pages of at most [[runsPageSize]] runs.
   * The next page is requested with the oldest run of the previous page as cursor.
   */
  override def listRuns(application: String): Seq[StateRunId] = {
    @tailrec
    def listPages(before: Option[StateRunId], runs: Seq[StateRunId]): Seq[StateRunId] = {
      val page = listRunsPage(application, runsPageSize, before)
      val olderRuns = page.filter(run => before.forall(StateRunId.ordering.lt(run, _)))
      if (olderRuns.size < page.size) {
        // a backend not supporting paging ignores the cursor and returns its latest runs again
        logger.warn(s"UI backend does not support paging runs, only the ${runs.size} latest runs of application $application are listed")
        runs
      } else if (page.size < runsPageSize) runs ++ page
      else listPages(Some(page.min), runs ++ page)
    }
    listPages(None, Seq())
  }

  override def latestRun(application: String): Option[StateRunId] = {
    listRunsPage(application, 1, None).maxOption
  }

  private def listRunsPage(application: String, limit: Int, before: Option[StateRunId]): Seq[StateRunId] = {
    val params = Map("application" -> application, "limit" -> limit.toString) ++
      before.map(run => Map("beforeRunId" -> run.runId.toString, "beforeAttemptId" -> run.attemptId.toString)).getOrElse(Map())
    download("workflow", additionalParams = params)
      .map(JsonMethods.parse(_).extract[Seq[StateRunId]])
      .getOrElse(Seq())
  }

  private val runsPageSize = 200

  /**
   * Not supported, as the UI backend converts the state on upload into a format which SDLB cannot read anymore.
   */
  override def readState(application: String, run: StateRunId): Option[String] = {
    throw new NotImplementedException("Downloading state from the UI backend is not supported, as the UI backend stores it in a different format than SDLB")
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

  /**
   * @return None if the document does not exist (HTTP 404)
   */
  private def download(subPath: String, additionalParams: Map[String, String] = Map()): Option[String] = {
    logger.info(s"Downloading $subPath " + additionalParams.map { case (k, v) => s"$k=$v" }.mkString(" "))
    try {
      uploader.send(subPath, method = Method.GET, additionalParams = additionalParams)
    } catch {
      case HttpRequestError(_, 404, _) => None
    }
  }

  private def upload(content: String, subPath: String, method: Method = Method.PUT, additionalParams: Map[String, String] = Map()): Unit = {
    logger.info(s"Uploading $subPath " + additionalParams.map { case (k, v) => s"$k=$v" }.mkString(" "))
    uploader.send(subPath, body = Some(content), method = method, additionalParams = additionalParams)
  }

  org.json4s.ext.JavaTimeSerializers.all
  implicit private val formats: Formats = DefaultFormats + new CustomSerializer[Timestamp](_ => (
    { case json: JString => Timestamp.from(OffsetDateTime.parse(json.s).toInstant) },
    { case obj: Timestamp => JString(obj.toLocalDateTime.toString) }
  ))

  private def parseFileDescriptors(jsonStr: String): Seq[FileDescriptor] = {
    val json = JsonMethods.parse(jsonStr).camelizeKeys.transformField {
      case ("type", x) => ("mediaType", x)
    }
    json.extract[Seq[FileDescriptor]]
  }
}

private case class WorkflowSummary(name: String)

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
