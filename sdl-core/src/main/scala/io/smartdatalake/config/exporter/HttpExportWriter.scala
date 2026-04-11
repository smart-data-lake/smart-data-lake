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

package io.smartdatalake.config.exporter

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.misc.{SmartDataLakeLogger, URIUtil}
import io.smartdatalake.util.webservice.SttpWebserviceClient


case class HttpExportWriter(baseUrl: String) extends ExportWriter with SmartDataLakeLogger {

  override def writeConfig(document: String, version: Option[String]): Unit = {
    upload(document.getBytes("UTF-8"), "config", Seq(version.map("version" -> _)).flatten.toMap)
  }

  override def writeSchema(document: String, dataObjectId: DataObjectId, tstamp: Long): Unit = {
    upload(document.getBytes("UTF-8"), s"dataobject/schema/${dataObjectId.id}", Map("tstamp" -> tstamp.toString))
  }

  override def writeStats(document: String, dataObjectId: DataObjectId, tstamp: Long): Unit = {
    upload(document.getBytes("UTF-8"), s"dataobject/stats/${dataObjectId.id}", Map("tstamp" -> tstamp.toString))
  }

  override def writeFile(content: Array[Byte], filename: String, version: Option[String]): Unit = {
    upload(content, "descriptions", Seq(Some("filename" -> filename), version.map("version" -> _)).flatten.toMap)
  }

  private def upload(content: Array[Byte], subPath: String, additionalParams: Map[String, String] = Map()): Unit = {
    logger.info(s"Uploading $subPath " + additionalParams.map { case (k, v) => s"$k=$v" }.mkString(" "))
    val wsClient = SttpWebserviceClient(url = URIUtil.appendPath(baseUrl, subPath), additionalHeaders = Map(), timeouts = None, authMode = None, proxy = None, followRedirects = true, retries = 1, sttpBackendOption = None)
    wsClient.put(content, "application/json", additionalParams).get
  }
}