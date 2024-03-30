/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.meta.configexporter

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.misc.FileUtil.readFile
import io.smartdatalake.util.misc.UploadDefaults.uploadMimeType
import io.smartdatalake.util.misc.{SmartDataLakeLogger, URIUtil, UploadDefaults}
import io.smartdatalake.util.webservice.ScalaJWebserviceClient
import org.apache.commons.lang.NotImplementedException

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.io.Source
import scala.util.Using

trait ExportWriter {
  def writeConfig(document: String, version: Option[String]): Unit
  def writeSchema(document: String, dataObjectId: DataObjectId, version: Long): Unit
  def writeStats(document: String, dataObjectId: DataObjectId, version: Long): Unit
  def writeFile(content: Array[Byte], filename: String, version: Option[String]): Unit = throw new NotImplementedException()
}

object ExportWriter {

  /**
   * create document writer depending on target uri scheme
   */
  def apply(uri: String): ExportWriter = {
    uri.takeWhile(_ != ':') match {
      case "file" => FileExportWriter(Paths.get(uri.stripPrefix("file:")))
      case "http" | "https" => HttpExportWriter(uri)
      case x => throw new ConfigurationException(s"scheme of target uri must be 'file:' or 'http[s]:', but got '${x}:'")
    }
  }
}

case class FileExportWriter(path: Path) extends ExportWriter with SmartDataLakeLogger {

  override def writeConfig(document: String, version: Option[String]): Unit = {
    writeFile(document, "config")
  }

  override def writeSchema(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    writeWithIndex(document, dataObjectId, "schema", version)
  }

  override def writeStats(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    writeWithIndex(document, dataObjectId, "stats", version)
  }

  private def writeWithIndex(document: String, dataObjectId: DataObjectId, tpe: String, version: Long): Unit = {
    if (path.getParent != null) Files.createDirectories(path)
    val indexFile = getIndexPath(dataObjectId, tpe)
    val (newFilename, newFile) = getDataPath(dataObjectId, tpe, version)
    val latestDocument = getLatestData(dataObjectId, tpe)
    if (!latestDocument.contains(document)) {
      logger.info(s"Writing $tpe for $dataObjectId to file $newFile and updating index")
      Files.write(newFile, document.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      Files.write(indexFile, (newFilename + System.lineSeparator).getBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    }
  }

  private def writeFile(document: String, tpe: String): Unit = {
    if (path.getParent != null) Files.createDirectories(path.getParent)
    logger.info(s"Writing $tpe to file $path")
    Files.write(path, document.getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  private[configexporter] def getLatestData(dataObjectId: DataObjectId, tpe: String): Option[String] ={
    val lastIndexEntry = readIndex(dataObjectId, tpe).lastOption
    val latestFile = lastIndexEntry.map(path.resolve).map(_.toFile)
    latestFile.map(readFile)
  }

  private[configexporter] def readIndex(dataObjectId: DataObjectId, tpe: String): Seq[String] = {
    val indexFile = getIndexPath(dataObjectId, tpe)
    Using(Source.fromFile(indexFile.toFile)) {
      _.getLines().filter(_.trim.nonEmpty).toVector
    }.getOrElse(Seq())
  }

  private def getIndexPath(dataObjectId: DataObjectId, tpe: String) = {
    path.resolve(s"${dataObjectId.id}.$tpe.index")
  }

  private def getDataPath(dataObjectId: DataObjectId, tpe: String, version: Long) = {
    val filename = s"${dataObjectId.id}.$tpe.$version.json"
    val file = path.resolve(filename)
    (filename, file)
  }
}

case class HttpExportWriter(baseUrl: String) extends ExportWriter with SmartDataLakeLogger {

  private val uploadWsClients = Seq("config", "schema", "stats", "description").map(
    tpe => (tpe -> ScalaJWebserviceClient(URIUtil.appendPath(baseUrl, tpe), Map(), None, None, None, followRedirects = true))
  ).toMap

  override def writeConfig(document: String, version: Option[String]): Unit = {
    upload(document.getBytes("UTF-8"), "config", Seq(version.map("version" -> _)).flatten.toMap)
  }

  override def writeSchema(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    upload(document.getBytes("UTF-8"), "schema", Map("dataObjectId" -> dataObjectId.id, "version" -> version.toString))
  }

  override def writeStats(document: String, dataObjectId: DataObjectId, version: Long): Unit = {
    upload(document.getBytes("UTF-8"), "stats", Map("dataObjectId" -> dataObjectId.id, "version" -> version.toString))
  }

  override def writeFile(content: Array[Byte], filename: String, version: Option[String]): Unit = {
    upload(content, "description",  Seq(Some("filename" -> filename), version.map("version" -> _)).flatten.toMap)
  }

  private def upload(content: Array[Byte], tpe: String, additionalParams: Map[String,String] = Map()): Unit = {
    val defaultUploadCategoryParams = Map(
      "tenant" -> (if (!baseUrl.contains("tenant=")) Some(UploadDefaults.privateTenant) else None),
      "env" -> (if (!baseUrl.contains("env=")) Some(UploadDefaults.envDefault) else None),
    ).filter(_._2.nonEmpty).mapValues(_.get)
    assert(baseUrl.contains("repo="), throw new IllegalArgumentException(s"repository not defined in upload target=$baseUrl, add query parameter 'repo' to target, e.g. https://<host>?repo=<repository>"))
    logger.info(s"Uploading $tpe "+additionalParams.map{case (k,v) => s"$k=$v"}.mkString(" "))
    uploadWsClients(tpe).post(content, uploadMimeType, defaultUploadCategoryParams ++ additionalParams).get
  }
}