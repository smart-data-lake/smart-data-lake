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

import io.smartdatalake.app.GlobalConfig
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigLoader, ConfigurationException}
import io.smartdatalake.util.misc.FileUtil.readFile
import io.smartdatalake.util.misc.{SmartDataLakeLogger, URIUtil}
import io.smartdatalake.util.webservice.ScalaJWebserviceClient
import org.apache.commons.lang.NotImplementedException
import org.apache.hadoop.conf.Configuration
import sttp.model.Method

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
  def apply(uri: String, configPaths: Seq[String]): ExportWriter = {
    uri.takeWhile(_ != ':').toLowerCase match {
      case "file" => FileExportWriter(Paths.get(uri.stripPrefix("file:")))
      case "uibackend" => UIExportWriter(configPaths)
      case "http" | "https" => HttpExportWriter(uri)
      case x => throw new ConfigurationException(s"scheme of target uri must be 'file:', 'uiBackend' or 'http[s]:', but got '${x}:'")
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


case class UIExportWriter(configPaths: Seq[String]) extends ExportWriter with SmartDataLakeLogger {

  implicit val hadoopConf: Configuration = new Configuration()
  private val config = ConfigLoader.loadConfigFromFilesystem(configPaths, hadoopConf)
  private val globalConfig = GlobalConfig.from(config)
  private val uploader = globalConfig.uiBackend.map(_.getUploadService)
    .getOrElse(throw ConfigurationException("global.uiBackend configuration missing in SDLB configuration files"))

  override def writeConfig(document: String, version: Option[String]): Unit = {
    upload(document.getBytes("UTF-8"), "config", additionalParams = Seq(version.map("version" -> _)).flatten.toMap)
  }

  override def writeSchema(document: String, dataObjectId: DataObjectId, tstamp: Long): Unit = {
    upload(document.getBytes("UTF-8"), s"dataobject/schema/${dataObjectId.id}", additionalParams = Map("tstamp" -> tstamp.toString))
  }

  override def writeStats(document: String, dataObjectId: DataObjectId, tstamp: Long): Unit = {
    upload(document.getBytes("UTF-8"), s"dataobject/stats/${dataObjectId.id}", additionalParams = Map("tstamp" -> tstamp.toString))
  }

  override def writeFile(content: Array[Byte], filename: String, version: Option[String]): Unit = {
    upload(content, "description", additionalParams = Seq(Some("filename" -> filename), version.map("version" -> _)).flatten.toMap)
  }

  private def upload(content: Array[Byte], subPath: String, method: Method = Method.PUT, additionalParams: Map[String, String] = Map()): Unit = {
    logger.info(s"Uploading $subPath " + additionalParams.map { case (k, v) => s"$k=$v" }.mkString(" "))
    uploader.sendBytes(subPath, content, method, additionalParams = additionalParams)
  }
}

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
    upload(content, "description",  Seq(Some("filename" -> filename), version.map("version" -> _)).flatten.toMap)
  }

  private def upload(content: Array[Byte], subPath: String, additionalParams: Map[String,String] = Map()): Unit = {
    logger.info(s"Uploading $subPath "+additionalParams.map{case (k,v) => s"$k=$v"}.mkString(" "))
    val wsClient = ScalaJWebserviceClient(URIUtil.appendPath(baseUrl, subPath), Map(), None, None, None, followRedirects = true)
    wsClient.put(content, "application/json", additionalParams).get
  }
}