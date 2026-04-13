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
package io.smartdatalake.config.exporter

import io.smartdatalake.app.BackendClient
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.GenericSchema
import org.apache.commons.lang.NotImplementedException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.pretty
import org.json4s.{JArray, JObject}

import java.nio.file.Paths
import java.sql.Timestamp

trait ExportWriter {
  def writeConfig(document: String, version: Option[String]): Unit

  def writeSchema(document: String, dataObjectId: DataObjectId, version: Long): Unit

  def writeStats(document: String, dataObjectId: DataObjectId, version: Long): Unit

  def writeFile(content: Array[Byte], filename: String, version: Option[String]): Unit = throw new NotImplementedException()

  def deleteFile(filename: String, version: Option[String]): Unit = throw new NotImplementedException()

  def listFiles(version: Option[String]): Seq[FileDescriptor] = throw new NotImplementedException()

  def readLatestSchema(dataObjectId: DataObjectId): Option[String] = throw new NotImplementedException()
}


object ExportWriter {

  /**
   * create document writer depending on target uri scheme
   */
  def apply(uri: String, configPaths: Seq[String] = Seq(), backendClient: Option[BackendClient] = None, hadoopConfig: Option[Configuration] = None): ExportWriter = {
    uri.takeWhile(_ != ':').toLowerCase match {
      case "uibackend" =>
        backendClient
          .orElse(if (configPaths.nonEmpty) Some(BackendClient(configPaths)) else None)
          .getOrElse(throw new IllegalArgumentException(s"cannot initialize BackendClient as configPaths and global.uiBackend are missing"))
      case "http" | "https" => HttpExportWriter(uri)
      case "localfile" => FileExportWriter(Paths.get(uri.stripPrefix("localfile:")))
      case _ => HadoopExportWriter(new HadoopPath(uri), hadoopConfig.getOrElse(new Configuration()))
    }
  }

  def formatSchema(schema: Option[GenericSchema], info: Option[String]): String = {
    val contentJson = JObject(Seq(
      info.toSeq.map("info" -> JString(_)),
      schema.toSeq.map("schema" -> _.toJson),
      schema.toSeq.map(s => "subFeedType" -> JString(s.subFeedType.typeSymbol.name.toString))
    ).flatten: _*)
    pretty(contentJson)
  }

  def parseSchema(content: String): (GenericSchema, Option[String]) = {
    val json = JsonMethods.parse(content) match {
      case jObj: org.json4s.JObject => jObj
      case _ => throw new IllegalStateException("Not a valid Json object")
    }
    val schema = json \ "schema" match {
      case jsonSchema: JArray =>
        val subFeedType = (json \ "subFeedType") match {
          case JString(tpe) =>
            DataFrameSubFeed.getKnownSubFeedTypes.find(_.typeSymbol.name.toString.endsWith(tpe))
              .getOrElse(throw new IllegalStateException(s"Could not find SubFeedType $tpe"))
          case _ => throw new IllegalStateException(s"Attribute 'subFeedType' not found")
        }
        GenericSchema.fromJson(jsonSchema, subFeedType)
      case _ => throw new IllegalStateException(s"Attribute 'schema' not found")
    }
    val info = (json \ "info") match {
      case JString(s) => Some(s)
      case _ => None
    }
    (schema, info)
  }
}


case class FileDescriptor(name: String, mediaType: String, size: Long, lastModified: Timestamp)

private[smartdatalake] object UploadDefaults {
  val versionDefault = "latest"
}