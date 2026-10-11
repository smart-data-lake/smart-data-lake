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
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.exporter.ExportType.ExportType
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{ColumnLineage, ColumnLineageDebug, GenericSchema}
import org.apache.commons.lang3.NotImplementedException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.json4s.JsonAST.JString
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.pretty
import org.json4s.{JArray, JField, JInt, JObject, JValue}

import java.nio.file.Paths
import java.sql.Timestamp

trait ExportWriter extends SmartDataLakeLogger {
  def writeConfig(document: String, version: Option[String]): Unit

  def writeSchema(document: String, dataObjectId: DataObjectId, version: Long): Unit

  def writeStats(document: String, dataObjectId: DataObjectId, version: Long): Unit

  /**
   * Write the column level lineage of a DataObject, see [[io.smartdatalake.app.TestMode.DryRunWithLineageExport]].
   */
  def writeLineage(document: String, dataObjectId: DataObjectId, version: Long): Unit

  /**
   * Write why the column lineage of a DataObject could not be traced back completely, see
   * `Environment.columnLineageDebug`. This is diagnostic output for developing the lineage extraction, so
   * writers publishing to a service ignore it instead of uploading it there.
   */
  def writeLineageDebug(document: String, dataObjectId: DataObjectId): Unit = {
    logger.warn(s"${getClass.getSimpleName} does not write column lineage debug output, skipping $dataObjectId")
  }

  def writeFile(content: Array[Byte], filename: String, version: Option[String]): Unit = throw new NotImplementedException()

  def deleteFile(filename: String, version: Option[String]): Unit = throw new NotImplementedException()

  def listFiles(version: Option[String]): Seq[FileDescriptor] = throw new NotImplementedException()

  def readLatestSchema(dataObjectId: DataObjectId): Option[String] = readLatest(ExportType.Schema, dataObjectId)

  /**
   * List the versions of a document stored for a DataObject, see [[ExportType]].
   * The versions are timestamps in epoch seconds. An empty list is returned if no document is stored.
   */
  def listVersions(tpe: ExportType, dataObjectId: DataObjectId): Seq[Long] = throw new NotImplementedException(s"${getClass.getSimpleName} does not support reading exported documents")

  /**
   * Read a given version of a document stored for a DataObject, see [[listVersions]].
   */
  def readVersion(tpe: ExportType, dataObjectId: DataObjectId, version: Long): Option[String] = throw new NotImplementedException(s"${getClass.getSimpleName} does not support reading exported documents")

  /**
   * List the versions of a document type for all DataObjects at once, see [[listVersions]].
   * This avoids a request per DataObject for remote writers.
   *
   * @param latestOnly if true, only the latest version of each DataObject is needed.
   * @return versions per DataObject, or None if not supported, then [[listVersions]] is used per DataObject.
   */
  def listAllVersions(tpe: ExportType, latestOnly: Boolean): Option[Map[DataObjectId, Seq[Long]]] = None

  def readLatest(tpe: ExportType, dataObjectId: DataObjectId): Option[String] = {
    listVersions(tpe, dataObjectId).maxOption.flatMap(readVersion(tpe, dataObjectId, _))
  }

  /**
   * Write a given version of a document, e.g. one read from another [[ExportWriter]] with [[readVersion]].
   */
  def writeVersion(tpe: ExportType, document: String, dataObjectId: DataObjectId, version: Long): Unit = tpe match {
    case ExportType.Schema => writeSchema(document, dataObjectId, version)
    case ExportType.Stats => writeStats(document, dataObjectId, version)
    case ExportType.Lineage => writeLineage(document, dataObjectId, version)
    case _ => throw new IllegalArgumentException(s"$tpe is not a document type exported per DataObject")
  }

  /**
   * False if the writer keeps only the latest version of a document, overwriting older versions.
   */
  def keepsHistory: Boolean = true

  /**
   * Rebuild the search index of the target over the configuration, the descriptions and the schemas, if it has one.
   *
   * @param version version of the configuration to index, or None for the latest version
   * @return false if the target has no search index
   */
  def updateSearchIndex(version: Option[String]): Boolean = false

  /**
   * Rebuild the column lineage index of the target over the lineage documents of all DataObjects, if it has one.
   *
   * @return false if the target has no column lineage index
   */
  def updateLineageIndex(): Boolean = false
}

/**
 * Types of exported documents.
 */
object ExportType extends Enumeration {
  type ExportType = Value
  /** resolved configuration, see [[ExportWriter.writeConfig]] */
  val ConfigJson: Value = Value("configJson")
  /** markdown description files, see [[ExportWriter.writeFile]] */
  val Description: Value = Value("description")
  /** schema of a DataObject */
  val Schema: Value = Value("schema")
  /** statistics of a DataObject */
  val Stats: Value = Value("stats")
  /** column lineage of a DataObject */
  val Lineage: Value = Value("lineage")
  /** state of workflow runs, see [[StateSyncEndpoint]] */
  val State: Value = Value("state")

  /** the types exported per DataObject, which are versioned */
  val dataObjectTypes: Seq[ExportType] = Seq(Schema, Stats, Lineage)
}


object ExportWriter {

  /**
   * Filename of the exported configuration if it is written to a directory.
   */
  val configFilename = "exportedConfig.json"

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

  /**
   * Format a schema as Json document.
   *
   * @param transformSchemaJson optional transformation of the schema Json, e.g. to merge column descriptions,
   *                            see [[ColumnDescriptionParser.mergeIntoSchemaJson]].
   */
  def formatSchema(schema: Option[GenericSchema], info: Option[String], transformSchemaJson: JArray => JArray = identity, viewQuery: Option[String] = None): String = {
    val contentJson = JObject(Seq(
      info.toSeq.map("info" -> JString(_)),
      schema.toSeq.map(s => "schema" -> transformSchemaJson(s.toJson)),
      schema.toSeq.map(s => "subFeedType" -> JString(s.subFeedType.typeSymbol.name.toString)),
      viewQuery.toSeq.map("viewQuery" -> JString(_))
    ).flatten.toIndexedSeq: _*)
    pretty(contentJson)
  }

  /**
   * Format the column level lineage of an output DataObject as Json document.
   *
   * The lineage itself is the `columnLineage` dataset facet of the OpenLineage standard, see [[ColumnLineage]].
   * It is wrapped with the ids of the DataObject and of the Action which created it, as these are the SDLB
   * objects the lineage belongs to, and with the columns whose lineage could not be traced completely. The
   * latter are needed to tell a column without a source apart from a column SDLB could not analyze.
   */
  def formatColumnLineage(actionId: ActionId, dataObjectId: DataObjectId, columnLineage: ColumnLineage): String = {
    val contentJson = JObject(
      Seq[Option[JField]](
        Some("actionId" -> JString(actionId.id)),
        Some("dataObjectId" -> JString(dataObjectId.id)),
        Some("columnLineage" -> columnLineage.toJson),
        Option.when(columnLineage.unresolvedColumns.nonEmpty)(
          "unresolvedFields" -> (JArray(columnLineage.unresolvedColumns.map(JString(_)).toList): JValue)
        )
      ).flatten.toList
    )
    pretty(contentJson)
  }

  /**
   * Format why the column lineage of an output DataObject could not be traced back completely.
   *
   * This is a text report and not Json: it is read by a developer extending the lineage extraction, and its
   * main part is the plan of the engine, which is a text tree.
   */
  def formatColumnLineageDebug(actionId: ActionId, dataObjectId: DataObjectId, debug: ColumnLineageDebug): String = {
    val header = Seq(s"Action ${actionId.id} -> DataObject ${dataObjectId.id} (engine ${debug.engine})")
    val inputs = Seq("", "Inputs:") ++ {
      if (debug.inputs.isEmpty) Seq("  none - there are no input columns the lineage could be traced back to")
      else debug.inputs.flatMap { input =>
        Seq(s"  ${input.dataObjectId.id}: ${input.columns.mkString(", ")}") ++
          Option.when(input.columnsNotInPlan.nonEmpty)(
            s"    not used by the output DataFrame: ${input.columnsNotInPlan.mkString(", ")}"
          )
      }
    }
    val unresolvedColumns = Seq("", "Unresolved columns:") ++ {
      if (debug.unresolvedColumns.isEmpty) Seq("  none")
      else debug.unresolvedColumns.flatMap { column =>
        Seq(s"  ${column.column}") ++ column.deadEnds.flatMap { deadEnd =>
          Seq(s"    ${deadEnd.path.mkString(" <- ")}") ++
            deadEnd.producedBy.map(nodeType => s"    produced by $nodeType").toSeq ++
            deadEnd.producedByNode.map(node => s"      $node").toSeq
        }
      }
    }
    val plan = if (debug.plan.isEmpty) Seq() else Seq("", "Analyzed plan:") ++ debug.plan
    (header ++ inputs ++ unresolvedColumns ++ plan).mkString(System.lineSeparator) + System.lineSeparator
  }

  /**
   * Operation listing the versions of a document type for all DataObjects of the UI backend, see
   * [[ExportWriter.listAllVersions]]. It returns a Json object with the versions per DataObject id.
   */
  def allVersionsOperation(tpe: ExportType): String = s"dataobjects/$tpe/tstamps"

  def allVersionsParams(latestOnly: Boolean): Map[String, String] = if (latestOnly) Map("latest" -> "true") else Map()

  def parseAllVersions(content: String): Map[DataObjectId, Seq[Long]] = JsonMethods.parse(content) match {
    case JObject(fields) => fields.map {
      case (id, JArray(versions)) => DataObjectId(id) -> versions.collect { case JInt(v) => v.toLong }.sorted
      case (id, _) => throw new IllegalStateException(s"Versions of DataObject $id are not a Json array")
    }.toMap
    case _ => throw new IllegalStateException("Versions of all DataObjects are not a Json object")
  }

  /**
   * The UI backend returns statistics wrapped as `{"stats": ...}`, while they are uploaded unwrapped.
   */
  def unwrapDownloadedDocument(tpe: ExportType, content: String): String = {
    if (tpe == ExportType.Stats) JsonMethods.parse(content) match {
      case JObject(List(("stats", stats))) => pretty(stats)
      case _ => content
    } else content
  }

  /**
   * The query of a view exported with its schema, see [[formatSchema]] and
   * [[io.smartdatalake.workflow.dataobject.generic.ViewDataObject.getViewQuery]].
   */
  def parseViewQuery(content: String): Option[String] = JsonMethods.parse(content) \ "viewQuery" match {
    case JString(query) => Some(query)
    case _ => None
  }

  def parseSchema(content: String): (GenericSchema, Option[String]) = {
    val json = JsonMethods.parse(content) match {
      case jObj: org.json4s.JObject => jObj
      case _ => throw new IllegalStateException("Not a valid Json object")
    }
    val schema = json \ "schema" match {
      case jsonSchema: JArray =>
        val subFeedType = json \ "subFeedType" match {
          case JString(tpe) =>
            DataFrameSubFeed.getKnownSubFeedTypes.find(_.typeSymbol.name.toString.endsWith(tpe))
              .getOrElse(throw new IllegalStateException(s"Could not find SubFeedType $tpe"))
          case _ => throw new IllegalStateException(s"Attribute 'subFeedType' not found")
        }
        GenericSchema.fromJson(jsonSchema, subFeedType)
      case _ => throw new IllegalStateException(s"Attribute 'schema' not found")
    }
    val info = json \ "info" match {
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