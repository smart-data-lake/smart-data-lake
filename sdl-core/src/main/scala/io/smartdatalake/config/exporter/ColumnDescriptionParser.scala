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

import io.smartdatalake.config.ConfigParser
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.{JArray, JObject, JString, JValue}

/**
 * Parses column descriptions from the Markdown description files of DataObjects.
 *
 * A description is introduced by a `@column <name> <text>` tag and continues over the following lines
 * until the next `@column` tag or the next Markdown header. The column name may be a nested path, e.g.
 * `address.street`, and may contain array markers, e.g. `addresses.[].street`.
 *
 * The descriptions are merged into the schemas exported by an SDLB run with "--test dry-run-with-schema-export",
 * see [[mergeIntoSchemaJson]], where the SDLB UI reads them from, and are applied as column comments to the catalog
 * by CatalogSchemaUpdater.
 */
object ColumnDescriptionParser extends SmartDataLakeLogger {

  private val columnDescriptionRegex = """\s*@column\s+["`']?([^\s"`']+)["`']?\s+(.*)""".r.anchored

  /**
   * Read the column descriptions of all DataObjects having a Markdown description file in
   * `<descriptionPath>/dataObjects`.
   *
   * If the directory does not exist, a warning is logged and no descriptions are returned.
   *
   * @return the column descriptions per DataObject id, keyed by the column name as written in the file.
   */
  def parse(descriptionPath: String)(implicit hadoopConf: Configuration): Map[DataObjectId, Map[String, String]] = {
    val hadoopPath = new Path(descriptionPath, ConfigParser.CONFIG_SECTION_DATAOBJECTS)
    implicit val filesystem: FileSystem = Environment.fileSystemFactory.getFileSystem(hadoopPath, hadoopConf)
    if (!filesystem.exists(hadoopPath)) {
      logger.warn(s"DataObject description directory $hadoopPath not found, no column descriptions are used")
      return Map()
    }
    logger.info(s"Searching DataObject description files in $hadoopPath")
    RemoteIteratorWrapper(filesystem.listStatusIterator(hadoopPath)).filterNot(_.isDirectory)
      .filter(_.getPath.getName.endsWith(".md")).toSeq // only Markdown files
      .map { p =>
        val dataObjectId = DataObjectId(p.getPath.getName.split('.').head)
        (dataObjectId, parseContent(HdfsUtil.readHadoopFile(p.getPath)))
      }
      .filter(_._2.nonEmpty)
      .toMap
  }

  /**
   * Parse the column descriptions out of the content of one Markdown description file.
   */
  def parseContent(content: String): Map[String, String] = {
    content.linesIterator.foldLeft((Seq[(String, String)](), false)) {
      // if new column description tag, add new column description
      case ((descriptions, _), columnDescriptionRegex(name, description)) =>
        (descriptions :+ (name, description.trim), true)
      // if new header tag and column description open, close column description
      case ((descriptions, true), line) if line.startsWith("#") =>
        (descriptions, false)
      // if last column description open, add line to last column description text
      case ((descriptions, true), line) =>
        val (lastName, lastDesc) = descriptions.last
        (descriptions.init :+ (lastName, (lastDesc + System.lineSeparator() + line.trim).trim), true)
      // if last column description closed, ignore line
      case ((descriptions, false), _) =>
        (descriptions, false)
    }._1.filter(_._2.nonEmpty).toMap
  }

  /**
   * Convert a column name as written in a description file into the column path used to address the
   * column in an SQL statement, e.g. "addresses.[].street" becomes Seq("addresses", "street").
   * Array markers are dropped as arrays are traversed transparently when commenting a nested column.
   */
  def toColumnPath(columnName: String): Seq[String] = columnName.split('.').filter(_ != "[]").toIndexedSeq

  /**
   * Merge column descriptions into the Json representation of a schema, see `GenericSchema.toJson`.
   * A description overrides the comment already defined in the schema.
   *
   * The column name is resolved segment by segment against the data type it is applied to:
   * - a struct is entered by the name of one of its fields, compared case-insensitive unless
   *   [[Environment.caseSensitive]] is set.
   * - an array is entered by `[]`, or transparently, e.g. `addresses.street` is the same as `addresses.[].street`.
   * - a map is entered by `key` or `value`.
   *
   * A description of an array element or of a map key or value itself, e.g. `addresses.[]` or `attributes.key`,
   * has no field to be set on. It is set as attribute `elementComment`, `keyComment` or `valueComment` of the
   * array or map data type.
   *
   * @return the merged schema Json, and the names of the columns whose description could not be merged as they
   *         are not found in the schema.
   */
  def mergeIntoSchemaJson(schemaJson: JArray, descriptions: Map[String, String]): (JArray, Seq[String]) = {
    descriptions.toSeq.sortBy(_._1).foldLeft((schemaJson, Seq[String]())) {
      case ((json, unresolved), (name, description)) =>
        setInFields(json.arr, name.split('.').toList, description) match {
          case Some(fields) => (JArray(fields), unresolved)
          case None => (json, unresolved :+ name)
        }
    }
  }

  private def setInFields(fields: List[JValue], path: List[String], description: String): Option[List[JValue]] = {
    path match {
      case name :: remaining =>
        val idx = fields.indexWhere(field => (field \ "name") match {
          case JString(fieldName) => if (Environment.caseSensitive) fieldName == name else fieldName.equalsIgnoreCase(name)
          case _ => false
        })
        if (idx < 0) None
        else {
          val field = fields(idx).asInstanceOf[JObject]
          val newField =
            if (remaining.isEmpty) Some(withAttribute(field, "comment", JString(description)))
            else setInDataType(field \ "dataType", remaining, description).map(withAttribute(field, "dataType", _))
          newField.map(fields.updated(idx, _))
        }
      case Nil => None
    }
  }

  private def setInDataType(dataType: JValue, path: List[String], description: String): Option[JValue] = {
    (dataType, dataType \ "dataType", path) match {
      case (struct: JObject, JString("struct"), _) =>
        (struct \ "fields") match {
          case JArray(fields) => setInFields(fields, path, description).map(f => withAttribute(struct, "fields", JArray(f)))
          case _ => None
        }
      case (array: JObject, JString("array"), "[]" :: Nil) =>
        Some(withAttribute(array, "elementComment", JString(description)))
      case (array: JObject, JString("array"), "[]" :: remaining) =>
        setInDataType(array \ "elementType", remaining, description).map(withAttribute(array, "elementType", _))
      case (array: JObject, JString("array"), _) => // arrays are traversed transparently
        setInDataType(array \ "elementType", path, description).map(withAttribute(array, "elementType", _))
      case (map: JObject, JString("map"), (keyOrValue @ ("key" | "value")) :: remaining) =>
        if (remaining.isEmpty) Some(withAttribute(map, s"${keyOrValue}Comment", JString(description)))
        else setInDataType(map \ s"${keyOrValue}Type", remaining, description).map(withAttribute(map, s"${keyOrValue}Type", _))
      case _ => None
    }
  }

  /**
   * Set an attribute of a Json object, keeping the position of an existing attribute.
   */
  private def withAttribute(obj: JObject, name: String, value: JValue): JObject = {
    if (obj.obj.exists(_._1 == name)) JObject(obj.obj.map { case (n, v) => if (n == name) (n, value) else (n, v) })
    else JObject(obj.obj :+ (name -> value))
  }
}
