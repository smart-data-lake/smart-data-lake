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
package io.smartdatalake.meta.configexporter

import io.smartdatalake.config.ConfigParser
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Parses column descriptions from the Markdown description files of DataObjects.
 *
 * A description is introduced by a `@column <name> <text>` tag and continues over the following lines
 * until the next `@column` tag or the next Markdown header. The column name may be a nested path, e.g.
 * `address.street`, and may contain array markers, e.g. `addresses.[].street`.
 *
 * The descriptions are used by [[ConfigJsonExporter]] to document the columns in the SDLB UI, and by
 * [[DataObjectSchemaExporter]] to apply them as column comments to the catalog.
 */
object ColumnDescriptionParser extends SmartDataLakeLogger {

  private val columnDescriptionRegex = """\s*@column\s+["`']?([^\s"`']+)["`']?\s+(.*)""".r.anchored

  /**
   * Read the column descriptions of all DataObjects having a Markdown description file in
   * `<descriptionPath>/dataObjects`.
   *
   * @return the column descriptions per DataObject id, keyed by the column name as written in the file.
   */
  def parse(descriptionPath: String)(implicit hadoopConf: Configuration): Map[DataObjectId, Map[String, String]] = {
    val hadoopPath = new Path(descriptionPath, ConfigParser.CONFIG_SECTION_DATAOBJECTS)
    implicit val filesystem: FileSystem = Environment.fileSystemFactory.getFileSystem(hadoopPath, hadoopConf)
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
}
