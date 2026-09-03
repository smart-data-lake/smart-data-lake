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
package io.smartdatalake.workflow.dataframe

import scala.annotation.tailrec

/**
 * Engine independent helpers to work with [[GenericSchema]].
 *
 * This mirrors the Spark specific SparkSchemaUtil for the cases where no Spark session is available,
 * e.g. when applying column comments to a catalog from DataObjectSchemaExporter.
 */
object GenericSchemaUtil {

  /**
   * Collect the comments of all columns of a schema, including nested columns.
   *
   * Column names are returned as path, e.g. Seq("address", "street") for a column "street" nested in a
   * struct column "address". Use [[formatColumnPath]] to get the name to be used in an SQL statement.
   * Note that arrays are traversed transparently, e.g. a struct nested in an array of the column
   * "addresses" is returned as Seq("addresses", "street"), matching the SQL syntax to comment it.
   */
  def columnComments(schema: GenericSchema): Map[Seq[String], String] = columnComments(schema.fields, Seq())

  private def columnComments(fields: Seq[GenericField], parents: Seq[String]): Map[Seq[String], String] = {
    fields.foldLeft(Map[Seq[String], String]()) { case (comments, field) =>
      val path = parents :+ field.name
      val fieldComment = field.comment.map(c => Map(path -> c)).getOrElse(Map())
      comments ++ fieldComment ++ nestedComments(field.dataType, path)
    }
  }

  @tailrec
  private def nestedComments(dataType: GenericDataType, parents: Seq[String]): Map[Seq[String], String] = dataType match {
    case struct: GenericStructDataType => columnComments(struct.fields, parents)
    case array: GenericArrayDataType => nestedComments(array.elementDataType, parents)
    case _ => Map()
  }

  /**
   * Format a column path as returned by [[columnComments]] for use in an SQL statement.
   */
  def formatColumnPath(path: Seq[String]): String = path.mkString(".")
}
