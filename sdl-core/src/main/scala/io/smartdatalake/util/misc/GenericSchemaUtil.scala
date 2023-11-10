/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.misc

import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.dataframe.GenericSchema
import org.slf4j.LoggerFactory

object GenericSchemaUtil {

  def columnExists(schema: GenericSchema, colName: String): Boolean = {
    if (Environment.caseSensitive) schema.columns.contains(colName)
    else schema.columns.contains(colName.toLowerCase)
  }

  def filterColumns(schema: GenericSchema, columnsFilterList: Seq[String], includeColumns: Boolean = true): Seq[String] = {
    val columns = schema.columns
    val filteredColumns =
      if (Environment.caseSensitive) filterColumnsCaseSensitive(columns, columnsFilterList, includeColumns)
      else filterColumnsCaseInsensitive(columns, columnsFilterList, includeColumns)

    filteredColumns
  }

  private def filterColumnsCaseInsensitive(columns: Seq[String], columnsFilterList: Seq[String], includeColumns: Boolean): Seq[String] = {
    val lowerCaseSchemaColumns = columns.map(_.toLowerCase()).toSet
    val nonExistingColumns = columnsFilterList.filter(colName => !lowerCaseSchemaColumns.contains(colName.toLowerCase()))
    if (nonExistingColumns.nonEmpty) {
      logNonExistingColumns(nonExistingColumns, columns)
    }
    val lowerCaseColumns = columnsFilterList.map(_.toLowerCase()).toSet
    if (includeColumns) columns.filter(colName => lowerCaseColumns.contains(colName.toLowerCase()))
    else columns.filter(colName => !lowerCaseColumns.contains(colName.toLowerCase()))
  }

  private def filterColumnsCaseSensitive(columns: Seq[String], columnsFilterList: Seq[String], includeColumns: Boolean): Seq[String] = {
    val schemaColumnsSet = columns.toSet
    val columnSet = columnsFilterList.toSet
    val nonExistingColumns = columnSet -- schemaColumnsSet
    if (nonExistingColumns.nonEmpty) {
      logNonExistingColumns(nonExistingColumns, columns)
    }
    if (includeColumns) (schemaColumnsSet & columnSet).toSeq
    else (schemaColumnsSet -- columnSet).toSeq
  }

  private def logNonExistingColumns(nonExistingColumns: Iterable[String], existingColumns: Iterable[String]): Unit = {
    LoggerFactory.getLogger(getClass.getName).warn(s"The columns [${nonExistingColumns.mkString(", ")}] do not exist in dataframe. " +
      s"Available columns are [${existingColumns.mkString(", ")}].")
  }


}
