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
package io.smartdatalake.workflow

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataframe.GenericSchema

/**
 * A filter expression bound to a column.
 *
 * The filter is applied to a DataFrame only if `column` exists in its schema, see [[GenericSchema.columnExists]].
 * This allows an ExecutionMode to push one filter to all inputs of an Action, and filters to be propagated to
 * following Actions, applying them where they make sense - analogous to how partition values are handled.
 *
 * @param column        name of the column this filter is about. It is used to decide whether the filter is applicable
 *                      to a DataFrame, and as unique key: a SubFeed holds at most one filter per column.
 * @param expression    SQL expression returning a boolean value, evaluated by the engine of the SubFeed.
 * @param mainInputOnly if true this filter is only applied to the main input of the Action, otherwise to all inputs
 *                      which have the column.
 * @param propagate     if true this filter is passed on to the outputs of the Action, and therefore to following
 *                      Actions, restricted to the columns they have. Set this only if the filter still describes the
 *                      data written by the Action, as the filter is then assumed to be applied already.
 */
case class ColumnFilter(column: String, expression: String, mainInputOnly: Boolean = false, propagate: Boolean = false) {
  override def toString: String = {
    val flags = Seq(if (mainInputOnly) Some("mainInputOnly") else None, if (propagate) Some("propagate") else None).flatten
    s"$column: $expression" + (if (flags.nonEmpty) flags.mkString(" (", ", ", ")") else "")
  }
}

object ColumnFilter extends SmartDataLakeLogger {

  /**
   * Normalize a column name for comparison, honouring [[Environment.caseSensitive]].
   */
  private def key(colName: String): String = if (Environment.caseSensitive) colName else colName.toLowerCase

  /**
   * Merge `added` into `existing`, keeping at most one filter per column and preserving insertion order.
   * A filter for a column which already has a different filter replaces it and logs a warning.
   *
   * @param context by-name description of the location, e.g. s"($dataObjectId)". It is only evaluated if a warning
   *                is logged.
   */
  def merge(existing: Seq[ColumnFilter], added: Seq[ColumnFilter], context: => String): Seq[ColumnFilter] = {
    added.foldLeft(existing) { case (acc, filter) =>
      val idx = acc.indexWhere(f => key(f.column) == key(filter.column))
      if (idx < 0) acc :+ filter
      else if (acc(idx) == filter) acc
      else {
        logger.warn(s"$context filter on column ${acc(idx).column} is replaced:" +
          s" '${acc(idx).expression}' -> '${filter.expression}'")
        acc.updated(idx, filter)
      }
    }
  }

  /**
   * Keep only the filters which are applicable to the given schema, e.g. whose column exists.
   * This is the column analogue of [[SubFeed.filterPartitionValues]].
   */
  def filterExistingColumns(filters: Seq[ColumnFilter], schema: GenericSchema): Seq[ColumnFilter] = {
    filters.filter(f => schema.columnExists(f.column))
  }

  /**
   * true if the given filters contain more than one filter for the same column.
   */
  def hasDuplicateColumns(filters: Seq[ColumnFilter]): Boolean = {
    filters.map(f => key(f.column)).distinct.size != filters.size
  }

  def describe(filters: Seq[ColumnFilter]): String = filters.mkString(", ")
}
