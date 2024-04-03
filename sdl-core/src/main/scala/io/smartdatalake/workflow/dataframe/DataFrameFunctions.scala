/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.ActionPipelineContext

import scala.reflect.runtime.universe

/**
 * A trait with functions for working with GenericDataFrames, which are not tied to a DataFrame, Column or Schema.
 * This is the generic counterpart for Spark package org.apache.spark.sql.functions
 */
trait DataFrameFunctions {
  protected def subFeedType: universe.Type

  def col(colName: String): GenericColumn
  def lit(value: Any): GenericColumn
  def min(column: GenericColumn): GenericColumn
  def max(column: GenericColumn): GenericColumn
  def size(column: GenericColumn): GenericColumn
  def explode(column: GenericColumn): GenericColumn
  /**
   * Construct array from given columns and removing null values (Snowpark API)
   */
  def array_construct_compact(columns: GenericColumn*): GenericColumn
  def array(columns: GenericColumn*): GenericColumn
  def struct(columns: GenericColumn*): GenericColumn
  def expr(sqlExpr: String): GenericColumn
  def not(column: GenericColumn): GenericColumn
  def count(column: GenericColumn): GenericColumn
  def coalesce(columns: GenericColumn*): GenericColumn
  def when(condition: GenericColumn, value: GenericColumn): GenericColumn
  def stringType: GenericDataType
  def arrayType(dataType: GenericDataType): GenericDataType
  def structType(colTypes: Map[String,GenericDataType]): GenericDataType
  def concat(exprs: GenericColumn*): GenericColumn
  def regexp_extract(e: GenericColumn, regexp: String, groupIdx: Int): GenericColumn
  def raise_error(column: GenericColumn): GenericColumn
  /**
   * Get a DataFrame with the result of the given sql statement.
   * @param dataObjectId Snowpark implementation needs to get the Snowpark-Session from the DataObject. This should not be used otherwise.
   */
  def sql(query: String, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame

  def window(aggFunction: () => GenericColumn, partitionBy: Seq[GenericColumn], orderBy: GenericColumn): GenericColumn

  def row_number: GenericColumn

  def transform(column: GenericColumn, func: GenericColumn => GenericColumn): GenericColumn
  def transform_keys(column: GenericColumn, func: (GenericColumn,GenericColumn) => GenericColumn): GenericColumn
  def transform_values(column: GenericColumn, func: (GenericColumn,GenericColumn) => GenericColumn): GenericColumn
}
