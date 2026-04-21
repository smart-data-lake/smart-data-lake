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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.ActionPipelineContext

import scala.reflect.runtime.universe

/**
 * A trait with functions for working with GenericDataFrames, which are not tied to a DataFrame, Column or Schema.
 * This is the generic counterpart for Spark package org.apache.spark.sql.functions
 */
trait DataFrameFunctions {
  protected def subFeedType: universe.Type

  // Attention: Don't name this method getSubFeedType, Scala will otherwise compile it as a property and StatusInfoServer will try to serialize it and get an error "Direct self-reference leading to cycle..."
  def requestSubFeedType(): universe.Type = subFeedType

  def col(colName: String): GenericColumn
  def lit(value: Any): GenericColumn
  def min(column: GenericColumn): GenericColumn
  def max(column: GenericColumn): GenericColumn
  def first(column: GenericColumn): GenericColumn
  def size(column: GenericColumn): GenericColumn
  def explode(column: GenericColumn): GenericColumn
  def abs(column: GenericColumn): GenericColumn
  def least(columns: GenericColumn*): GenericColumn
  def greatest(columns: GenericColumn*): GenericColumn
  def substring(column: GenericColumn, pos: Int, len: Int): GenericColumn

  /**
   * Construct array from given columns and removing null values
   */
  def array_construct_compact(columns: GenericColumn*): GenericColumn
  def array(columns: GenericColumn*): GenericColumn
  def struct(columns: GenericColumn*): GenericColumn
  def expr(sqlExpr: String): GenericColumn
  def not(column: GenericColumn): GenericColumn
  def count(column: GenericColumn): GenericColumn
  def countDistinct(column: GenericColumn): GenericColumn
  def approxCountDistinct(column: GenericColumn, rsd: Option[Double] = None): GenericColumn
  def coalesce(columns: GenericColumn*): GenericColumn

  def when(condition: GenericColumn, value: GenericColumn): GenericColumn with GenericWhen
  def stringType: GenericDataType

  def arrayType(dataType: GenericDataType): GenericDataType with GenericArrayDataType

  def structType(colTypes: Map[String, GenericDataType]): GenericDataType with GenericStructDataType

  def structType(fields: Seq[GenericField]): GenericDataType with GenericStructDataType

  def mapType(keyType: GenericDataType, valueType: GenericDataType): GenericDataType with GenericMapDataType

  def field(name: String, dataType: GenericDataType, nullable: Boolean): GenericField
  def concat(exprs: GenericColumn*): GenericColumn
  def regexp_extract(e: GenericColumn, regexp: String, groupIdx: Int): GenericColumn
  def raise_error(column: GenericColumn): GenericColumn

  def from_json(column: GenericColumn, dataType: GenericDataType): GenericColumn

  def hash(column: GenericColumn): GenericColumn

  /**
   * Create a column expression to compare a list of columns between rows.
   * If useHash is true, the expression will use a hash function to reduce the size of the value to compare, otherwise the columns are compared as is, normally as struct of the columns.
   * The default implementation below can be overridden by implementations if needed, e.g. because they dont support struct or hash functions, but the default implementation should work for most cases.
   */
  def colsComparisionExpr(cols: Seq[GenericColumn], useHash: Boolean = false): GenericColumn = {
    assert(cols.forall(_.getName.nonEmpty), "All columns must have a name for colsComparisionExpr, otherwise the generated expression is not deterministic. Please check that all columns used for comparison are named.")
    if (useHash) hash(struct(cols.sortBy(_.getName.get):_*))
    else struct(cols.sortBy(_.getName.get):_*)
  }

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

  def rowFromSeq(values: Seq[Any]): GenericRow

  def schemaEvolutionUdf(srcType: GenericDataType, tgtType: GenericDataType): GenericUnaryUdf

}

trait GenericWhen {
  def when(condition: GenericColumn, value: GenericColumn): GenericColumn with GenericWhen

  def otherwise(value: GenericColumn): GenericColumn
}