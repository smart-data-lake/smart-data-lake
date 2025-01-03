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

package io.smartdatalake.lab

import io.smartdatalake.lab.DataFrameBaseBuilder.DEFAULT_DATAOBJECT_ID
import io.smartdatalake.util.hdfs.PartitionValues
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, expr, lit}

abstract class DataFrameBaseBuilder[R] {
  def partitionValues: Seq[PartitionValues]
  protected def setPartitionValues(partitionValues: Seq[PartitionValues]): R
  def filters: Map[String,Column]
  protected def setFilters(filters: Map[String,Column]): R

  /**
   * Filter partitions based on a list of partitions, each with one or multiple columns and corresponding values.
   */
  def withPartitionValues(partitionValues: Seq[PartitionValues]): R = {
    assert(partitionValues.isEmpty, "partitionValues are already set, they can not be overwritten.")
    setPartitionValues(partitionValues)
  }
  /**
   * Filter partitions based on one column and multiple values for this column.
   */
  def withPartitionValues(colName: String, values: Seq[String]): R = {
    assert(partitionValues.isEmpty, "partitionValues are already set, they can not be overwritten.")
    setPartitionValues(values.map(v => PartitionValues(Map(colName -> v))))
  }
  /**
   * Filter partitions by choosing one column and one value.
   */
  def withPartitionValues(colName: String, value: String): R = {
    assert(partitionValues.isEmpty, "partitionValues are already set, they can not be overwritten.")
    setPartitionValues(Seq(PartitionValues(Map(colName -> value))))
  }

  /**
   * Spark Filter expression to be applied on all DataFrames that contain the given column.
   * @param colName column name
   * @param expr a Spark expression returning a boolean value
   */
  def withFilter(colName: String, expr: Column): R = setFilters(filters + (colName -> expr))

  /**
   * SQL Filter expression to be applied on all DataFrames that contain the given column.
   * @param colName column name
   * @param sqlExpr a Spark SQL expression returning a boolean value
   */
  def withFilter(colName: String, sqlExpr: String): R = setFilters(filters + (colName -> expr(sqlExpr)))

  /**
   * Filter DataFrame with an equals condition on a given column.
   * @param colName column name to filter on.
   * @param literal a literal to be used in equals condition on column.
   */
  def withFilterEquals(colName: String, literal: Any): R = {
    val literalExpr = literal match {
      case x: String => lit(x)
      case x: AnyRef => throw new IllegalArgumentException("Only AnyVal (Int, Long, ... ) and String supported as parameter of withFilterEquals method")
      case x => lit(x) // if type is not AnyRef, it is AnyVal; but Scala can not match against AnyVal...
    }
    setFilters(filters + (colName -> (col(colName) === literalExpr)))
  }

  /**
   * Get DataFrames using selected options.
   */
  def get: Map[String, DataFrame]

  /**
   * Get one DataFrames using selected options.
   * Only works if the last transformation return only one DataFrame, or the DataObjectId to return is given.
   */
  def getOne(dataObjectId: String): DataFrame = {
    val dfs = get
    if (dataObjectId != DEFAULT_DATAOBJECT_ID) dfs.getOrElse(dataObjectId, throw new IllegalArgumentException(s"DataObjectId $dataObjectId not found in result of Transformer. dataObjectIds returned: ${dfs.keys.mkString(", ")}"))
    else if (dfs.size == 1) dfs.head._2
    else if (dfs.contains(dataObjectId)) dfs(dataObjectId)
    else throw new IllegalArgumentException(s"getOne cannot return more than 1 DataFrame. The transformer should return only one DataFrame, or the dataObjectId to return must be given as parameter. dataObjectIds returned: ${dfs.keys.mkString(", ")}")
  }

  def getOne(): DataFrame = {
    getOne(DEFAULT_DATAOBJECT_ID)
  }
}

object DataFrameBaseBuilder {
  final val DEFAULT_DATAOBJECT_ID = "df"
}
