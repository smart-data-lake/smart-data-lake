/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.dataframe

import io.smartdatalake.dataframe.DomainSpecificLanguage.Language

object SparkLanguageImplementation {

  private[smartdatalake] val language: Language[SparkDataFrame, SparkColumn, SparkStructType, SparkDataType] =
    new Language[SparkDataFrame, SparkColumn, SparkStructType, SparkDataType] {

      override def join(left: SparkDataFrame,
                        right: SparkDataFrame,
                        joinCols: Seq[String]): SparkDataFrame = {
        left.join(right, joinCols)
      }

      override def col(colName: String): SparkColumn = {
        org.apache.spark.sql.functions.column(colName)
      }

      override def lit(value: Any): SparkColumn = {
        org.apache.spark.sql.functions.lit(value)
      }

      override def select(dataFrame: SparkDataFrame,
                          column: SparkColumn): SparkDataFrame = {
        dataFrame.select(column)
      }

      override def filter(dataFrame: SparkDataFrame,
                          column: SparkColumn): SparkDataFrame = {
        dataFrame.filter(column)
      }

      override def and(left: SparkColumn,
                       right: SparkColumn): SparkColumn = {
        left.and(right)
      }

      override def ===(left: SparkColumn, right: SparkColumn): SparkColumn = {
        left === right
      }

      override def =!=(left: SparkColumn, right: SparkColumn): SparkColumn = {
        left =!= right
      }

      override def schema(dataFrame: SparkDataFrame): SparkStructType = {
        dataFrame.schema
      }

      override def columns(dataFrame: SparkDataFrame): Seq[String] = {
        dataFrame.columns
      }
    }
}
