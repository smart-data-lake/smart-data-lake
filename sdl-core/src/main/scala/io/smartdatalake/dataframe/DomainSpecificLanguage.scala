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

object DomainSpecificLanguage {

  trait Language[DataFrame, Column, Schema, DataType] {
    def col(colName: String): Column

    def lit(value: Any): Column

    def ===(left: Column, right: Column): Column

    def =!=(left: Column, right: Column): Column

    def join(left: DataFrame, right: DataFrame, joinCols: Seq[String]): DataFrame

    def select(dataFrame: DataFrame, column: Column): DataFrame

    def filter(dataFrame: DataFrame, expression: Column): DataFrame

    def and(left: Column, right: Column): Column

    def schema(dataFrame: DataFrame): Schema

    def columns(dataFrame: DataFrame): Seq[String]


    implicit class RichDataFrame[RichDataFrame, RichColumn, RichSchema, RichDataType](dataFrame: RichDataFrame)
                                                           (implicit L: Language[RichDataFrame, RichColumn, RichSchema, RichDataType]) {
      def join(other: RichDataFrame, joinCols: Seq[String]): RichDataFrame = {
        L.join(dataFrame, other, joinCols)
      }

      def select(column: RichColumn): RichDataFrame = {
        L.select(dataFrame, column)
      }

      def filter(column: RichColumn): RichDataFrame = {
        L.filter(dataFrame, column)
      }

      def schema: RichSchema = {
        L.schema(dataFrame)
      }

      def columns: Seq[String] = {
        L.columns(dataFrame)
      }

    }

    implicit class RichColumn[RichDataFrame, RichColumn, RichSchema, RichDataType](column: RichColumn)
                                                        (implicit L: Language[RichDataFrame, RichColumn, RichSchema, RichDataType]) {
      def ===(other: RichColumn): RichColumn = {
        L.===(column, other)
      }
    }

  }

}

