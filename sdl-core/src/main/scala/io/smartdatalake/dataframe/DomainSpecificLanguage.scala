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

trait Language[DataFrame, Column, Schema, DataType] {

  // Functions
  def col(colName: String): Column

  def lit(value: Any): Column

  // operators
  def ===(left: Column, right: Column): Column

  def =!=(left: Column, right: Column): Column

  def and(left: Column, right: Column): Column

  // DataFrame operations
  def join(left: DataFrame, right: DataFrame, joinCols: Seq[String]): DataFrame

  def select(dataFrame: DataFrame, column: Column): DataFrame

  def filter(dataFrame: DataFrame, expression: Column): DataFrame

  def schema(dataFrame: DataFrame): Schema

  def columns(dataFrame: DataFrame): Seq[String]

  val implicits: DomainSpecificLanguage.type = DomainSpecificLanguage
}

object DomainSpecificLanguage {

  def col[DataFrame, Column, Schema, DataType](colName: String)
                                              (implicit L: Language[DataFrame, Column, Schema, DataType]): Column = {
    L.col(colName)
  }

  def lit[DataFrame, Column, Schema, DataType](value: Any)
                                              (implicit L: Language[DataFrame, Column, Schema, DataType]): Column = {
    L.lit(value)
  }

  implicit class RichDataFrame[DataFrame, Column, Schema, DataType](dataFrame: DataFrame)
                                                                   (implicit L: Language[DataFrame, Column, Schema, DataType]) {
    def join(other: DataFrame, joinCols: Seq[String]): DataFrame = {
      L.join(dataFrame, other, joinCols)
    }

    def select(column: Column): DataFrame = {
      L.select(dataFrame, column)
    }

    def filter(column: Column): DataFrame = {
      L.filter(dataFrame, column)
    }

    def schema: Schema = {
      L.schema(dataFrame)
    }

    def columns: Seq[String] = {
      L.columns(dataFrame)
    }

  }

  implicit class RichColumn[DataFrame, Column, Schema, DataType](column: Column)
                                                                (implicit L: Language[DataFrame, Column, Schema, DataType]) {
    def ===(other: Column): Column = {
      L.===(column, other)
    }
  }
}

