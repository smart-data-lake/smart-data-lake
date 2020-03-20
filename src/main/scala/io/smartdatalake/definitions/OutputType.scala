/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.definitions

/**
 * Options for HDFS output
 */
object OutputType extends Enumeration {
  type OutputType = Value
  /**
   * Output Type CSV (Text)
   */
  val Csv = Value("csv")
  /**
   * Output type Avro
   */
  val Avro = Value("avro")
  /**
   * Output type Parquet
   */
  val Parquet = Value("parquet")
  /**
   * Output type schema
   */
  val Schema = Value("schema")
  /**
   * Output type Hive
   */
  val Hive = Value("hive")
  /**
   * File with unknown format (i.e. binary)
   */
  val Raw = Value("raw")
  /**
   * Output type JSON
   */
  val Json = Value("json")
  /**
   * Output type XML
   */
  val Xml = Value("xml")
  /**
   * Output type DataFrame
   * Can be used to transport DataFrames between actions.
   */
  val DataFrame = Value("dataframe")
  /**
   * Output type JDBC
   */
  val Jdbc = Value("jdbc")

}


