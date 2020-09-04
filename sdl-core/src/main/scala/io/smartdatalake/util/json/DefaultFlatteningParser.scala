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
package io.smartdatalake.util.json

import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{udf, lit, when}

private[smartdatalake] class DefaultFlatteningParser {

  def parse(nestedJsonDf: DataFrame): DataFrame = {
    val flattenedStructs = nestedJsonDf.select(flattenSchema(nestedJsonDf.schema):_*)
    val flattenedArray = flattenArrays(flattenedStructs)
    flattenedArray
  }

  private def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)
      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }

  private def arrayCols(schema: StructType): Array[StructField] = {
    schema.fields.filter(f => {
      f.dataType match {
        case ar: ArrayType => true
        case _ => false
      }
    })
  }

  private val mkString = udf((a: Seq[Double]) => a.mkString(", "))

  private def flattenArrays(df: DataFrame): DataFrame = {
    var flattened = df
    val colsWithArrays = arrayCols(df.schema)
    colsWithArrays.foreach( x => {
      val name = x.name
      flattened = flattened.withColumn(x.name, when(flattened(name).isNull, lit("")).otherwise(mkString(flattened(name))))
    })
    flattened
  }
}
