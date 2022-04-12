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

package io.smartdatalake.workflow.snowflake

import com.snowflake.snowpark.{types => snowpark}
import io.smartdatalake.workflow.dataframe.snowflake.{SnowparkSchema, SnowparkSubFeed}
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import org.apache.spark.sql.{types => spark}
import org.scalatest.FunSuite

import scala.reflect.runtime.universe.typeOf

class SnowparkSchemaConverterTest extends FunSuite {

  test("converting simple schema from Spark to Snowflake") {
    val sparkSchema = SparkSchema(
      spark.StructType( Seq(
        spark.StructField("a", spark.StringType, false),
        spark.StructField("b", spark.IntegerType, true),
        spark.StructField("c", spark.DecimalType(10,3), true),
      ))
    )
    val expectedSnowparkSchema = SnowparkSchema(
      snowpark.StructType( Seq(
        snowpark.StructField("a", snowpark.StringType, false),
        snowpark.StructField("b", snowpark.IntegerType, true),
        snowpark.StructField("c", snowpark.DecimalType(10,3), true),
      ))
    )

    // convert to snowpark schema and check
    val convertedSnowparkSchema = sparkSchema.convert(typeOf[SnowparkSubFeed])
    assert(expectedSnowparkSchema == convertedSnowparkSchema)

    // convert back to spark schema and check
    val convertedSparkSchema = convertedSnowparkSchema.convert(typeOf[SparkSubFeed])
    assert(sparkSchema == convertedSparkSchema)
  }

  test("converting complex schema from Spark to Snowflake") {
    val sparkSchema = SparkSchema(
      spark.StructType( Seq(
        spark.StructField("a", spark.StringType, false),
        spark.StructField("b", spark.ArrayType(spark.IntegerType), true),
        spark.StructField("c", spark.StructType( Seq(
          spark.StructField("c1", spark.FloatType),
          spark.StructField("c2", spark.DoubleType),
        )), true),
      ))
    )
    val expectedSnowparkSchema = SnowparkSchema(
      snowpark.StructType( Seq(
        snowpark.StructField("a", snowpark.StringType, false),
        snowpark.StructField("b", snowpark.ArrayType(snowpark.IntegerType), true),
        snowpark.StructField("c", snowpark.StructType( Seq(
          snowpark.StructField("c1", snowpark.FloatType),
          snowpark.StructField("c2", snowpark.DoubleType),
        )), true),
      ))
    )

    // convert to snowpark schema and check
    val convertedSnowparkSchema = sparkSchema.convert(typeOf[SnowparkSubFeed])
    assert(expectedSnowparkSchema == convertedSnowparkSchema)

    // convert back to spark schema and check
    val convertedSparkSchema = convertedSnowparkSchema.convert(typeOf[SparkSubFeed])
    assert(sparkSchema == convertedSparkSchema)
  }
}
