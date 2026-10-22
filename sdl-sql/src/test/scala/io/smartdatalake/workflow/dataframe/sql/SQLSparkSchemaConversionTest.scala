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
package io.smartdatalake.workflow.dataframe.sql

import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf

/**
 * Tests converting schemas between the SQL and the Spark engine, which is done through their Json representation,
 * see SchemaConverter. sdl-sql has sdl-spark as test dependency only.
 */
class SQLSparkSchemaConversionTest extends AnyFunSuite {

  private def simple(sql: String) = SQLSimpleDataType(sql)

  private val sqlSchema = SQLSchema(Seq(
    SQLField("id", simple("INT"), nullable = false, comment = Some("the id")),
    SQLField("cnt", simple("BIGINT")),
    SQLField("amount", simple("DECIMAL(10, 2)")),
    SQLField("name", simple("VARCHAR(20)")),
    SQLField("ts", simple("TIMESTAMP")),
    SQLField("tags", SQLArrayDataType(simple("TEXT"))),
    SQLField("props", SQLMapDataType(simple("TEXT"), simple("DOUBLE"))),
    SQLField("address", SQLStructDataType(Seq(SQLField("city", simple("TEXT")), SQLField("zip", simple("SMALLINT")))))
  ))

  private val sparkSchema = SparkSchema(StructType(Seq(
    StructField("id", IntegerType, nullable = false).withComment("the id"),
    StructField("cnt", LongType),
    StructField("amount", DecimalType(10, 2)),
    StructField("name", StringType),
    StructField("ts", TimestampType),
    StructField("tags", ArrayType(StringType)),
    StructField("props", MapType(StringType, DoubleType)),
    StructField("address", StructType(Seq(StructField("city", StringType), StructField("zip", ShortType))))
  )))

  test("SQL schema is converted to Spark") {
    assert(sqlSchema.convert(typeOf[SparkSubFeed]) == sparkSchema)
  }

  test("Spark schema is converted to SQL") {
    val expected = sqlSchema.copy(fields = sqlSchema.fields.map(f => if (f.name == "name") f.copy(dataType = simple("TEXT")) else f))
    assert(sparkSchema.convert(typeOf[SQLSubFeed]) == expected)
  }

  test("types without Spark equivalent can not be converted") {
    val ex = intercept[IllegalStateException](SQLSchema(Seq(SQLField("x", simple("UNKNOWN")))).convert(typeOf[SparkSubFeed]))
    assert(ex.getMessage.contains("Can not convert schema from SQLSubFeed to SparkSubFeed"))
  }

  test("a SQL schema is validated against a Spark schema") {
    val schemaMin = SparkSchema(StructType(Seq(StructField("id", IntegerType), StructField("name", StringType))))
    assert(schemaMin.diffSchema(sqlSchema).isEmpty)
    val missing = schemaMin.add("zip", SparkSchema(StructType(Seq(StructField("zip", StringType)))).fields.head.dataType)
    assert(missing.diffSchema(sqlSchema).map(_.columns).contains(Seq("zip")))
  }
}
