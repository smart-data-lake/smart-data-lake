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

import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import java.time.{Duration, LocalDate}

/**
 * Tests for composing SQL text of columns in Scala. They need no Python environment.
 */
class SQLColumnTest extends AnyFunSuite {

  import SQLSubFeed._

  test("column references are quoted and can be qualified") {
    assert(col("a").expr == "\"a\"")
    assert(col("a").getName.contains("a"))
    assert(col("t.a").expr == "\"t\".\"a\"")
    assert(col("t.a").getName.contains("a"))
    assert(col("`my.col`").expr == "\"my.col\"")
    assert(col("`my``col`").expr == "\"my`col\"")
    assert(col("say \"hi\"").expr == "\"say \"\"hi\"\"\"")
    assert(col("*").expr == "*")
    assert(col("t.*").expr == "\"t\".*")
  }

  test("literals") {
    assert(lit("it's").expr == "'it''s'")
    assert(lit(1).expr == "1")
    assert(lit(-1L).expr == "-1")
    assert(lit(1.5).expr == "1.5")
    assert(lit(Double.NaN).expr == "CAST('NaN' AS DOUBLE)")
    assert(lit(BigDecimal("1.10")).expr == "1.10")
    assert(lit(true).expr == "TRUE")
    assert(lit(null).expr == "NULL")
    assert(lit(None).expr == "NULL")
    assert(lit(Some("x")).expr == "'x'")
    assert(lit(LocalDate.of(2024, 1, 31)).expr == "CAST('2024-01-31' AS DATE)")
    assert(lit(Timestamp.valueOf("2024-01-31 10:00:00")).expr == "CAST('2024-01-31 10:00:00.0' AS TIMESTAMP)")
    intercept[IllegalArgumentException](lit(new Object))
  }

  test("operands are put into parentheses only if needed") {
    assert(((col("a") + lit(1)) * lit(2)).expr == "(\"a\" + 1) * 2")
    assert((col("a") + lit(-1)).expr == "\"a\" + (-1)")
    assert((col("a") > lit(1) and col("b").isNull).expr == "(\"a\" > 1) AND (\"b\" IS NULL)")
    assert(not(col("a") === col("b")).expr == "NOT (\"a\" = \"b\")")
    assert(col("a").isin(1, "x").expr == "\"a\" IN (1, 'x')")
    assert(col("a").isin().expr == "FALSE")
    assert((col("a") <=> lit(null)).expr == "\"a\" IS NOT DISTINCT FROM NULL")
  }

  test("names and aliases") {
    // an alias is not part of the expression when used as operand
    val aliased = (col("a") + lit(1)).as("x")
    assert(aliased.getName.contains("x"))
    assert(aliased.projectionSql == "\"a\" + 1 AS \"x\"")
    assert((aliased * lit(2)).expr == "(\"a\" + 1) * 2")
    // a cast keeps the name of the column, as in Spark
    val casted = col("a").cast(stringType)
    assert(casted.getName.contains("a"))
    assert(casted.projectionSql == "CAST(\"a\" AS TEXT) AS \"a\"")
    // a plain reference needs no alias
    assert(col("t.a").projectionSql == "\"t\".\"a\"")
    // expressions have no name
    assert((col("a") + lit(1)).getName.isEmpty)
    assert(col("s")("f").expr == "\"s\".\"f\"")
    assert(col("s")(0).expr == "\"s\"[0]")
  }

  test("functions") {
    assert(when(col("a") === lit(1), lit("one")).when(col("a") === lit(2), lit("two")).otherwise(lit("many")).expr ==
      "CASE WHEN \"a\" = 1 THEN 'one' WHEN \"a\" = 2 THEN 'two' ELSE 'many' END")
    assert(when(col("a") === lit(1), lit("one")).exprSql == "CASE WHEN \"a\" = 1 THEN 'one' END")
    assert(countDistinct(col("a")).expr == "COUNT(DISTINCT \"a\")")
    assert(coalesce(col("a"), lit(0)).expr == "COALESCE(\"a\", 0)")
    assert(substring(col("a"), 1, 2).expr == "SUBSTRING(\"a\", 1, 2)")
    assert(struct(col("a"), (col("b") + lit(1)).as("c")).expr == "STRUCT(\"a\" AS \"a\", \"b\" + 1 AS \"c\")")
    assert(window(() => row_number, Seq(col("a")), col("b").desc).expr == "ROW_NUMBER() OVER (PARTITION BY \"a\" ORDER BY \"b\" DESC)")
    assert(transform(col("a"), x => x + lit(1)).expr == "TRANSFORM(\"a\", __sdlb_x -> __sdlb_x + 1)")
    assert(timestampAdd(col("t"), Duration.ofMillis(1500)).expr == "\"t\" + INTERVAL '1.5' SECOND")
    intercept[NotImplementedError](hash(col("a")))
  }

  test("data types") {
    assert(createSimpleDataType("string") == SQLSimpleDataType("TEXT"))
    assert(createSimpleDataType("decimal(10,2)") == SQLSimpleDataType("DECIMAL(10, 2)"))
    assert(createSimpleDataType("decimal(10,2)").getDecimalSpec.contains((10, 2)))
    assert(createSimpleDataType("varchar(20)") == SQLSimpleDataType("VARCHAR(20)"))
    assert(createSimpleDataType(" Decimal( 10 ,2 ) ") == SQLSimpleDataType("DECIMAL(10, 2)"))
    assert(createSimpleDataType("bigint").isNumeric)
    assert(!createSimpleDataType("text").isNumeric)
    assert(createSimpleDataType("int").typeName == "int")
    val struct = structType(Seq(field("x", createSimpleDataType("int"), nullable = true), field("y", arrayType(stringType), nullable = true)))
    assert(struct.sql == "STRUCT<\"x\" INT, \"y\" ARRAY<TEXT>>")
    assert(mapType(stringType, createSimpleDataType("int")).sql == "MAP<TEXT, INT>")
  }

  test("wider data types") {
    def wider(l: String, r: String) = widerSimpleType(SQLSimpleDataType(l), SQLSimpleDataType(r)).map(_.sql)
    assert(wider("INT", "INT").contains("INT"))
    assert(wider("INT", "BIGINT").contains("BIGINT"))
    assert(wider("SMALLINT", "TINYINT").contains("SMALLINT"))
    assert(wider("INT", "DECIMAL(5, 2)").contains("DECIMAL(12, 2)"))
    assert(wider("DECIMAL(10, 2)", "DECIMAL(5, 4)").contains("DECIMAL(12, 4)"))
    assert(wider("DECIMAL(38, 10)", "DECIMAL(38, 20)").contains("DECIMAL(38, 20)"))
    assert(wider("INT", "DOUBLE").contains("DOUBLE"))
    assert(wider("FLOAT", "DECIMAL(10, 2)").contains("DOUBLE"))
    assert(wider("VARCHAR(10)", "VARCHAR(20)").contains("VARCHAR(20)"))
    assert(wider("VARCHAR(10)", "TEXT").contains("TEXT"))
    assert(wider("CHAR(10)", "VARCHAR(5)").contains("TEXT"))
    assert(wider("DATE", "TIMESTAMP").contains("TIMESTAMP"))
    assert(wider("INT", "TEXT").isEmpty)
    assert(wider("BOOLEAN", "INT").isEmpty)
  }

  test("schema") {
    val schema = SQLSchema(Seq(SQLField("A", SQLSimpleDataType("INT")), SQLField("b", SQLSimpleDataType("TEXT"), nullable = false)))
    assert(schema.columns == Seq("A", "b"))
    assert(schema.sql == "\"A\" INT, \"b\" TEXT NOT NULL")
    assert(schema.diffSchema(schema.toLowerCase).isEmpty)
    assert(schema.diffSchema(schema.remove("b")).map(_.columns).contains(Seq("b")))
    assert(schema.add("c", stringType).columns == Seq("A", "b", "c"))
  }
}
