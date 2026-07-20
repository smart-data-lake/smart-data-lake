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
package io.smartdatalake.workflow.sparkconnect

import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.workflow.dataframe.LazyGenericSchema
import io.smartdatalake.workflow.dataframe.sparkconnect.{SparkConnectSchema, SparkConnectSchemaProvider, SparkConnectSimpleDataType, SparkConnectSubFeed}
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

/**
 * Tests for client-local schema operations. These do not need a running Spark Connect server.
 */
class SparkConnectSchemaTest extends AnyFunSuite {

  test("create schema from DDL") {
    val schema = SparkConnectSubFeed.createSchemaFromDdl("id bigint, value string").asInstanceOf[SparkConnectSchema]
    assert(schema.columns == Seq("id", "value"))
    assert(schema.getDataType("id").inner == LongType)
  }

  test("add and remove columns") {
    val schema = SparkConnectSubFeed.createSchemaFromDdl("id bigint").asInstanceOf[SparkConnectSchema]
    val extended = schema.add("value", SparkConnectSimpleDataType(StringType))
    assert(extended.columns == Seq("id", "value"))
    assert(extended.remove("id").columns == Seq("value"))
  }

  test("diffSchema returns missing columns") {
    val schema = SparkConnectSubFeed.createSchemaFromDdl("id bigint, value string")
    val schemaSubset = SparkConnectSubFeed.createSchemaFromDdl("id bigint")
    assert(schema.diffSchema(schemaSubset).map(_.columns).contains(Seq("value")))
    assert(schemaSubset.diffSchema(schema).isEmpty)
  }

  test("SparkConnectSchemaProvider supports only DDL based providers") {
    assert(SparkConnectSchemaProvider.supports("ddl#id bigint"))
    assert(SparkConnectSchemaProvider.supports("ddlfile#/path/to/schema.ddl"))
    // provider types requiring classic Spark / additional libraries are not supported by the Spark Connect client
    assert(!SparkConnectSchemaProvider.supports("xsdfile#/path/to/schema.xsd"))
    assert(!SparkConnectSchemaProvider.supports("jsonschemafile#/path/to/schema.json"))
    assert(!SparkConnectSchemaProvider.supports("avroschemafile#/path/to/schema.avsc"))
    assert(!SparkConnectSchemaProvider.supports("caseclass#com.example.MyClass"))
  }

  test("SchemaUtil discovers SparkConnectSchemaProvider and parses DDL") {
    val schema = SchemaUtil.readSchemaFromConfigValue("ddl#id bigint, value string")
    assert(schema.isInstanceOf[SparkConnectSchema])
    assert(schema.columns == Seq("id", "value"))
    assert(schema.asInstanceOf[SparkConnectSchema].getDataType("id").inner == LongType)
  }

  test("SchemaUtil parses DDL from file via SparkConnectSchemaProvider") {
    val ddlFile = Files.createTempFile("schema", ".ddl")
    Files.write(ddlFile, "id bigint, value string".getBytes("UTF-8"))
    val schema = SchemaUtil.readSchemaFromConfigValue(s"ddlfile#${ddlFile.toUri}")
    assert(schema.isInstanceOf[SparkConnectSchema])
    assert(schema.columns == Seq("id", "value"))
  }

  test("SchemaUtil falls back to LazyGenericSchema for provider types unsupported by Spark Connect") {
    val schema = SchemaUtil.readSchemaFromConfigValue("xsdfile#/path/to/schema.xsd")
    assert(schema.isInstanceOf[LazyGenericSchema])
  }
}
