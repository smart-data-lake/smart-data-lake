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

import io.smartdatalake.workflow.dataframe.sparkconnect.{SparkConnectSchema, SparkConnectSimpleDataType, SparkConnectSubFeed}
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.funsuite.AnyFunSuite

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
}
