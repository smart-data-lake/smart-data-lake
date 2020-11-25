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
package io.smartdatalake.workflow.dataobject

import java.nio.file.Files

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil._
import io.smartdatalake.util.misc.{DataFrameUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.{ActionPipelineContext, SchemaViolationException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.mutable._

class HiveTableSchemaViolationTest extends FunSuite with Matchers with BeforeAndAfter with SmartDataLakeLogger {

  protected implicit val session: SparkSession = sessionHiveCatalog
  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val actionPipelineContext : ActionPipelineContext = ActionPipelineContext("testFeed", "testApp", 1, 1, instanceRegistry, None, SmartDataLakeBuilderConfig())

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  before {
    instanceRegistry.clear()
  }

  test("Read: SchemaMin equals Schema is valid.") {
    val schemaMin: StructType = StructType(StructField("id", StringType, nullable = true) :: StructField("value", StringType, nullable = true) :: Nil)
    val sourceDo = createHiveTable(
      schemaMin = Some(schemaMin),
      tableName = "source_table",
      dirPath = tempPath,
      df = dfNonUniqueWithNull,
      primaryKeyColumns = Some(Seq("id"))
    )

    noException should be thrownBy sourceDo.getDataFrame()
  }

  test("Read: SchemaMin equals Schema is valid (ignoring nullable)") {
    val schemaMin: StructType = StructType(StructField("id", StringType, nullable = false) :: StructField("value", StringType, nullable = true) :: Nil)
    val sourceDo = createHiveTable(
      schemaMin = Some(schemaMin),
      tableName = "source_table",
      dirPath = tempPath,
      df = dfNonUniqueWithNull,
      primaryKeyColumns = Some(Seq("id"))
    )

    noException should be thrownBy sourceDo.getDataFrame()
  }

  test("Read: SchemaMin is valid subset of Schema") {
    val schemaMin: StructType = StructType(StructField("id", StringType, nullable = false) :: Nil)
    val sourceDo = createHiveTable(
      schemaMin = Some(schemaMin),
      tableName = "source_table",
      dirPath = tempPath,
      df = dfNonUniqueWithNull,
      primaryKeyColumns = Some(Seq("id"))
    )

    noException should be thrownBy sourceDo.getDataFrame()
  }

  test("Read: Invalid schema - missing column") {
    val schemaMin: StructType = StructType(StructField("id",
      StringType, nullable = false) ::
      StructField("value", StringType, nullable = true) ::
      StructField("value2", StringType, nullable = true) ::
      Nil)
    val sourceDo = createHiveTable(
      schemaMin = Some(schemaMin),
      tableName = "source_table",
      dirPath = tempPath,
      df = dfNonUniqueWithNull,
      primaryKeyColumns = Some(Seq("id"))
    )

    val thrown = the [SchemaViolationException] thrownBy sourceDo.getDataFrame()
    println(thrown.getMessage)
  }

  test("Read: Invalid schema - wrong datatype") {
    val schemaMin: StructType = StructType(StructField("id",
      StringType, nullable = false) ::
      StructField("value", IntegerType, nullable = true) ::
      Nil)
    val sourceDo = createHiveTable(
      schemaMin = Some(schemaMin),
      tableName = "source_table",
      dirPath = tempPath,
      df = dfNonUniqueWithNull,
      primaryKeyColumns = Some(Seq("id"))
    )

    val thrown = the [SchemaViolationException] thrownBy sourceDo.getDataFrame()
    println(thrown.getMessage)
  }

  test("Write: SchemaMin equals Schema is valid.") {
    val schemaMin: StructType = StructType(StructField("id", StringType, nullable = true) :: StructField("value", StringType, nullable = true) :: Nil)
    val schema = schemaMin
    val sourceDo = createHiveTable(
      schemaMin = Some(schemaMin),
      tableName = "source_table",
      dirPath = tempPath,
      df = DataFrameUtil.getEmptyDataFrame(schema),
      primaryKeyColumns = Some(Seq("id"))
    )

    noException should be thrownBy sourceDo.writeDataFrame(Seq(
      ("foo", "bar")
    ).toDF(schema.names:_*), Seq.empty)
  }

  test("Write: SchemaMin equals Schema is valid (ignoring nullable)") {
    val schemaMin: StructType = StructType(StructField("id", StringType, nullable = false) :: StructField("value", StringType, nullable = true) :: Nil)
    val schema: StructType = StructType(StructField("id", StringType, nullable = true) :: StructField("value", StringType, nullable = true) :: Nil)
    val sourceDo = createHiveTable(
      schemaMin = Some(schemaMin),
      tableName = "source_table",
      dirPath = tempPath,
      df = DataFrameUtil.getEmptyDataFrame(schema),
      primaryKeyColumns = Some(Seq("id"))
    )

    noException should be thrownBy sourceDo.writeDataFrame(Seq(
      ("foo", "bar")
    ).toDF(schema.names:_*), Seq.empty)
  }

  test("Write: SchemaMin is valid subset of Schema") {
    val schemaMin: StructType = StructType(StructField("id", StringType, nullable = false) :: Nil)
    val schema: StructType = StructType(StructField("id", StringType, nullable = true) :: StructField("value", StringType, nullable = true) :: Nil)
    val sourceDo = createHiveTable(
      schemaMin = Some(schemaMin),
      tableName = "source_table",
      dirPath = tempPath,
      df = DataFrameUtil.getEmptyDataFrame(schema),
      primaryKeyColumns = Some(Seq("id"))
    )

    noException should be thrownBy sourceDo.writeDataFrame(Seq(
      ("foo", "bar")
    ).toDF(schema.names:_*), Seq.empty)
  }

  test("Write: Invalid schema - missing column") {
    val schemaMin: StructType = StructType(StructField("id",
      StringType, nullable = false) ::
      StructField("value", StringType, nullable = true) ::
      StructField("value2", StringType, nullable = true) ::
      Nil)
    val schema: StructType = StructType(StructField("id", StringType, nullable = true) :: StructField("value", StringType, nullable = true) :: Nil)
    val sourceDo = createHiveTable(
      schemaMin = Some(schemaMin),
      tableName = "source_table",
      dirPath = tempPath,
      df = DataFrameUtil.getEmptyDataFrame(schema),
      primaryKeyColumns = Some(Seq("id"))
    )

    val thrown = the [SchemaViolationException] thrownBy sourceDo.writeDataFrame(Seq(
      ("foo", "bar")
    ).toDF(schema.names:_*), Seq.empty)
    println(thrown.getMessage)
  }

  test("Write: Invalid schema - wrong datatype") {
    val schemaMin: StructType = StructType(StructField("id",
      StringType, nullable = false) ::
      StructField("value", IntegerType, nullable = true) ::
      Nil)
    val schema: StructType = StructType(StructField("id", StringType, nullable = true) :: StructField("value", StringType, nullable = true) :: Nil)
    val sourceDo = createHiveTable(
      schemaMin = Some(schemaMin),
      tableName = "source_table",
      dirPath = tempPath,
      df = DataFrameUtil.getEmptyDataFrame(schema),
      primaryKeyColumns = Some(Seq("id"))
    )

    val thrown = the [SchemaViolationException] thrownBy sourceDo.writeDataFrame(Seq(
      ("foo", "bar")
    ).toDF(schema.names:_*), Seq.empty)
    println(thrown.getMessage)
  }
}
