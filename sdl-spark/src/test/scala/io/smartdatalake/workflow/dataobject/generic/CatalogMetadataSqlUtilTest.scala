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
package io.smartdatalake.workflow.dataobject.generic

import io.smartdatalake.util.misc.SQLUtil
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test the statements applying schema changes to catalogs using Spark SQL syntax, e.g. Delta Lake and
 * Iceberg tables, see issue #1129.
 */
class CatalogMetadataSqlUtilTest extends AnyFunSuite {

  private val table = Table(db = Some("testdb"), name = "testtable")

  private def dataTypeOf(dataType: org.apache.spark.sql.types.DataType) =
    SparkSchema(StructType(Seq(StructField("c", dataType)))).fields.head.dataType

  test("add a column with a comment") {
    val stmt = CatalogMetadataSqlUtil.getSchemaChangeStmt(table, AddColumn(Seq("city"), dataTypeOf(StringType), Some("City's name")))
    assert(stmt == "ALTER TABLE testdb.testtable ADD COLUMNS (city STRING COMMENT 'City''s name')")
  }

  test("add a nested column") {
    val stmt = CatalogMetadataSqlUtil.getSchemaChangeStmt(table, AddColumn(Seq("address", "street"), dataTypeOf(StringType)))
    assert(stmt == "ALTER TABLE testdb.testtable ADD COLUMNS (address.street STRING)")
  }

  test("change the data type of a column") {
    val stmt = CatalogMetadataSqlUtil.getSchemaChangeStmt(table, ChangeColumnType(Seq("nr"), dataTypeOf(LongType), dataTypeOf(StringType)))
    assert(stmt == "ALTER TABLE testdb.testtable ALTER COLUMN nr TYPE BIGINT")
  }

  test("change the nullability of a column") {
    assert(CatalogMetadataSqlUtil.getSchemaChangeStmt(table, ChangeColumnNullable(Seq("nr"), nullable = true))
      == "ALTER TABLE testdb.testtable ALTER COLUMN nr DROP NOT NULL")
    assert(CatalogMetadataSqlUtil.getSchemaChangeStmt(table, ChangeColumnNullable(Seq("nr"), nullable = false))
      == "ALTER TABLE testdb.testtable ALTER COLUMN nr SET NOT NULL")
  }

  test("create a foreign key constraint") {
    val foreignKey = ForeignKeyDefinition(Map("customer_id" -> "id"), "testdb.customer", Some("sdlb_testtable_customer_fk"))
    assert(SQLUtil.createForeignKeyStatement(table.fullName, foreignKey) ==
      "ALTER TABLE testdb.testtable ADD CONSTRAINT sdlb_testtable_customer_fk" +
        " FOREIGN KEY (customer_id) REFERENCES testdb.customer (id)")
  }
}
