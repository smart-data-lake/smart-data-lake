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

import io.smartdatalake.workflow.dataframe.GenericSchema
import org.json4s.JString
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf

/**
 * Tests for the engine-neutral Json representation of SQL schemas, see GenericSchema.toJson.
 */
class SQLSchemaTest extends AnyFunSuite {

  private def simple(sql: String) = SQLSimpleDataType(sql)

  test("simple types are written with their Spark type name") {
    val names = Seq("TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL(10, 2)", "DECIMAL", "FLOAT", "DOUBLE", "TEXT", "VARCHAR(20)",
      "UUID", "BOOLEAN", "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMPNTZ", "VARBINARY", "UNKNOWN", "GEOMETRY")
      .map(t => simple(t).toJson)
    assert(names == Seq("byte", "short", "integer", "long", "decimal(10,2)", "decimal(38,18)", "float", "double", "string", "string",
      "string", "boolean", "date", "timestamp", "timestamp", "timestamp_ntz", "binary", "UNKNOWN", "GEOMETRY").map(JString))
  }

  test("schema is parsed from its Json representation") {
    val schema = SQLSchema(Seq(
      SQLField("id", simple("INT"), nullable = false, comment = Some("the id")),
      SQLField("amount", simple("DECIMAL(10, 2)")),
      SQLField("name", simple("VARCHAR(20)")),
      SQLField("ts", simple("TIMESTAMP")),
      SQLField("tags", SQLArrayDataType(simple("TEXT"))),
      SQLField("props", SQLMapDataType(simple("TEXT"), simple("BIGINT"))),
      SQLField("address", SQLStructDataType(Seq(SQLField("city", simple("TEXT")), SQLField("zip", simple("SMALLINT")))))
    ))
    val parsed = GenericSchema.fromJson(schema.toJson, typeOf[SQLSubFeed])
    // string types lose their length
    assert(parsed == schema.copy(fields = schema.fields.map(f => if (f.name == "name") f.copy(dataType = simple("TEXT")) else f)))
  }
}
