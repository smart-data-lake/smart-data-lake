/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.connection.jdbc

import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.scalatest.FunSuite

class MariaDbDialectTest extends FunSuite {

  test("can handle MariaDB url") {
    assert(MariaDbDialect.canHandle("jdbc:mariadb://localhost:3306"))
  }

  test("quote identifier") {
    val quotedIdentifier = MariaDbDialect.quoteIdentifier("quote.me")
    assert(quotedIdentifier == "`quote.me`")
  }

  test("table update query") {
    val tableUpdateQuery = MariaDbDialect.getUpdateColumnTypeQuery("test_table", "test_column", "varchar(10)")
    assert(tableUpdateQuery == "ALTER TABLE test_table MODIFY COLUMN `test_column` varchar(10)")
  }

  test("Spark IntegerType to JDBC INTEGER") {
    val jdbcType = MariaDbDialect.getJDBCType(IntegerType)
    assert(jdbcType.isDefined)
    assert(jdbcType.get.databaseTypeDefinition == "INTEGER")
  }

  test("Spark FloatType to JDBC FLOAT") {
    val jdbcType = MariaDbDialect.getJDBCType(FloatType)
    assert(jdbcType.isDefined)
    assert(jdbcType.get.databaseTypeDefinition == "FLOAT")
  }
}
