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
package io.smartdatalake.workflow.connection.jdbc

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import org.scalatest.funsuite.AnyFunSuite

class JdbcTableConnectionDialectTest extends AnyFunSuite {

  private def connection(url: String, dialect: Option[String] = None) =
    JdbcTableConnection(ConnectionId("c"), url = url, driver = "org.hsqldb.jdbcDriver", dialect = dialect)

  test("SQLGlot dialect is derived from the JDBC url") {
    assert(connection("jdbc:postgresql://localhost:5432/db").sqlGlotDialect == "postgres")
    assert(connection("jdbc:sqlserver://localhost:1433;databaseName=db").sqlGlotDialect == "tsql")
    assert(connection("jdbc:duckdb:").sqlGlotDialect == "duckdb")
    assert(connection("jdbc:hsqldb:mem:x", dialect = Some("postgres")).sqlGlotDialect == "postgres")
    intercept[ConfigurationException](connection("jdbc:hsqldb:mem:x").sqlGlotDialect)
  }

  test("identifiers are quoted according to the database") {
    assert(connection("jdbc:postgresql://localhost:5432/db").catalog.quoteIdentifier("a") == "\"a\"")
    assert(connection("jdbc:mariadb://localhost:3306/db").catalog.quoteIdentifier("a") == "`a`")
    assert(connection("jdbc:mysql://localhost:3306/db").catalog.isQuotedIdentifier("`a`"))
  }
}
