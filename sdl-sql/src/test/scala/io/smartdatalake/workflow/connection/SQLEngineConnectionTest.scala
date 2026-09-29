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
package io.smartdatalake.workflow.connection

import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.{ConfigParser, ConfigurationException, InstanceRegistry}
import io.smartdatalake.testutils.sql.SQLTestUtil
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.workflow.dataframe.sql.SQLSubFeed
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf

class SQLEngineConnectionTest extends AnyFunSuite {

  test("SQLEngineConnection is parsable") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    val connection = ConfigParser.parseConfigObject[Connection](
      ConfigFactory.parseString("""type = SQLEngineConnection, id = sql-engine, url = "jdbc:postgresql://localhost/db", sqlDialect = spark""")
    )
    assert(connection == SQLEngineConnection(ConnectionId("sql-engine"), url = "jdbc:postgresql://localhost/db", sqlDialect = Some("spark")))
    assert(connection.asInstanceOf[EngineConnection].subFeedType =:= typeOf[SQLSubFeed])
  }

  test("dialect is derived from the JDBC url") {
    val connection = SQLEngineConnection(ConnectionId("c"), url = "jdbc:sqlserver://localhost:1433;databaseName=db")
    assert(connection.databaseDialect == "tsql")
    assert(connection.queryDialect == "tsql")
    assert(SQLEngineConnection(ConnectionId("c"), url = "jdbc:duckdb:").databaseDialect == "duckdb")
    assert(SQLEngineConnection(ConnectionId("c"), url = "jdbc:hsqldb:mem:x", dialect = Some("postgres")).databaseDialect == "postgres")
    intercept[ConfigurationException](SQLEngineConnection(ConnectionId("c"), url = "jdbc:hsqldb:mem:x"))
  }

  test("SQL is executed on the database") {
    val connection = SQLTestUtil.createEngineConnection("SQLEngineConnectionTest")
    connection.execJdbcStatement("create table t as select 1 as a")
    assert(connection.execJdbcQuery("select a from t", rs => { rs.next(); rs.getInt(1) }) == 1)
    connection.pool.close()
  }
}
