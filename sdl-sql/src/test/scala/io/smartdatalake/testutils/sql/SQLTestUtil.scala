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
package io.smartdatalake.testutils.sql

import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.python.JepInterpreter
import io.smartdatalake.workflow.connection.SQLEngineConnection

import java.io.File

/**
 * Utilities for tests of the SQL engine. They run against a DuckDB database file in target/duckdb.
 */
object SQLTestUtil {

  /**
   * JDBC url of a new, empty DuckDB database. A database file is used instead of an in-memory database, because the
   * latter is dropped as soon as the connection pool closes its last idle connection.
   */
  def createDuckDbUrl(name: String): String = {
    val dir = new File("target/duckdb")
    dir.mkdirs()
    val file = new File(dir, s"$name.duckdb")
    file.delete()
    new File(file.getPath + ".wal").delete()
    s"jdbc:duckdb:${file.getAbsolutePath}"
  }

  /**
   * Create an SQLEngineConnection with id `default-engine` to a new DuckDB database.
   */
  def createEngineConnection(name: String, sqlDialect: Option[String] = Some("spark")): SQLEngineConnection =
    SQLEngineConnection(ConnectionId(Environment.defaultEngineConnectionId), url = createDuckDbUrl(name), sqlDialect = sqlDialect)

  /**
   * The reason why the tests needing Python must be canceled, see [[JepInterpreter.unavailableReason]].
   */
  def pythonUnavailableReason: Option[String] = JepInterpreter.unavailableReason
}
