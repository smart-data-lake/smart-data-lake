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
import io.smartdatalake.workflow.connection.jdbc.JdbcConnection
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres

import java.io.File

/**
 * Utilities for tests of the SQL engine. They run against a DuckDB database file in target/duckdb, or against an
 * embedded Postgres database for features DuckDB does not support, e.g. materialized views.
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
   * Create a JdbcConnection to a new DuckDB database, to be used as engine connection of the SQL engine.
   * Its id is `default-engine` by default, so that it is used by Actions without engineConnectionId.
   */
  def createEngineConnection(name: String, id: String = Environment.defaultEngineConnectionId): JdbcConnection =
    JdbcConnection(ConnectionId(id), url = createDuckDbUrl(name), driver = "org.duckdb.DuckDBDriver", db = Some("main"))

  /**
   * Embedded Postgres server, started on first use and shared by all tests of the JVM.
   */
  lazy val embeddedPostgres: EmbeddedPostgres = {
    val postgres = EmbeddedPostgres.start()
    sys.addShutdownHook(postgres.close())
    postgres
  }

  /**
   * Create a JdbcConnection to a new, empty schema `name` of the embedded Postgres server.
   */
  def createPostgresConnection(name: String, id: String = Environment.defaultEngineConnectionId): JdbcConnection = {
    val url = embeddedPostgres.getJdbcUrl("postgres", "postgres")
    val connection = JdbcConnection(ConnectionId(id), url = url, driver = "org.postgresql.Driver", db = Some(name))
    connection.execJdbcStatement(s"drop schema if exists $name cascade")
    connection.execJdbcStatement(s"create schema $name")
    connection
  }

  /**
   * The reason why the tests needing Python must be canceled, see [[JepInterpreter.unavailableReason]].
   */
  def pythonUnavailableReason: Option[String] = JepInterpreter.unavailableReason
}
