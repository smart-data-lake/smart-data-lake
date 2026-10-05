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
package io.smartdatalake.workflow.action.sql

import io.smartdatalake.testutils.HistorizeActionBehaviour
import io.smartdatalake.testutils.sql.{MockSQLTableDataObject, SQLTestUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.jdbc.JdbcConnection
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataobject.JdbcTableDataObject
import io.smartdatalake.workflow.dataobject.generic.Table
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, Outcome}

/**
 * HistorizeAction with the SQL engine, executing SQL on a DuckDB database. The DuckDB connection is the default
 * engine connection, so the Actions use the SQL engine without engineConnectionId.
 */
class HistorizeActionTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with SmartDataLakeLogger
  with HistorizeActionBehaviour {

  override def withFixture(test: NoArgTest): Outcome = {
    val reason = SQLTestUtil.pythonUnavailableReason
    assume(reason.isEmpty, reason.getOrElse(""))
    super.withFixture(test)
  }

  private lazy val connection: JdbcConnection = SQLTestUtil.createEngineConnection(getClass.getSimpleName)

  override def afterAll(): Unit = connection.pool.close()

  override def defaultEngineConnection: Connection with EngineConnection = connection

  testsFor(historizeWithMergeMode(
    (id, registry) => new MockSQLTableDataObject(id, connection.id)(registry),
    (id, pks, registry) => JdbcTableDataObject(id, table = Table(db = None, name = id, primaryKey = pks), connectionId = connection.id,
      allowSchemaEvolution = true)(registry)
  ))

  testsFor(historizeIncrementalPipeline(
    (id, registry) => new MockSQLTableDataObject(id, connection.id)(registry),
    (id, pks, registry) => JdbcTableDataObject(id, table = Table(db = None, name = id, primaryKey = pks), connectionId = connection.id,
      allowSchemaEvolution = true)(registry)
  ))

  testsFor(activateMergeMode(
    (id, registry) => new MockSQLTableDataObject(id, connection.id)(registry),
    (id, pks, registry) => JdbcTableDataObject(id, table = Table(db = None, name = id, primaryKey = pks), connectionId = connection.id,
      allowSchemaEvolution = true)(registry)
  ))
}
