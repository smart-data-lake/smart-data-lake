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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.UpsertActionBehaviour
import io.smartdatalake.testutils.sql.{MockSQLTableDataObject, SQLTestUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.jdbc.JdbcConnection
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.sql.SQLSubFeed
import io.smartdatalake.workflow.dataobject.JdbcTableDataObject
import io.smartdatalake.workflow.dataobject.generic.Table
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, Outcome}

import scala.reflect.runtime.universe.typeOf

/**
 * UpsertAction with the SQL engine, executing SQL on a DuckDB database. The DuckDB connection is the default
 * engine connection, so the Actions use the SQL engine without engineConnectionId.
 */
class UpsertActionTest extends AnyFunSuite with BeforeAndAfterAll with SmartDataLakeLogger with UpsertActionBehaviour {

  override def withFixture(test: NoArgTest): Outcome = {
    val reason = SQLTestUtil.pythonUnavailableReason
    assume(reason.isEmpty, reason.getOrElse(""))
    super.withFixture(test)
  }

  private lazy val connection: JdbcConnection = SQLTestUtil.createEngineConnection(getClass.getSimpleName)

  override def afterAll(): Unit = connection.pool.close()

  override def defaultEngineConnection: Connection with EngineConnection = connection

  private def createSrcDataObject(id: String, registry: InstanceRegistry) = new MockSQLTableDataObject(id, connection.id)(registry)

  private def createTgtDataObject(id: String, primaryKey: Option[Seq[String]], registry: InstanceRegistry) =
    JdbcTableDataObject(id, table = Table(db = None, name = id, primaryKey = primaryKey), connectionId = connection.id,
      allowSchemaEvolution = true)(registry)

  test("upsert 1st 2nd load") {
    testUpsertTwoRuns(createSrcDataObject, createTgtDataObject)
  }

  test("upsert with filter clause") {
    testUpsertWithFilter(createSrcDataObject, createTgtDataObject)
  }

  test("upsert 1st 2nd load with transformer changing schema") {
    testUpsertWithTransformerChangingSchema(createSrcDataObject, createTgtDataObject)
  }

  test("upsert with schema evolution") {
    testUpsertWithSchemaEvolution(typeOf[SQLSubFeed])
  }

  test("upsert load mergeModeEnable") {
    testUpsertWithMergeMode(createSrcDataObject, createTgtDataObject)
  }

  test("upsert load mergeModeEnable updateCapturedColumnOnlyWhenChanged") {
    testUpsertWithMergeModeUpdateCapturedColumnOnlyWhenChanged(createSrcDataObject, createTgtDataObject)
  }

  test("upsert load mergeModeEnable sourceTimestampColumn") {
    testUpsertWithMergeModeSourceTimestampColumn(createSrcDataObject, createTgtDataObject)
  }

  test("upsert load mergeModeEnable sourceTimestampColumn updateCapturedColumnOnlyWhenChanged") {
    testUpsertWithMergeModeSourceTimestampColumnUpdateOnlyWhenChanged(createSrcDataObject, createTgtDataObject)
  }
}
