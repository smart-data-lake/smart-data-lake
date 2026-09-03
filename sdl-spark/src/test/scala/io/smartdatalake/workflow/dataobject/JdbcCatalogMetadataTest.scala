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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.spark.{SparkTestTool, SparkTestUtil}
import io.smartdatalake.testutils.{CatalogMetadataBehaviour, CatalogMetadataTestParams, DataObjectTestSuite}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.jdbc.JdbcTableConnection
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}

/**
 * Tests managing jdbc tables in the catalog at deployment time using the shared CatalogMetadataBehaviour,
 * see issue #1129. JdbcTableDataObject supports all of it: creating tables, schema changes, comments,
 * primary and foreign keys.
 */
class JdbcCatalogMetadataTest extends DataObjectTestSuite with SparkTestTool with SmartDataLakeLogger
  with CatalogMetadataBehaviour {

  private val jdbcConnection = JdbcTableConnection("jdbcCon1", "jdbc:hsqldb:mem:JdbcCatalogMetadataTest", "org.hsqldb.jdbcDriver")

  override def defaultEngineConnection: Connection with EngineConnection = SparkTestUtil.defaultSparkConnection

  private def createDataObject(id: String, params: CatalogMetadataTestParams, registry: InstanceRegistry): JdbcTableDataObject = {
    registry.register(jdbcConnection)
    JdbcTableDataObject(id, table = params.createTable(db = Some("public")), connectionId = "jdbcCon1",
      metadata = params.dataObjectMetadata)(registry)
  }

  test("create a missing table") {
    testCreateMissingTable(createDataObject)
  }

  test("evolve the schema of an existing table") {
    testEvolveSchema(createDataObject)
  }

  test("create the primary key") {
    testCreatePrimaryKey(createDataObject)
  }

  test("create foreign keys in a second phase") {
    testCreateForeignKeys(createDataObject)
  }

  test("foreign keys are not created if not enabled") {
    testForeignKeysNotCreatedIfNotEnabled(createDataObject)
  }
}
