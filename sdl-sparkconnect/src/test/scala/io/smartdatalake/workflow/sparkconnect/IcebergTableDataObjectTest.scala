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
package io.smartdatalake.workflow.sparkconnect

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.sparkconnect.SparkConnectTestUtil
import io.smartdatalake.testutils.{TableDataObjectBehaviour, TableDataObjectTestParams}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.{Connection, EngineConnection, SparkConnectConnection}
import io.smartdatalake.workflow.dataobject.generic.Table
import io.smartdatalake.workflow.dataobject.{IcebergTableDataObject, SparkConnectTableDataObject}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{Canceled, Outcome}

/**
 * Tests IcebergTableDataObject with the Spark Connect engine implementation (IcebergTableSparkConnectEngine),
 * using the shared engine-agnostic TableDataObjectBehaviour.
 * Needs a Spark Connect server with Iceberg support, see [[SparkConnectTestUtil]] and start-spark-connect.sh.
 * Tests are cancelled (not failed) if no such server is available.
 */
class IcebergTableDataObjectTest extends AnyFunSuite
  with SmartDataLakeLogger with TableDataObjectBehaviour {

  override val defaultEngineConnection: Connection with EngineConnection =
    SparkConnectConnection(ConnectionId(Environment.defaultEngineConnectionId), SparkConnectTestUtil.url)

  // cancel all tests of this suite if no spark connect server with Iceberg support is available
  override def withFixture(test: NoArgTest): Outcome = {
    if (!SparkConnectTestUtil.icebergAvailable) Canceled(s"No Spark Connect server with Iceberg support available at ${SparkConnectTestUtil.url}")
    else super.withFixture(test)
  }

  // the source DataObject is a plain spark table in the default (delta) catalog
  private def createSrcDataObject(id: String, registry: InstanceRegistry) =
    SparkConnectTableDataObject(id, Table(Some("default"), s"sdlb_iceberg_behaviour_$id"),
      connectionId = defaultEngineConnection.id)(registry)

  // Spark Connect has no client-side filesystem access, tables are created as managed tables of the Iceberg catalog
  private def createTableDataObject(id: String, params: TableDataObjectTestParams, registry: InstanceRegistry): IcebergTableDataObject =
    IcebergTableDataObject(id, partitions = params.partitions, options = params.options,
      table = Table(catalog = Some(SparkConnectTestUtil.icebergCatalog), db = Some("default"), name = s"sdlb_iceberg_behaviour_$id", primaryKey = params.primaryKey),
      constraints = params.constraints, expectations = params.expectations, saveMode = params.saveMode,
      allowSchemaEvolution = params.allowSchemaEvolution)(registry)

  test("Write data") {
    testCopyLoad(createSrcDataObject, createTableDataObject)
  }

  test("Write data partitioned") {
    // movePartitions is not implemented by IcebergTableDataObject
    testCopyLoadPartitioned(createSrcDataObject, createTableDataObject, testMovePartitions = false)
  }

  test("SaveMode overwrite with different schema") {
    testOverwriteWithDifferentSchema(createTableDataObject)
  }

  test("SaveMode append with different schema") {
    testAppendWithDifferentSchema(createTableDataObject)
  }

  test("SaveMode overwrite and delete partition") {
    testOverwriteAndDeletePartition(createTableDataObject)
  }

  test("SaveMode overwrite partitions dynamically") {
    testOverwritePartitionsDynamically(createTableDataObject)
  }

  test("SaveMode append") {
    testAppend(createTableDataObject)
  }

  test("throw NoDataToProcessWarning if no new snapshot created (no data)") {
    testNoDataToProcessWarningOnEmptyWrite(createTableDataObject)
  }

  test("SaveMode merge") {
    testMerge(createTableDataObject)
  }

  test("SaveMode merge with updateCols") {
    testMergeWithUpdateColumns(createTableDataObject)
  }

  test("SaveMode merge with schema evolution") {
    testMergeWithSchemaEvolution(createTableDataObject)
  }

  test("write with different order of columns") {
    testWriteWithDifferentColumnOrder(createTableDataObject)
  }

  test("returns correct metrics") {
    testWriteMetrics(createSrcDataObject, createTableDataObject)
  }

  test("copy load expectations test") {
    testCopyLoadWithExpectations(createSrcDataObject, createTableDataObject)
  }

  test("constraints validation") {
    testConstraints(createSrcDataObject, createTableDataObject)
  }

  test("normal output mode") {
    testNormalOutputModeWithoutCdc(createTableDataObject)
  }

  test("incremental output mode with inserts") {
    // iceberg snapshot ids used as state are not monotonically increasing
    testIncrementalOutputModeWithInserts(createTableDataObject, stateIsOrdered = false)
  }

  test("incremental output mode without primary keys") {
    testIncrementalOutputModeWithoutPrimaryKey(createTableDataObject)
  }

  test("incremental output mode with updates and inserts") {
    testIncrementalOutputModeWithUpdatesAndInserts(createTableDataObject)
  }
}
