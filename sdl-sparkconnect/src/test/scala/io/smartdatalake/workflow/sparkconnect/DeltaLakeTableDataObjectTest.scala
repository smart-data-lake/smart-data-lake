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
import io.smartdatalake.workflow.dataobject.{DeltaLakeTableDataObject, SparkConnectTableDataObject}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{Canceled, Outcome}

/**
 * Tests DeltaLakeTableDataObject with the Spark Connect engine implementation (DeltaLakeTableSparkConnectEngine),
 * using the shared engine-agnostic TableDataObjectBehaviour.
 * Needs a Spark Connect server with delta lake support, see [[SparkConnectTestUtil]] and start-spark-connect.sh.
 * Tests are cancelled (not failed) if no such server is available.
 */
class DeltaLakeTableDataObjectTest extends AnyFunSuite
  with SmartDataLakeLogger with TableDataObjectBehaviour {

  override val defaultEngineConnection: Connection with EngineConnection =
    SparkConnectConnection(ConnectionId(Environment.defaultEngineConnectionId), SparkConnectTestUtil.url)

  // cancel all tests of this suite if no spark connect server with delta lake support is available
  override def withFixture(test: NoArgTest): Outcome = {
    if (!SparkConnectTestUtil.deltaAvailable) Canceled(s"No Spark Connect server with delta lake support available at ${SparkConnectTestUtil.url}")
    else super.withFixture(test)
  }

  private def createSrcDataObject(id: String, registry: InstanceRegistry) =
    SparkConnectTableDataObject(id, Table(Some("default"), s"sdlb_tdo_behaviour_$id"),
      connectionId = defaultEngineConnection.id)(registry)

  // Spark Connect has no client-side filesystem access, tables are created as managed tables
  private def createTableDataObject(id: String, params: TableDataObjectTestParams, registry: InstanceRegistry): DeltaLakeTableDataObject =
    DeltaLakeTableDataObject(id, partitions = params.partitions, options = params.options,
      table = Table(Some("default"), s"sdlb_tdo_behaviour_$id", primaryKey = params.primaryKey),
      constraints = params.constraints, expectations = params.expectations, saveMode = params.saveMode,
      allowSchemaEvolution = params.allowSchemaEvolution)(registry)

  test("CustomDf2DeltaTable") {
    // no column statistics with the Spark Connect engine
    testCopyLoad(createSrcDataObject, createTableDataObject, expectColumnStats = false)
  }

  test("CustomDf2DeltaTable_partitioned") {
    testCopyLoadPartitioned(createSrcDataObject, createTableDataObject)
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

  test("SaveMode merge") {
    testMerge(createTableDataObject)
  }

  test("SaveMode merge with schema evolution") {
    testMergeWithSchemaEvolution(createTableDataObject)
  }

  test("SaveMode merge with updateCols") {
    testMergeWithUpdateColumns(createTableDataObject)
  }

  test("write with different order of columns") {
    testWriteWithDifferentColumnOrder(createTableDataObject)
  }

  // Note: testNoDataToProcessWarningOnEmptyWrite is not applicable to DeltaLake, as delta commits a new (empty)
  // table version even when writing an empty DataFrame, so the "no new version written" check never triggers.

  test("constraints validation") {
    testConstraints(createSrcDataObject, createTableDataObject)
  }

  test("returns correct metrics") {
    testWriteMetrics(createSrcDataObject, createTableDataObject)
  }

  test("normal output mode without cdc activated") {
    testNormalOutputModeWithoutCdc(createTableDataObject)
  }

  test("incremental output mode with inserts") {
    testIncrementalOutputModeWithInserts(createTableDataObject)
  }

  test("incremental output mode without primary keys") {
    testIncrementalOutputModeWithoutPrimaryKey(createTableDataObject)
  }

  test("incremental output mode with updates and inserts") {
    testIncrementalOutputModeWithUpdatesAndInserts(createTableDataObject)
  }

  test("copy load expectations test") {
    testCopyLoadWithExpectations(createSrcDataObject, createTableDataObject)
  }
}
