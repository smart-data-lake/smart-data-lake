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
import io.smartdatalake.workflow.dataobject.SparkConnectTableDataObject
import io.smartdatalake.workflow.dataobject.generic.Table
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{Canceled, Outcome}

/**
 * Tests constraints and expectations of SparkConnectTableDataObject, using the shared engine-agnostic
 * TableDataObjectBehaviour. Both are evaluated with the standard Spark Observation API, see `SparkConnectObservation`.
 * Needs a Spark Connect server, see [[SparkConnectTestUtil]] and start-spark-connect.sh.
 * Tests are cancelled (not failed) if no server is available.
 */
class SparkConnectTableDataObjectBehaviourTest extends AnyFunSuite
  with SmartDataLakeLogger with TableDataObjectBehaviour {

  override val defaultEngineConnection: Connection with EngineConnection =
    SparkConnectConnection(ConnectionId(Environment.defaultEngineConnectionId), SparkConnectTestUtil.url)

  // cancel all tests of this suite if no spark connect server is available
  override def withFixture(test: NoArgTest): Outcome = {
    if (!SparkConnectTestUtil.serverAvailable) Canceled(s"No Spark Connect server available at ${SparkConnectTestUtil.url}")
    else super.withFixture(test)
  }

  private def createSrcDataObject(id: String, registry: InstanceRegistry) =
    SparkConnectTableDataObject(id, Table(Some("default"), s"sdlb_sctdo_behaviour_$id"),
      connectionId = defaultEngineConnection.id)(registry)

  // Spark Connect has no client-side filesystem access, tables are created as managed tables.
  // Note that constraints and expectations do not need a table format supporting row-level operations.
  private def createTableDataObject(id: String, params: TableDataObjectTestParams, registry: InstanceRegistry): SparkConnectTableDataObject =
    SparkConnectTableDataObject(id, partitions = params.partitions, options = params.options,
      table = Table(Some("default"), s"sdlb_sctdo_behaviour_$id", primaryKey = params.primaryKey),
      constraints = params.constraints, expectations = params.expectations, saveMode = params.saveMode,
      allowSchemaEvolution = params.allowSchemaEvolution, connectionId = defaultEngineConnection.id)(registry)

  test("constraints validation") {
    testConstraints(createSrcDataObject, createTableDataObject)
  }

  test("copy load expectations test") {
    testCopyLoadWithExpectations(createSrcDataObject, createTableDataObject)
  }
}
