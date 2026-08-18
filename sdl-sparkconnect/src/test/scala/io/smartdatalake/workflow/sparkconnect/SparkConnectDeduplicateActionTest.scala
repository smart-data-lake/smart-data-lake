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
import io.smartdatalake.testutils.DeduplicateActionBehaviour
import io.smartdatalake.testutils.sparkconnect.SparkConnectTestUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.{Connection, EngineConnection, SparkConnectConnection}
import io.smartdatalake.workflow.dataobject.SparkConnectTableDataObject
import io.smartdatalake.workflow.dataobject.generic.Table
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{Canceled, Outcome}

/**
 * Tests DeduplicateAction with SparkConnectTableDataObjects, using the shared DeduplicateActionBehaviour.
 * Needs a Spark Connect server with delta lake support, see [[SparkConnectTestUtil]] and start-spark-connect.sh.
 * Tests are cancelled (not failed) if no such server is available.
 */
class SparkConnectDeduplicateActionTest extends AnyFunSuite
  with SmartDataLakeLogger with DeduplicateActionBehaviour {

  override val defaultEngineConnection: Connection with EngineConnection =
    SparkConnectConnection(ConnectionId(Environment.defaultEngineConnectionId), SparkConnectTestUtil.url)

  // cancel all tests of this suite if no spark connect server with delta lake support is available
  override def withFixture(test: NoArgTest): Outcome = {
    if (!SparkConnectTestUtil.deltaAvailable) Canceled(s"No Spark Connect server with delta lake support available at ${SparkConnectTestUtil.url}")
    else super.withFixture(test)
  }

  private def createSrcDataObject(id: String, registry: InstanceRegistry) =
    SparkConnectTableDataObject(id, Table(Some("default"), s"sdlb_dedup_$id"),
      connectionId = defaultEngineConnection.id)(registry)

  private def createTgtDataObject(id: String, primaryKey: Option[Seq[String]], registry: InstanceRegistry) =
    SparkConnectTableDataObject(id, Table(Some("default"), s"sdlb_dedup_$id", primaryKey = primaryKey),
      format = Some("delta"), allowSchemaEvolution = true, connectionId = defaultEngineConnection.id)(registry)

  test("deduplicate 1st and 2nd load") {
    testDeduplicateTwoRuns(createSrcDataObject, createTgtDataObject)
  }

  test("deduplicate load with filter") {
    testDeduplicateWithFilter(createSrcDataObject, createTgtDataObject)
  }

  test("deduplicate load mergeModeEnable") {
    testDeduplicateWithMergeMode(createSrcDataObject, createTgtDataObject)
  }

  test("deduplicate load mergeModeEnable updateCapturedColumnOnlyWhenChanged") {
    testDeduplicateWithMergeModeUpdateCapturedColumnOnlyWhenChanged(createSrcDataObject, createTgtDataObject)
  }

  test("deduplicate load mergeModeEnable sourceTimestampColumn") {
    testDeduplicateWithMergeModeSourceTimestampColumn(createSrcDataObject, createTgtDataObject)
  }

  test("deduplicate load mergeModeEnable sourceTimestampColumn updateCapturedColumnOnlyWhenChanged") {
    testDeduplicateWithMergeModeSourceTimestampColumnUpdateOnlyWhenChanged(createSrcDataObject, createTgtDataObject)
  }

  test("deduplicate 1st 2nd load with transformer changing schema") {
    testDeduplicateWithTransformerChangingSchema(createSrcDataObject, createTgtDataObject)
  }
}
