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
package io.smartdatalake.workflow.action

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.{DeduplicateActionBehaviour, MockSparkDataObject, TestUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataobject.DeltaLakeTableDataObject
import io.smartdatalake.workflow.dataobject.DeltaLakeTestUtils.deltaDb
import io.smartdatalake.workflow.dataobject.generic.Table
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class DeltaLakeDeduplicateWithMergeActionTest extends AnyFunSuite
    with SmartDataLakeLogger with DeduplicateActionBehaviour {

  private implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  override def defaultEngineConnection: Connection with EngineConnection = TestUtil.defaultSparkConnection

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  test("deduplicate load mergeModeEnable") {
    testDeduplicateWithMergeMode(
      (id, _) => MockSparkDataObject(id),
      (id, pks, registry) => {
        val tgtTable = Table(db = Some(deltaDb), name = id.replaceAll("-", "_"), primaryKey = pks)
        DeltaLakeTableDataObject(id, Some(tempPath + s"/${tgtTable.fullName}"), table = tgtTable, allowSchemaEvolution = true)(registry)
      }
    )
  }

  test("deduplicate load mergeModeEnable updateCapturedColumnOnlyWhenChanged") {
    testDeduplicateWithMergeModeUpdateCapturedColumnOnlyWhenChanged(
      (id, _) => MockSparkDataObject(id),
      (id, pks, registry) => {
        val tgtTable = Table(db = Some(deltaDb), name = id.replaceAll("-", "_"), primaryKey = pks)
        DeltaLakeTableDataObject(id, Some(tempPath + s"/${tgtTable.fullName}"), table = tgtTable, allowSchemaEvolution = true)(registry)
      }
    )
  }

  test("deduplicate 1st 2nd load with transformer changing schema") {
    testDeduplicateWithTransformerChangingSchema(
      (id, _) => MockSparkDataObject(id),
      (id, pks, registry) => {
        val tgtTable = Table(db = Some(deltaDb), name = id.replaceAll("-", "_"), primaryKey = pks)
        DeltaLakeTableDataObject(id, Some(tempPath + s"/${tgtTable.fullName}"), table = tgtTable, allowSchemaEvolution = true)(registry)
      }
    )
  }

}
