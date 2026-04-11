/*
 * Smart Data Lake - Build your data lake the smart way.
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

package io.smartdatalake.workflow.action.spark

import io.smartdatalake.testutils.{CopyActionBehaviour, MockSparkDataObject, TestUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataobject.ParquetFileDataObject
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class OfflineCopyActionTest extends AnyFunSuite with SmartDataLakeLogger with CopyActionBehaviour {

  override def defaultEngineConnection: Connection with EngineConnection = TestUtil.defaultSparkConnection

  test("copy dry-run in offline environment, reading exported schemas") {

    testCopyActionOffline(
      (id, registry) => MockSparkDataObject(id)(registry),
      (id, pks, registry) => MockSparkDataObject(id, primaryKey = pks)(registry),
    )
  }

  test("copy dry-run in offline environment, reading exported schemas with filename column") {
    val tempDir = Files.createTempDirectory(getClass.getSimpleName)

    val tgtSubFeed = testCopyActionOffline(
      (id, registry) => ParquetFileDataObject(id, tempDir.resolve("test1/src1").toString, filenameColumn = Some("_filename"))(registry),
      (id, pks, registry) => MockSparkDataObject(id, primaryKey = pks)(registry),
    )

    // Verify that the filename column is included in the target schema
    assert(tgtSubFeed.dataFrame.get.columns.contains("_filename"))
  }
}
