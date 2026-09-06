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
package io.smartdatalake.workflow.action.plainScala

import io.smartdatalake.testutils.plainScala.{MockScalaDataObject, ScalaTestUtil}
import io.smartdatalake.testutils.UpsertActionBehaviour
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import org.scalatest.funsuite.AnyFunSuite

class UpsertWithMergeActionTest extends AnyFunSuite with SmartDataLakeLogger with UpsertActionBehaviour {

  override def defaultEngineConnection: Connection with EngineConnection = ScalaTestUtil.defaultScalaConnection

  test("upsert load mergeModeEnable") {
    testUpsertWithMergeMode(
      (id, _) => MockScalaDataObject(id),
      (id, pks, _) => MockScalaDataObject(id, primaryKey = pks)
    )
  }

  test("upsert load mergeModeEnable updateCapturedColumnOnlyWhenChanged") {
    testUpsertWithMergeModeUpdateCapturedColumnOnlyWhenChanged(
      (id, _) => MockScalaDataObject(id),
      (id, pks, _) => MockScalaDataObject(id, primaryKey = pks)
    )

  }

  test("upsert load mergeModeEnable sourceTimestampColumn") {
    testUpsertWithMergeModeSourceTimestampColumn(
      (id, _) => MockScalaDataObject(id),
      (id, pks, _) => MockScalaDataObject(id, primaryKey = pks)
    )
  }

  test("upsert load mergeModeEnable sourceTimestampColumn updateCapturedColumnOnlyWhenChanged") {
    testUpsertWithMergeModeSourceTimestampColumnUpdateOnlyWhenChanged(
      (id, _) => MockScalaDataObject(id),
      (id, pks, _) => MockScalaDataObject(id, primaryKey = pks)
    )
  }

  // SQLDfTransformer does not yet work with ScalaSubFeed
  ignore("upsert 1st 2nd load with transformer changing schema") {
    testUpsertWithTransformerChangingSchema(
      (id, _) => MockScalaDataObject(id),
      (id, pks, _) => MockScalaDataObject(id, primaryKey = pks)
    )
  }
}
