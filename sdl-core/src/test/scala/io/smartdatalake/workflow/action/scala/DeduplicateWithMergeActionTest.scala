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
package io.smartdatalake.workflow.action.scala

import io.smartdatalake.testutils.{DeduplicateActionBehaviour, MockScalaDataObject}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.scalatest.funsuite.AnyFunSuite

class DeduplicateWithMergeActionTest extends AnyFunSuite with SmartDataLakeLogger with DeduplicateActionBehaviour {

  test("deduplicate load mergeModeEnable") {
    testDeduplicateWithMergeMode(
      (id, registry) => MockScalaDataObject(id),
      (id, pks, registry) => MockScalaDataObject(id, primaryKey = pks)
    )
  }

  test("deduplicate load mergeModeEnable updateCapturedColumnOnlyWhenChanged") {
    testDeduplicateWithMergeModeUpdateCapturedColumnOnlyWhenChanged(
      (id, registry) => MockScalaDataObject(id),
      (id, pks, registry) => MockScalaDataObject(id, primaryKey = pks)
    )

  }

  // SQLDfTransformer does not yet work with ScalaSubFeed
  ignore("deduplicate 1st 2nd load with transformer changing schema") {
    testDeduplicateWithTransformerChangingSchema(
      (id, registry) => MockScalaDataObject(id),
      (id, pks, registry) => MockScalaDataObject(id, primaryKey = pks)
    )
  }
}
