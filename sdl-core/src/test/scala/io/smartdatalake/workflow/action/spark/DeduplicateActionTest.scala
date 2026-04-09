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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.spark.dataset.TestToolDataset
import io.smartdatalake.testutils.{DeduplicateActionBehaviour, MockSparkDataObject, TestUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.dataset.Equality
import io.smartdatalake.workflow.action.DeduplicateAction
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

class DeduplicateActionTest extends AnyFunSuite with BeforeAndAfter with TestToolDataset with Equality with SmartDataLakeLogger with DeduplicateActionBehaviour {

  private implicit val loggerImpl: Logger = logger
  protected implicit val session: SparkSession = TestUtil.session

  test("deduplicate 1st 2nd load") {
    testDeduplicateTwoRuns(
      (id, registry) => MockSparkDataObject(id)(registry),
      (id, pks, registry) => MockSparkDataObject(id, primaryKey = pks)(registry)
    )
  }

  test("early validation that output primary key exists") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

    // setup DataObjects
    val srcDO = MockSparkDataObject("src1").register
    val tgtDO = MockSparkDataObject("tgt1").register

    // prepare & start 1st load
    intercept[IllegalArgumentException] {
      DeduplicateAction("dda", srcDO.id, tgtDO.id)
    }
  }

  test("deduplicate with filter clause") {
    testDeduplicateWithFilter(
      (id, registry) => MockSparkDataObject(id)(registry),
      (id, pks, registry) => MockSparkDataObject(id, primaryKey = pks)(registry)
    )
  }

  test("deduplicate 1st 2nd load with transformer changing schema") {
    testDeduplicateWithTransformerChangingSchema(
      (id, registry) => MockSparkDataObject(id)(registry),
      (id, pks, registry) => MockSparkDataObject(id, primaryKey = pks)(registry)
    )
  }

  test("deduplicate with schema evolution") {
    testDeduplicateWithSchemaEvolution(SparkSubFeed.subFeedType)
  }
}
