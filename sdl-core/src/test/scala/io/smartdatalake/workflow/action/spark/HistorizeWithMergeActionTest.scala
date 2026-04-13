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
package io.smartdatalake.workflow.action.spark

import io.smartdatalake.testutils.spark.dataset.TestToolDataset
import io.smartdatalake.testutils.{HistorizeActionBehaviour, MockSparkDataObject, TestUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.dataset.Equality
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HistorizeWithMergeActionTest extends AnyFunSuite with Matchers with SmartDataLakeLogger
  with TestToolDataset with Equality with HistorizeActionBehaviour {

  override def defaultEngineConnection: Connection with EngineConnection = TestUtil.defaultSparkConnection

  testsFor(historizeWithMergeMode(
    (id, registry) => MockSparkDataObject(id)(registry),
    (id, pks, registry) => MockSparkDataObject(id, primaryKey = pks)(registry)
  ))

}
