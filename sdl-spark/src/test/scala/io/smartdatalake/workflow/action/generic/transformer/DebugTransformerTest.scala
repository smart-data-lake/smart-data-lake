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
package io.smartdatalake.workflow.action.generic.transformer

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.DebugTransformerBehaviour
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

class DebugTransformerTest extends AnyFunSuite with BeforeAndAfter with DebugTransformerBehaviour {
  protected implicit val session: SparkSession = SparkTestUtil.session

  override def subFeedType: Type = typeOf[SparkSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext

  before {
    instanceRegistry.clear()
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
  }

  test("copy load with transformer, a regular and a skipped input, skipped input is reset after decision to execute Action was made") {
    testCopyLoadWithTransformerRegularAndSkippedInput(
      id => MockSparkDataObject(id),
      id => MockSparkDataObject(id, partitions = Seq("lastname"), primaryKey = Some(Seq("lastname", "firstname")))
    )
  }
}
