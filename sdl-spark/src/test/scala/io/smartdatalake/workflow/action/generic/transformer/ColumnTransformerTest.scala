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

import io.smartdatalake.app.DefaultSmartDataLakeBuilder
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.ColumnsTransformerBehaviour
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

class ColumnTransformerTest extends AnyFunSuite with BeforeAndAfter with ColumnsTransformerBehaviour {

  protected implicit val session: SparkSession = SparkTestUtil.session

  override def subFeedType: Type = typeOf[SparkSubFeed]
  val sdlb: DefaultSmartDataLakeBuilder.type = DefaultSmartDataLakeBuilder
  implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
  implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
  }

  test("additional derived, renamed and dropped columns") {
    testAdditionalDerivedRenamedAndDroppedColumns()
  }

  test("additional columns using a context expression") {
    testAdditionalColumnsUsingContextExpression()
  }

  test("additional derived column using a window function") {
    testAdditionalDerivedColumnUsingWindowFunction()
  }
}