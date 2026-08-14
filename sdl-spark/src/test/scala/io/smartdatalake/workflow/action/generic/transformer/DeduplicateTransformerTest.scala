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
import io.smartdatalake.testutils.DeduplicateTransformerBehaviour
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

class DeduplicateTransformerTest extends AnyFunSuite with BeforeAndAfter with DeduplicateTransformerBehaviour {

  protected implicit val session: SparkSession = SparkTestUtil.session

  override def subFeedType: Type = typeOf[SparkSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec) // note that the DataFrameCacheRegistry is shared between phases like this!

  before {
    instanceRegistry.clear()
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
  }

  test("deduplication test with primary key") {
    testDeduplicationWithPrimaryKey()
  }

  test("deduplication test with primary key and different rankingExpression") {
    testDeduplicationWithPrimaryKeyAndDifferentRankingExpression()
  }

  test("deduplication test with multiple primary key columns") {
    testDeduplicationWithMultiplePrimaryKeyColumns()
  }

  test("deduplication test without primary key") {
    testDeduplicationWithoutPrimaryKey(
      id => MockSparkDataObject(id, primaryKey = Some(Seq("pk1", "pk2"))),
      id => MockSparkDataObject(id)
    )
  }

  test("deduplication test with primary key columns detection") {
    testDeduplicationWithPrimaryKeyColumnsDetection(
      id => MockSparkDataObject(id, primaryKey = Some(Seq("pk1", "pk2"))),
      (id, pks) => MockSparkDataObject(id, primaryKey = Some(pks))
    )
  }

}
