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
import io.smartdatalake.testutils.plainScala.{MockScalaDataObject, ScalaTestUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

// DeduplicateTransformer with rankingExpression uses window/row_number, which are not implemented for ScalaSubFeed
class DeduplicateTransformerTest extends AnyFunSuite with BeforeAndAfter with DeduplicateTransformerBehaviour {

  override def subFeedType: Type = typeOf[ScalaSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  before {
    instanceRegistry.clear()
  }

  ignore("deduplication test with primary key") {
    testDeduplicationWithPrimaryKey()
  }

  ignore("deduplication test with primary key and different rankingExpression") {
    testDeduplicationWithPrimaryKeyAndDifferentRankingExpression()
  }

  ignore("deduplication test with multiple primary key columns") {
    testDeduplicationWithMultiplePrimaryKeyColumns()
  }

  ignore("deduplication test without primary key") {
    testDeduplicationWithoutPrimaryKey(
      id => MockScalaDataObject(id, primaryKey = Some(Seq("pk1", "pk2"))),
      id => MockScalaDataObject(id)
    )
  }

  ignore("deduplication test with primary key columns detection") {
    testDeduplicationWithPrimaryKeyColumnsDetection(
      id => MockScalaDataObject(id, primaryKey = Some(Seq("pk1", "pk2"))),
      (id, pks) => MockScalaDataObject(id, primaryKey = Some(pks))
    )
  }

}
