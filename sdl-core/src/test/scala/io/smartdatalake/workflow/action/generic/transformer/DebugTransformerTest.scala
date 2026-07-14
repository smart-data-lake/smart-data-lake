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
import io.smartdatalake.testutils.plainScala.{MockScalaDataObject, ScalaTestUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

// this test combines two inputs via SQLDfsTransformer, which uses DataFrameFunctions.sql, not implemented for ScalaSubFeed
class DebugTransformerTest extends AnyFunSuite with BeforeAndAfter with DebugTransformerBehaviour {

  override def subFeedType: Type = typeOf[ScalaSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  before {
    instanceRegistry.clear()
  }

  ignore("copy load with transformer, a regular and a skipped input, skipped input is reset after decision to execute Action was made") {
    testCopyLoadWithTransformerRegularAndSkippedInput(
      id => MockScalaDataObject(id),
      id => MockScalaDataObject(id, partitions = Seq("lastname"), primaryKey = Some(Seq("lastname", "firstname")))
    )
  }
}
