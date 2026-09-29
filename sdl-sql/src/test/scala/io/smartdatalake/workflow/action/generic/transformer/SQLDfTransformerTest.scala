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

import io.smartdatalake.definitions.Environment
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.testutils.SQLDfTransformerBehaviour
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.util.python.JepInterpreter
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.SQLEngineConnection
import io.smartdatalake.workflow.dataframe.sql.SQLSubFeed
import org.scalatest.Outcome
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

class SQLDfTransformerTest extends AnyFunSuite with SQLDfTransformerBehaviour {

  override def subFeedType: Type = typeOf[SQLSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  instanceRegistry.register(SQLEngineConnection(ConnectionId(Environment.defaultEngineConnectionId), dialect = "postgres", sqlDialect = Some("spark")))
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  override def withFixture(test: NoArgTest): Outcome = {
    val reason = JepInterpreter.unavailableReason
    assume(reason.isEmpty, reason.getOrElse(""))
    super.withFixture(test)
  }

  test("options and view name token are replaced") {
    testOptionsAndViewNameTokenAreReplaced()
  }

  test("view name token without input name is replaced") {
    testViewNameTokenWithoutInputNameIsReplaced()
  }

  test("legacy view name without postfix is still supported") {
    testLegacyViewNameWithoutPostfixIsStillSupported()
  }

}
