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
import io.smartdatalake.testutils.WhitelistTransformerBehaviour
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

class WhitelistTransformerTest extends AnyFunSuite with WhitelistTransformerBehaviour {

  override def subFeedType: Type = typeOf[ScalaSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  test("only columns where the names match are whitelisted") {
    testOnlyMatchingColumnsWhitelisted()
  }

  test("column whitelisting is case insensitive per default") {
    testCaseInsensitiveByDefault()
  }

  test("column whitelisting is case sensitive if Environment.caseSensitive=true") {
    testCaseSensitiveIfEnvironmentCaseSensitive()
  }

  test("column whitelisting throws no error if whitelisted column has dots") {
    testNoErrorIfWhitelistedColumnHasDots()
  }
}
