/*
 * sdl-core - Build your data lake the smart way.
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
package io.smartdatalake.util.misc

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.action.executionMode.DefaultExecutionModeExpressionData
import org.scalatest.funsuite.AnyFunSuite

class ExpressionUtilTest extends AnyFunSuite {

  private implicit val registry: InstanceRegistry = new InstanceRegistry
  private val context = TestUtil.getDefaultActionPipelineContext
  private val data: DefaultExecutionModeExpressionData = DefaultExecutionModeExpressionData.from(context)

  test("evaluate boolean") {
    val result = ExpressionUtil.evaluateBoolean(id = DataObjectId("test"), configName = Some("testCondition"), expression = "runId + attemptId = 2", data = data)
    // result should be true
    assert(result)
  }

  test("evaluate string") {
    val result = ExpressionUtil.evaluateString(DataObjectId("test"), Some("testCondition"), "concat(feed, '-', application)", data)
    assert(result.contains("feedTest-appTest"))
  }

  test("substitute tokens") {
    val result = ExpressionUtil.substitute(DataObjectId("test"), Some("testCondition"), "hello %{concat(feed, '-', application)}, lets make %{runId + attemptId}", data)
    assert(result.contains("hello feedTest-appTest, lets make 2"))
  }

  test("substitute options") {
    val result = ExpressionUtil.substituteOptions(DataObjectId("test"), Some("testCondition"), "hello %{key1}, lets make %{key2}", Map("key1" -> "tester", "key2" -> "tests"))
    assert(result.contains("hello tester, lets make tests"))
  }
}
