/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.definitions.DefaultExecutionModeExpressionData
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.spark.SparkExpressionUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.custom.ExpressionEvaluator
import org.apache.spark.sql.functions.udf
import org.scalatest.FunSuite

class SparkExpressionUtilTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog

  private implicit val registry: InstanceRegistry = new InstanceRegistry
  private val context = TestUtil.getDefaultActionPipelineContext
  private val data = DefaultExecutionModeExpressionData.from(context)

  test("evaluate boolean") {
    val result = SparkExpressionUtil.evaluateBoolean(DataObjectId("test"), Some("testCondition"), "runId + attemptId = 2", data)
    // result should be true
    assert(result)
  }

  test("evaluate string") {
    val result = SparkExpressionUtil.evaluateString(DataObjectId("test"), Some("testCondition"), "concat(feed, '-', application)", data)
    assert(result.contains("feedTest-appTest"))
  }

  test("substitute tokens") {
    val result = SparkExpressionUtil.substitute(DataObjectId("test"), Some("testCondition"), "hello %{concat(feed, '-', application)}, lets make %{runId + attemptId}", data)
    assert(result.contains("hello feedTest-appTest, lets make 2"))
  }

  test("substitute options") {
    val result = SparkExpressionUtil.substituteOptions(DataObjectId("test"), Some("testCondition"), "hello %{key1}, lets make %{key2}", Map("key1"->"tester", "key2"->"tests"))
    assert(result.contains("hello tester, lets make tests"))
  }

  test("register & apply udf") {
    ExpressionEvaluator.registerUdf("udfAdd1", udf((v: Int) => v + 1))
    val result = SparkExpressionUtil.evaluate[DefaultExecutionModeExpressionData,Int](DataObjectId("test"), Some("testCondition"), "udfAdd1(runId)", data)
    assert(result.contains(2))
  }
}
