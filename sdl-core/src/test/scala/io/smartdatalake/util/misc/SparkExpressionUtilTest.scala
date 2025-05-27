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
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.spark.SparkExpressionUtil
import io.smartdatalake.workflow.action.executionMode.DefaultExecutionModeExpressionData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.custom.ExpressionEvaluator
import org.apache.spark.sql.functions.{col, min, udf}
import org.scalatest.FunSuite

class SparkExpressionUtilTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.session

  private implicit val registry: InstanceRegistry = new InstanceRegistry
  private val context = TestUtil.getDefaultActionPipelineContext
  private val data: DefaultExecutionModeExpressionData = DefaultExecutionModeExpressionData.from(context)

  case class MyData(a: Double, b: Double, c: Double, d: Double, x: Double)

  test("evaluate dataFrame equations") {
    import session.implicits._
    val df = List(
      ("a+b*x+c*x*x+d*x*x*x", 2d, 3d, 4d, 5d, 10d, 5432d),
      ("a+b*x+c*x*x+d*log10(x)", 2d, 3d, 4d, 5d, 10d, 437d)
    ).toDF("eqn", "a", "b", "c", "d", "x", "expected")
    val evalUdf = udf((eqn: String, a: Double, b: Double, c: Double, d: Double, x: Double) => {
      SparkExpressionUtil.evaluate[MyData, Double](
        id = DataObjectId("Why do we need this parameter?"),
        configName = Some("Why do we need this parameter?"),
        expression = eqn,
        data = MyData(a, b, c, d, x))
    })
    val resultDf = df.withColumn("actual", evalUdf($"eqn", $"a", $"b", $"c", $"d", $"x"))
    val result = resultDf.agg(min($"actual" === $"expected")).as[Boolean].head
    if (!result) {
      println(s"Test Failed for the following rows:")
      resultDf.where($"actual" =!= $"expected")
        .orderBy(resultDf.columns.map(col): _*).show(false)
    }
    assert(result)
  }

  test("evaluate boolean") {
    val result = SparkExpressionUtil.evaluateBoolean(id = DataObjectId("test"), configName = Some("testCondition"), expression = "runId + attemptId = 2", data = data)
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
    val result = SparkExpressionUtil.substituteOptions(DataObjectId("test"), Some("testCondition"), "hello %{key1}, lets make %{key2}", Map("key1" -> "tester", "key2" -> "tests"))
    assert(result.contains("hello tester, lets make tests"))
  }

  test("register & apply udf") {
    ExpressionEvaluator.registerUdf("udfAdd1", udf((v: Int) => v + 1))
    val result = SparkExpressionUtil.evaluate[DefaultExecutionModeExpressionData, Int](DataObjectId("test"), Some("testCondition"), "udfAdd1(runId)", data)
    assert(result.contains(2))
  }
}
