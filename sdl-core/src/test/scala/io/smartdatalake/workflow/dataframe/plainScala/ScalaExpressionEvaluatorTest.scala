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
package io.smartdatalake.workflow.dataframe.plainScala

import io.smartdatalake.workflow.dataobject.generic.PartitionExpressionData
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

/**
 * Test [[ScalaExpressionEvaluator]] with the housekeeping expressions of
 * [[io.smartdatalake.workflow.dataobject.generic.HousekeepingMode]], as the plain-Scala engine is used
 * to evaluate them if there is no Spark expression library in the classpath, e.g. with Spark Connect.
 */
class ScalaExpressionEvaluatorTest extends AnyFunSuite {

  private val data = PartitionExpressionData("feed1", "app1", 2, Timestamp.valueOf("2020-12-05 10:00:00"), "tgt1",
    Map("dt" -> "20201101"))

  private def evaluate[R: scala.reflect.ClassTag : scala.reflect.runtime.universe.TypeTag](expression: String): R = {
    ScalaExpressionEvaluatorFactory.getEvaluator[PartitionExpressionData, R](expression).apply(data)
  }

  test("evaluate simple attribute of case class") {
    assert(evaluate[String]("dataObjectId") == "tgt1")
    assert(evaluate[Boolean]("runId > 1"))
  }

  test("evaluate retention condition on partition elements") {
    assert(!evaluate[Boolean]("elements.dt >= 20201201"))
    assert(evaluate[Boolean]("elements['dt'] >= 20201101"))
  }

  test("evaluate archive partition expression returning a map") {
    assert(evaluate[Map[String, String]]("map('dt','20201101')") == Map("dt" -> "20201101"))
    assert(evaluate[Map[String, String]]("map('dt', elements.dt)") == Map("dt" -> "20201101"))
  }

  test("fail on unknown attribute") {
    val evaluator = ScalaExpressionEvaluatorFactory.getEvaluator[PartitionExpressionData, String]("unknown")
    assertThrows[ColumnNotFoundException](evaluator(data))
  }
}
