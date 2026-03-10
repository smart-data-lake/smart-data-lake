/*
 * Smart Data Lake - Build your data lake the smart way.
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

import io.smartdatalake.workflow.dataframe.DataFrameFunctions
import org.scalatest.funsuite.AnyFunSuite

class ExpressionParserTest extends AnyFunSuite {

  private implicit val functions: DataFrameFunctions = ScalaSubFeed

  private def evaluate(expression: String): Any = {
    val inputCols = Seq(ScalaColumn("dummy", IndexedSeq(42)), ScalaColumn("test.a", IndexedSeq(42)))
    val column = ExpressionParser.parse(expression).asInstanceOf[ScalaAbstractColumn]
      .toScalaColumn(ScalaDataFrame(inputCols))
    column.data.head
  }

  test("respect operator precedence") {
    assert(evaluate("1 + 2 * 3") == 7)
  }

  test("respect parentheses") {
    assert(evaluate("(1 + 2) * 3") == 9)
  }

  test("support left associative arithmetic") {
    assert(evaluate("10 - 3 - 2") == 5)
  }

  test("support comparison operators") {
    assert(evaluate("1 + 2 = 3") == true)
    assert(evaluate("4 != 5") == true)

    val lessExpr = ExpressionParser.parse("2 < 3")
    assert(lessExpr.isInstanceOf[ScalaBinaryExpr])
    assert(lessExpr.asInstanceOf[ScalaBinaryExpr].opName == "lt")

    val greaterExpr = ExpressionParser.parse("3 > 2")
    assert(greaterExpr.isInstanceOf[ScalaBinaryExpr])
    assert(greaterExpr.asInstanceOf[ScalaBinaryExpr].opName == "gt")
  }

  test("support string and boolean literals") {
    assert(evaluate("'abc' = 'abc'") == true)
    assert(evaluate("true != false") == true)
  }

  test("reject unbalanced parentheses") {
    assertThrows[IllegalArgumentException](ExpressionParser.parse("(1 + 2"))
  }

  test("support generic function calls resolved via DataFrameFunctions") {
    assert(evaluate("least(2,3)") == 2)
  }

  test("support case-insensitive function names") {
    assert(evaluate("GREATEST(2,3) + 1") == 4)
  }

  test("reject incompatible function overloads") {
    assertThrows[IllegalArgumentException](ExpressionParser.parse("lit(2 + 3)"))
  }

  test("support function arguments with quotes") {
    assert(evaluate("concat('a','b')") == "ab")
  }

  test("reject unknown functions") {
    assertThrows[IllegalArgumentException](ExpressionParser.parse("does_not_exist(1)"))
  }

  test("support count(*)") {
    assert(evaluate("count(*)") == 1)
  }

  test("column reference") {
    assert(evaluate("a = 42") == true)
  }

  test("column reference with alias") {
    assert(evaluate("test.a = 42") == true)
  }
}
