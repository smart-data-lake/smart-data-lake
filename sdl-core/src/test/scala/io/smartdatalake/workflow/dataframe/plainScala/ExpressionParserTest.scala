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

import java.sql.Timestamp
import java.time.LocalDate

class ExpressionParserTest extends AnyFunSuite {

  private implicit val functions: DataFrameFunctions = ScalaSubFeed

  private def evaluate(expression: String): Any = {
    val inputCols = Seq(ScalaColumn("dummy", IndexedSeq(42)), ScalaColumn("test.a", IndexedSeq(42)))
    val column = ExpressionParser.parse(expression).asInstanceOf[ScalaAbstractColumn]
    val scalaColumn = column.toScalaColumn(ScalaDataFrame(inputCols))
    scalaColumn.data.head
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

  test("support logical and or") {
    assert(evaluate("1 = 1 and 2 = 2") == true)
    assert(evaluate("1 = 1 and 2 = 3") == false)
    assert(evaluate("1 = 1 or 2 = 3") == true)
  }

  test("respect logical precedence") {
    assert(evaluate("1 = 2 or 2 = 2 and 3 = 3") == true)
    assert(evaluate("1 = 1 or 2 = 2 and 3 = 4") == true)
  }

  test("support logical precedence override by parentheses") {
    assert(evaluate("(1 = 2 or 2 = 2) and 3 = 4") == false)
  }

  test("support case-insensitive logical operators") {
    assert(evaluate("1 = 1 AND 2 = 2 oR 3 = 4") == true)
  }

  test("support between comparison") {
    assert(evaluate("2 between 1 and 3") == true)
    assert(evaluate("4 between 1 and 3") == false)
    assert(evaluate("a between 40 and 42") == true)
  }

  test("support case-insensitive between keyword") {
    assert(evaluate("2 BeTwEeN 1 aNd 3") == true)
  }

  test("respect precedence between between and logical operators") {
    assert(evaluate("1 = 2 or 2 between 1 and 3 and 3 = 3") == true)
  }

  test("support timestamp literal") {
    val value = evaluate("timestamp'2026-03-11 21:25:04.6820765'")
    assert(value.isInstanceOf[Timestamp])
    assert(value == Timestamp.valueOf("2026-03-11 21:25:04.6820765"))
    assert(evaluate("timestamp'2026-03-11 21:25:04.6820765' = timestamp'2026-03-11 21:25:04.6820765'") == true)
    assert(evaluate("timestamp'2026-03-11 21:25:04.6820765' = timestamp'2025-01-01 21:25:04'") == false)
  }

  test("support date literal") {
    val value = evaluate("date'2026-03-11'")
    assert(value.isInstanceOf[Timestamp])
    assert(value == Timestamp.valueOf(LocalDate.parse("2026-03-11").atStartOfDay()))
    assert(evaluate("date'2026-03-11' = date'2026-03-11'") == true)
    assert(evaluate("date'2026-03-11' = date'2025-01-01'") == false)
  }

  test("support case-insensitive typed literal prefix") {
    assert(evaluate("DATE'2026-03-11' = date'2026-03-11'") == true)
    assert(evaluate("TIMESTAMP'2026-03-11 21:25:04.6820765' = timestamp'2026-03-11 21:25:04.6820765'") == true)
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

  test("reject invalid logical syntax") {
    assertThrows[IllegalArgumentException](ExpressionParser.parse("1 = 1 and"))
  }

  test("reject invalid between syntax") {
    assertThrows[IllegalArgumentException](ExpressionParser.parse("2 between 1"))
    assertThrows[IllegalArgumentException](ExpressionParser.parse("2 between and 3"))
  }

  test("reject invalid typed literals") {
    assertThrows[IllegalArgumentException](ExpressionParser.parse("date'2026-13-11'"))
    assertThrows[IllegalArgumentException](ExpressionParser.parse("timestamp'2026-03-11'"))
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
