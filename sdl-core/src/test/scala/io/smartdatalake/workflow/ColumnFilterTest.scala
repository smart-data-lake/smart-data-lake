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
package io.smartdatalake.workflow

import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.action.executionMode.ExecutionModeResult
import io.smartdatalake.workflow.dataframe.plainScala.{ScalaColumnDefinition, ScalaSchema}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ColumnFilterTest extends AnyFlatSpec with Matchers {

  private val filterA = ColumnFilter("colA", "colA > 1")
  private val filterB = ColumnFilter("colB", "colB = 'x'")

  private def schemaOf(colNames: String*): ScalaSchema =
    ScalaSchema(colNames.map(n => ScalaColumnDefinition[String](n)).toList)

  /**
   * Run the given function with Environment.caseSensitive set, restoring the previous value afterwards.
   */
  private def withCaseSensitive[T](caseSensitive: Boolean)(fn: => T): T = {
    val previous = Environment._caseSensitive
    Environment._caseSensitive = Some(caseSensitive)
    try fn finally Environment._caseSensitive = previous
  }

  "merge" should "append a filter for a new column" in {
    ColumnFilter.merge(Seq(filterA), Seq(filterB), "test") shouldBe Seq(filterA, filterB)
  }

  it should "keep an identical filter only once" in {
    ColumnFilter.merge(Seq(filterA), Seq(filterA), "test") shouldBe Seq(filterA)
  }

  it should "replace a different filter on the same column, keeping its position" in {
    val newFilterA = filterA.copy(expression = "colA > 99")
    ColumnFilter.merge(Seq(filterA, filterB), Seq(newFilterA), "test") shouldBe Seq(newFilterA, filterB)
  }

  it should "match columns case-insensitive by default" in withCaseSensitive(false) {
    val upperFilterA = ColumnFilter("COLA", "COLA > 99")
    ColumnFilter.merge(Seq(filterA), Seq(upperFilterA), "test") shouldBe Seq(upperFilterA)
  }

  it should "match columns case-sensitive if Environment.caseSensitive is set" in withCaseSensitive(true) {
    val upperFilterA = ColumnFilter("COLA", "COLA > 99")
    ColumnFilter.merge(Seq(filterA), Seq(upperFilterA), "test") shouldBe Seq(filterA, upperFilterA)
  }

  "filterExistingColumns" should "keep only filters whose column exists" in {
    ColumnFilter.filterExistingColumns(Seq(filterA, filterB), schemaOf("colA", "colC")) shouldBe Seq(filterA)
  }

  it should "find the column case-insensitive by default" in withCaseSensitive(false) {
    ColumnFilter.filterExistingColumns(Seq(filterA), schemaOf("COLA")) shouldBe Seq(filterA)
  }

  "hasDuplicateColumns" should "detect two filters for the same column" in withCaseSensitive(false) {
    ColumnFilter.hasDuplicateColumns(Seq(filterA, filterB)) shouldBe false
    ColumnFilter.hasDuplicateColumns(Seq(filterA, filterA.copy(expression = "colA > 99"))) shouldBe true
    ColumnFilter.hasDuplicateColumns(Seq(filterA, ColumnFilter("COLA", "COLA > 99"))) shouldBe true
  }

  "mainInputOnly and propagate" should "be independent of each other" in {
    // all four combinations are representable and only mainInputOnly influences filtersForInput
    val combinations = for {
      mainInputOnly <- Seq(false, true)
      propagate <- Seq(false, true)
    } yield ColumnFilter("colA", "colA > 1", mainInputOnly, propagate)
    combinations.size shouldBe 4
    combinations.foreach { filter =>
      ExecutionModeResult(filters = Seq(filter)).filtersForInput(isMainInput = true) shouldBe Seq(filter)
      ExecutionModeResult(filters = Seq(filter)).filtersForInput(isMainInput = false) shouldBe
        (if (filter.mainInputOnly) Seq() else Seq(filter))
    }
  }

  "ExecutionModeResult.filtersForInput" should "return mainInputOnly filters for the main input only" in {
    val mainInputOnlyFilter = filterA.copy(mainInputOnly = true)
    val result = ExecutionModeResult(filters = Seq(mainInputOnlyFilter, filterB))
    result.filtersForInput(isMainInput = true) shouldBe Seq(mainInputOnlyFilter, filterB)
    result.filtersForInput(isMainInput = false) shouldBe Seq(filterB)
  }

  "ExecutionModeResult" should "reject more than one filter per column" in withCaseSensitive(false) {
    an[IllegalArgumentException] should be thrownBy
      ExecutionModeResult(filters = Seq(filterA, filterA.copy(expression = "colA > 99")))
    an[IllegalArgumentException] should be thrownBy
      ExecutionModeResult(filters = Seq(filterA, ColumnFilter("COLA", "COLA > 99")))
    // one filter per column is fine
    ExecutionModeResult(filters = Seq(filterA, filterB)).filters.size shouldBe 2
  }

  "toString" should "describe the flags which are set" in {
    filterA.toString shouldBe "colA: colA > 1"
    filterA.copy(mainInputOnly = true).toString shouldBe "colA: colA > 1 (mainInputOnly)"
    filterA.copy(propagate = true).toString shouldBe "colA: colA > 1 (propagate)"
    filterA.copy(mainInputOnly = true, propagate = true).toString shouldBe "colA: colA > 1 (mainInputOnly, propagate)"
  }
}
