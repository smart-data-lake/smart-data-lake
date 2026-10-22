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

import org.scalatest.funsuite.AnyFunSuite

class ScalaAbstractColumnTest extends AnyFunSuite {

  import ScalaDataFrame.implicits._
  import ScalaSubFeed._

  test("add literal column") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Some(2), Some(3))
    val df = data.toDF("a", "b")
    val colN = (lit(1) + ScalaColumnReference("a")).toScalaColumn(df)
    assert(colN.data == expected)
    assert(colN.getName.exists(_.startsWith("col")))
  }

  test("add literal column with different datatypes") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Some(2d), Some(3d))
    val df = data.toDF("a", "b")
    val colN = (lit(1d) + ScalaColumnReference("a")).toScalaColumn(df)
    assert(colN.data == expected)
    assert(colN.getName.exists(_.startsWith("col")))
  }

  test("add literal column with different datatypes continued") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Some(2d), Some(3d))
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("a") + lit(1d)).toScalaColumn(df)
    assert(colN.data == expected)
    assert(colN.getName.exists(_.startsWith("col")))
  }

  test("add named expression") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Some(2d), Some(3d))
    val df = data.toDF("a", "b")
    val colC = (ScalaNamedExpr(lit(1d) + ScalaColumnReference("a"), "c")).toScalaColumn(df)
    assert(colC.data == expected)
    assert(colC.getName.contains("c"))
  }

  test("integer div expression") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Some(0), Some(1))
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("a") / lit(2)).toScalaColumn(df)
    assert(colN.data == expected)
  }

  test("double div expression") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Some(0.5d), Some(1d))
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("a") / lit(2d)).toScalaColumn(df)
    assert(colN.data == expected)
  }

  test("and expression") {
    val data = Seq(Seq(1, true), Seq(2, false))
    val expected = Seq(Some(true), Some(false))
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("b") and lit(true)).toScalaColumn(df)
    assert(colN.data == expected)
  }

  test("equal expression") {
    val data = Seq(Seq(1, true), Seq(2, false))
    val expected = Seq(Some(true), Some(false))
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("a") === lit(1)).toScalaColumn(df)
    assert(colN.data == expected)
  }

  test("unary expression (not)") {
    val data = Seq(Seq(1, true), Seq(2, false))
    val expected = Seq(Some(true), Some(true))
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("a").isNotNull).toScalaColumn(df)
    assert(colN.data == expected)
  }

  test("a binary expression reads the data of each operand only once") {
    val left = new CountingColumn("a")
    val right = new CountingColumn("b")
    assert(concat(left, right).data == Seq(Some("ab")))
    assert((left.reads, right.reads) == (1, 1))
  }

  test("a many argument expression reads the data of each argument only once") {
    val columns = Seq(new CountingColumn("a"), new CountingColumn("b"))
    assert(array(columns: _*).data == Seq(Some(Seq("a", "b"))))
    assert(columns.map(_.reads) == Seq(1, 1))
  }

  test("a map expression reads the data of each key and value only once") {
    val columns = Seq(new CountingColumn("k"), new CountingColumn("v"))
    assert(map(columns: _*).data == Seq(Some(Map("k" -> "v"))))
    assert(columns.map(_.reads) == Seq(1, 1))
  }

  test("a deeply nested expression is evaluated in linear time") {
    val df = Seq(Seq("a")).toDF("str")
    // functions like concat, least and greatest build a chain of binary expressions. As every operand is read
    // exactly once, evaluating a chain of n operations costs O(n) - reading an operand twice would cost
    // O(2^n), and this test would not terminate.
    val deepConcat = concat(ScalaColumnReference("str") +: (1 to 60).map(i => lit(i.toString)): _*)
    assert(deepConcat.toScalaColumn(df).data == Seq(Some("a" + (1 to 60).mkString)))
  }

  /**
   * A column counting how often its data was read, to check that an expression evaluates its inputs only once.
   */
  private class CountingColumn(value: String) extends ScalaAbstractColumn {
    var reads = 0

    override def dataType: ScalaDataType[_] = ScalaStringDataType

    override def data: Seq[Option[_]] = {
      reads += 1
      Seq(Some(value))
    }
  }

}
