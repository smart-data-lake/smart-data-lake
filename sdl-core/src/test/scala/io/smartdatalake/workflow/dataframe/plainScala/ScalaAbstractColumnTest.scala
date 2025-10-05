/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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

import org.scalatest.FunSuite

class ScalaAbstractColumnTest extends FunSuite {

  import ScalaDataFrame.implicits._

  test("add literal column") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(2, 3)
    val df = data.toDF("a", "b")
    val colN = (ScalaLiteral(1) + ScalaColumnReference("a")).toScalaColumn(df)
    assert(colN.data == expected)
    assert(colN.getName.exists(_.startsWith("col")))
  }

  test("add literal column with different datatypes") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(2d, 3d)
    val df = data.toDF("a", "b")
    val colN = (ScalaLiteral(1d) + ScalaColumnReference("a")).toScalaColumn(df)
    assert(colN.data == expected)
    assert(colN.getName.exists(_.startsWith("col")))
  }

  test("add literal column with different datatypes continued") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(2d, 3d)
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("a") + ScalaLiteral(1d)).toScalaColumn(df)
    assert(colN.data == expected)
    assert(colN.getName.exists(_.startsWith("col")))
  }

  test("add named expression") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(2d, 3d)
    val df = data.toDF("a", "b")
    val colC = (ScalaNamedExpr(ScalaLiteral(1d) + ScalaColumnReference("a"), "c")).toScalaColumn(df)
    assert(colC.data == expected)
    assert(colC.getName.contains("c"))
  }

  test("integer div expression") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(0, 1)
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("a") / ScalaLiteral(2)).toScalaColumn(df)
    assert(colN.data == expected)
  }

  test("double div expression") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(0.5d, 1d)
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("a") / ScalaLiteral(2d)).toScalaColumn(df)
    assert(colN.data == expected)
  }

  test("and expression") {
    val data = Seq(Seq(1, true), Seq(2, false))
    val expected = Seq(true, false)
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("b") and ScalaLiteral(true)).toScalaColumn(df)
    assert(colN.data == expected)
  }

  test("equal expression") {
    val data = Seq(Seq(1, true), Seq(2, false))
    val expected = Seq(true, false)
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("a") === ScalaLiteral(1)).toScalaColumn(df)
    assert(colN.data == expected)
  }

  test("unary expression (not)") {
    val data = Seq(Seq(1, true), Seq(2, false))
    val expected = Seq(true, true)
    val df = data.toDF("a", "b")
    val colN = (ScalaColumnReference("a").isNotNull).toScalaColumn(df)
    assert(colN.data == expected)
  }

}
