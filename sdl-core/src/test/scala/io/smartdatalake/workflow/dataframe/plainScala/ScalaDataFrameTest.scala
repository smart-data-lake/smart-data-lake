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

class ScalaDataFrameTest extends FunSuite {

  import ScalaDataFrame.implicits._

  test("create from Seq and rename columns") {
    val df = ScalaDataFrame.apply(Seq(Seq(1, "A"), Seq(2, "B")))
      .withColumnRenamed("col0", "a")
      .withColumnRenamed("col1", "b")
    val df1 = df
      .withColumn("c", df("a") * df("a"))
    println(df1.showString())
  }

  test("create from Seq with column names") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val df = data.toDF("a", "b")
    assert(df.collect.map(_.toSeq) == data)
  }

  test("add column with calculation") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1, "A", 1), Seq(2, "B", 4))
    val df = data.toDF("a", "b")
    val df1 = df
      .withColumn("c", df("a") * df("a"))
    assert(df1.collect.map(_.toSeq) == expected)
  }

  test("drop column") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1), Seq(2))
    val df = data.toDF("a", "b")
    val df1 = df
      .drop("b")
    assert(df1.collect.map(_.toSeq) == expected)
  }

  test("join other DataFrame") {
    val data1 = Seq(Seq(1, "A"), Seq(2, "B"))
    val data2 = Seq(Seq(1, "X"), Seq(3, "Y"))
    val expected = Seq(Seq(1, "A", "X"))
    val df = data1.toDF("a", "b")
      .join(data2.toDF("a", "c"), Seq("a"), "inner")
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("add literal column") {
    import ScalaSubFeed._
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1, "A", -1), Seq(2, "B", -1))
    val df = data.toDF("a", "b")
      .withColumn("c", lit(-1))
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("calculate with literal") {
    import ScalaSubFeed._
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1, "A", -1), Seq(2, "B", -2))
    val df = data.toDF("a", "b")
    val df1 = df
      .withColumn("c", df("a") * lit(-1))
    assert(df1.collect.map(_.toSeq) == expected)
  }

  test("calculate with column reference") {
    import ScalaSubFeed._
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1, "A", -1), Seq(2, "B", -2))
    val df = data.toDF("a", "b")
    val df1 = df
      .withColumn("c", col("a") * lit(-1))
    assert(df1.collect.map(_.toSeq) == expected)
  }


}
