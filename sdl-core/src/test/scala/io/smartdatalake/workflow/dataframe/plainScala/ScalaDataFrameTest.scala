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

import org.scalatest.funsuite.AnyFunSuite

class ScalaDataFrameTest extends AnyFunSuite {

  import ScalaDataFrame.implicits._

  //implementation was overriden, thus test needed
  // symmetric difference part of isEqual operator (part of further tests)
  test("test symmetric difference") {
    val df = ScalaDataFrame.apply(Seq(Seq(1,2,3,"a","b","c",true), Seq(4,5,6,"gf","dgd","gsg",false)))
    val df2 = ScalaDataFrame(Seq(Seq(1,2,3,"a","b","c",false), Seq(4,5,6,"gf","dgd","gsg",true))) //last value switch
    assert(df.symmetricDifference(df).isEmpty && !df.symmetricDifference(df2).isEmpty)
  }

  test("create from Seq and rename columns") {
    val df = ScalaDataFrame.apply(Seq(Seq(1, "A"), Seq(2, "B")))
      .withColumnRenamed("col0", "a")
      .withColumnRenamed("col1", "b")
    val df1 = df
      .withColumn("c", df("a") * df("a"))
    val df2 = ScalaDataFrame.apply(Seq(Seq(1, "A", 1), Seq(2, "B", 4)), Seq("a", "b", "c"))
    assert(df1.isEqual(df2))
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

  test("select works correctly") {
    val df = ScalaDataFrame(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
    assert(df.select("col2").isEqual(ScalaDataFrame(Seq(Seq("a"), Seq("b")), Seq("col2"))))
    assertThrows[IllegalArgumentException](df.select("col_non_existent"))
  }

  test("groupby is not implemented yet") {
    val df = ScalaDataFrame(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
    assertThrows[NotImplementedError](df.groupBy(df.cols))
  }

  test("unionByName works as expected") {
    def combine(a:Any, b: Any) = {
      (a,b) match {
        case (x: String, y: String) => x + y
        case (x: Int, y: Int) => x + y
        case _ => throw new IllegalArgumentException
      }
    }
    val df1 = ScalaDataFrame(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
    val df2 = ScalaDataFrame(Seq(Seq("c", 3), Seq("d", 4)), Seq("col2", "col1"))
    val df3 = ScalaDataFrame(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col3", "col4"))
    val df_union = df1.unionByName(df2)
    assert(df_union.cols.map(_.data.reduce(combine)) == Seq(10, "abcd"))
    assertThrows[IllegalArgumentException](df1.unionByName(df3))
  }

  test ("except works as planned") {
    val df1 = ScalaDataFrame(Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "a"), Seq(4, "c")), Seq("col1", "col2"))
    val df2 = ScalaDataFrame(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
    val df3 = ScalaDataFrame(Seq(Seq(3, "a"), Seq(4, "c")), Seq("col1", "col2"))
    val df_err = ScalaDataFrame(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col_error", "col2"))
    assert(df1.except(df2).isEqual(df3))
    assertThrows[IllegalArgumentException](df1.except(df_err))
  }

  test ("distinct works as expected") {
    val df1 = ScalaDataFrame(Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "a"), Seq(1, "a"), Seq(1, "a"), Seq(2, "b")))
    assert(df1.distinct.dim == (3,2)) //3 rows 2 cols
  }

  test("ScalaSequenceDataType stores Sequences in its cell values") {
    val df = ScalaDataFrame.apply(Seq(Seq(Seq(1,2,3,4)), Seq(Seq(5,6,7)), Seq(Seq(8,9,10))))
    val hasCorrectType = df.schema("col0").dataType == ScalaSeqDataType
    val storesCorrectData = Seq(0,1,2).forall(ix => df(ix)(0).isInstanceOf[Seq[Int]])
    assert(hasCorrectType && storesCorrectData)
  }


  test("Exploding a column with simple data types") {
    val df = ScalaDataFrame.apply(Seq(Seq("row1", Seq(1,2,3)), Seq("row2", Seq(4,5,6))))
    val exploded_df = df.withColumn("values", ScalaSubFeed.explode(df("col1")))
    val expected_pairs = Seq(("row1", 1), ("row1", 2), ("row1", 3),("row2", 4), ("row2", 5), ("row2", 6))
    assert(exploded_df.drop("col1").rows.map(row => (row.values(0), row.values(1))) == expected_pairs)
  }


  // TODO: check null values handling

}
