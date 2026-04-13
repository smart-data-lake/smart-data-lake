/*
 * sdl-core - Build your data lake the smart way.
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

class ScalaDataFrameTest extends AnyFunSuite {

  import ScalaDataFrame.implicits._
  private val functions = ScalaSubFeed.asInstanceOf[DataFrameFunctions]
  import functions._

  //implementation was overriden, thus test needed
  // symmetric difference part of isEqual operator (part of further tests)
  test("test symmetric difference") {
    val df = ScalaDataFrame.fromData(Seq(Seq(1,2,3,"a","b","c",true), Seq(4,5,6,"gf","dgd","gsg",false)))
    val df2 = ScalaDataFrame.fromData(Seq(Seq(1,2,3,"a","b","c",false), Seq(4,5,6,"gf","dgd","gsg",true))) //last value switch
    assert(df.symmetricDifference(df).isEmpty && !df.symmetricDifference(df2).isEmpty)
  }

  test("create from Seq and rename columns") {
    val df = ScalaDataFrame.fromData(Seq(Seq(1, "A"), Seq(2, "B")))
      .withColumnRenamed("col0", "a")
      .withColumnRenamed("col1", "b")
    val df1 = df
      .withColumn("c", df("a") * df("a"))
    val df2 = ScalaDataFrame.fromData(Seq(Seq(1, "A", 1), Seq(2, "B", 4)), Seq("a", "b", "c"))
    assert(df1.isEqual(df2))
  }

  test("create from Seq with column names") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val df = data.toDF("a", "b")
    assert(df.collect.map(_.toSeq) == data.map(_.map(Option(_))))
  }

  test("add column with calculation") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1, "A", 1), Seq(2, "B", 4)).map(_.map(Option(_)))
    val df = data.toDF("a", "b")
    val df1 = df
      .withColumn("c", df("a") * df("a"))
    assert(df1.collect.map(_.toSeq) == expected)
  }

  test("drop column") {
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1), Seq(2)).map(_.map(Option(_)))
    val df = data.toDF("a", "b")
    val df1 = df
      .drop("b")
    assert(df1.collect.map(_.toSeq) == expected)
  }

  test("join other DataFrame") {
    val data1 = Seq(Seq(1, "A"), Seq(2, "B"))
    val data2 = Seq(Seq(1, "X"), Seq(3, "Y"))
    val expected = Seq(Seq(1, "A", "X")).map(_.map(Option(_)))
    val df = data1.toDF("a", "b")
      .join(data2.toDF("a", "c"), Seq("a"), "inner")
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("left join other DataFrame using column name") {
    val data1 = Seq(Seq(1, "A"), Seq(2, "B"))
    val data2 = Seq(Seq(1, "X"), Seq(3, "Y"))
    val expected = Seq(Seq(1, "A", "X"), Seq(2, "B", null)).map(_.map(Option(_)))
    val df = data1.toDF("a", "b")
      .join(data2.toDF("a", "c"), Seq("a"), "left")
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("right join other DataFrame using column name") {
    val data1 = Seq(Seq(1, "A"), Seq(2, "B"))
    val data2 = Seq(Seq(1, "X"), Seq(3, "Y"))
    val expected = Seq(Seq(1, "A", "X"), Seq(3, null, "Y")).map(_.map(Option(_)))
    val df = data1.toDF("a", "b")
      .join(data2.toDF("a", "c"), Seq("a"), "right")
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("full join other DataFrame using column name") {
    val data1 = Seq(Seq(1, "A"), Seq(2, "B"))
    val data2 = Seq(Seq(1, "X"), Seq(3, "Y"))
    val expected = Seq(Seq(1, "A", "X"), Seq(2, "B", null), Seq(3, null, "Y")).map(_.map(Option(_)))
    val df = data1.toDF("a", "b")
      .join(data2.toDF("a", "c"), Seq("a"), "full")
    assert(df.collect.map(_.toSeq) == expected)
  }


  test("left join other DataFrame using condition") {
    val data1 = Seq(Seq(1, "A"), Seq(2, "B"))
    val data2 = Seq(Seq(1, "X"), Seq(3, "Y"))
    val expected = Seq(Seq(1, "A", 1, "X"), Seq(2, "B", null, null)).map(_.map(Option(_)))
    val df = data1.toDF("a", "b").as("df1")
      .join(data2.toDF("a", "c").as("df2"), col("df1.a") === col("df2.a"), "left")
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("right join other DataFrame using condition") {
    val data1 = Seq(Seq(1, "A"), Seq(2, "B"))
    val data2 = Seq(Seq(1, "X"), Seq(3, "Y"))
    val expected = Seq(Seq(1, "A", 1, "X"), Seq(null, null, 3, "Y")).map(_.map(Option(_)))
    val df = data1.toDF("a", "b").as("df1")
      .join(data2.toDF("a", "c").as("df2"), col("df1.a") === col("df2.a"), "right")
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("full join other DataFrame using condition") {
    val data1 = Seq(Seq(1, "A"), Seq(2, "B"))
    val data2 = Seq(Seq(1, "X"), Seq(3, "Y"))
    val expected = Seq(Seq(1, "A", 1, "X"), Seq(2, "B", null, null), Seq(null, null, 3, "Y")).map(_.map(Option(_)))
    val df = data1.toDF("a", "b").as("df1")
      .join(data2.toDF("a", "c").as("df2"), col("df1.a") === col("df2.a"), "full")
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("add literal column") {
    import ScalaSubFeed._
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1, "A", -1), Seq(2, "B", -1)).map(_.map(Option(_)))
    val df = data.toDF("a", "b")
      .withColumn("c", lit(-1))
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("replace column") {
    import ScalaSubFeed._
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1, "C"), Seq(2, "C")).map(_.map(Option(_)))
    val df = data.toDF("a", "b")
      .withColumn("b", lit("C"))
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("select column with DataFrame alias") {
    import ScalaSubFeed._
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1, "A"), Seq(2, "B")).map(_.map(Option(_)))
    val df = data.toDF("a", "b")
      .as("test")
      .select(Seq(col("test.a"), col("test.b")))
    assert(df.collect.map(_.toSeq) == expected)
  }

  test("calculate with literal") {
    import ScalaSubFeed._
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1, "A", -1), Seq(2, "B", -2)).map(_.map(Option(_)))
    val df = data.toDF("a", "b")
    val df1 = df
      .withColumn("c", df("a") * lit(-1))
    assert(df1.collect.map(_.toSeq) == expected)
  }

  test("when expression with literal") {
    import ScalaSubFeed._
    val data = Seq(Seq(1, "A"), Seq(2, "B"), Seq(3, "B"))
    val expected = Seq(Seq(1, "A", -1), Seq(2, "B", 0), Seq(3, "B", 3)).map(_.map(Option(_)))
    val df = data.toDF("a", "b")
    val df1 = df
      .withColumn("c", when(df("a") === lit(1), lit(-1)).when(lit(2) === col("a"), lit(0)).otherwise(col("a")))
    assert(df1.collect.map(_.toSeq) == expected)
  }

  test("calculate with column reference") {
    import ScalaSubFeed._
    val data = Seq(Seq(1, "A"), Seq(2, "B"))
    val expected = Seq(Seq(1, "A", -1), Seq(2, "B", -2)).map(_.map(Option(_)))
    val df = data.toDF("a", "b")
    val df1 = df
      .withColumn("c", col("a") * lit(-1))
    assert(df1.collect.map(_.toSeq) == expected)
  }

  test("select works correctly") {
    val df = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
    assert(df.select("col2").isEqual(ScalaDataFrame.fromData(Seq(Seq("a"), Seq("b")), Seq("col2"))))
    assertThrows[IllegalArgumentException](df.select("col_non_existent"))
  }

  test("select star expand correctly") {
    val df = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
    val df1 = df.select("*")
    assert(df1.isEqual(df))
    val df2 = df.as("df2").select("*")
    assert(df2.symmetricDifference(df).isEmpty)
  }

  test("unionByName works as expected") {
    val df1 = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
    val df2 = ScalaDataFrame.fromData(Seq(Seq("c", 3), Seq("d", 4)), Seq("col2", "col1"))
    val df3 = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col3", "col4"))
    val expected12 = Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "c"), Seq(4, "d")).map(_.map(Option(_)))
    val df_union = df1.unionByName(df2)
    assert(df_union.collect.map(_.toSeq) == expected12)
    assertThrows[IllegalArgumentException](df1.unionByName(df3))
  }

  test ("except works as planned") {
    val df1 = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "a"), Seq(4, "c")), Seq("col1", "col2"))
    val df2 = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
    val df3 = ScalaDataFrame.fromData(Seq(Seq(3, "a"), Seq(4, "c")), Seq("col1", "col2"))
    val df_err = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col_error", "col2"))
    assert(df1.except(df2).isEqual(df3))
    assertThrows[IllegalArgumentException](df1.except(df_err))
  }

  test ("distinct works as expected") {
    val df1 = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "a"), Seq(1, "a"), Seq(1, "a"), Seq(2, "b")))
    assert(df1.distinct.dim == (3,2)) //3 rows 2 cols
  }

  test("ScalaArrayDataType stores Sequences in its cell values") {
    val df = ScalaDataFrame.fromData(Seq(Seq(Seq(1,2,3,4)), Seq(Seq(5,6,7)), Seq(Seq(8,9,10))))
    val hasCorrectType = df.schema("col0").dataType == ScalaArrayDataType(None)
    val storesCorrectData = Seq(0,1,2).forall(ix => df(ix)(0).isInstanceOf[Option[Seq[Int]]])
    assert(hasCorrectType && storesCorrectData)
  }

  test("Exploding a column with simple data types") {
    val df = ScalaDataFrame.fromData(Seq(Seq("row1", Seq(1,2,3)), Seq("row2", Seq(4,5,6))))
    val exploded_df = df.withColumn("values", explode(df("col1")))
      .drop("col1")
    val expected = Seq(Seq("row1", 1), Seq("row1", 2), Seq("row1", 3), Seq("row2", 4), Seq("row2", 5), Seq("row2", 6)).map(_.map(Option(_)))
    assert(exploded_df.rows.map(_.toSeq) == expected)
  }

  test("Aggregate DataFrame") {
    val df1 = ScalaDataFrame.fromData(
      Seq(Seq(1, "a", "test", 4), Seq(1, "a", "test1", 5), Seq(2, "b", "test1", 6), Seq(3, "b", "test2", 7)), Seq("k1", "k2", "str", "num")
    )
    val dfAgg = df1
      .agg(Seq(count(col("str")), max(col("num"))))
    val expected = Seq(Seq(4, 7)).map(_.map(Option(_)))
    assert(dfAgg.collect.map(_.toSeq) == expected)
  }

  test("GroupBy aggregate DataFrame") {
    val df1 = ScalaDataFrame.fromData(Seq(Seq(1, "a", "test", 4), Seq(1, "a", "test1", 5), Seq(2, "b", "test1", 6), Seq(3, "b", "test2", 7)), Seq("k1", "k2", "str", "num"))
    val dfAgg = df1
      .groupBy(Seq(col("k1"), col("k2")))
      .agg(Seq(count(col("str")).as("cnt"), max(col("num")).as("max_num")))
    val expected = Set(Seq(1, "a", 2, 5), Seq(2, "b", 1, 6), Seq(3, "b", 1, 7)).map(_.map(Option(_)))
    assert(dfAgg.collect.map(_.toSeq).toSet == expected)
  }

  test("Compare computed column against literal") {
    val df1 = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "a"), Seq(1, "a"), Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
    val computedCol = (df1.cols.head === lit(2)).toScalaColumn(df1)
    val expected = Seq(false, true, false, false, false, true).map(Option(_))
    assert(computedCol.data == expected)
  }

  test("Condition on empty DataFrame should not fail") {
    val df1 = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b"), Seq(3, "a"), Seq(1, "a"), Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
    val dfEmpty = df1.where(lit(false))
    val dfTest = dfEmpty.where(lit(2) === col("col1"))
    assert(dfTest.isEmpty)
  }

  test("Join DataFrame with resolved columns should not fail") {
    val df1 = ScalaDataFrame.fromData(Seq(Seq(1, "a"), Seq(2, "b")), Seq("col1", "col2"))
      .as("df1")
    val dfEmpty = df1.where(lit(false))
      .as("dfEmpty")
    val dfTest = df1.join(dfEmpty, df1("col1") === dfEmpty("col1"), "inner")
  }

  // TODO: check null values handling

}
