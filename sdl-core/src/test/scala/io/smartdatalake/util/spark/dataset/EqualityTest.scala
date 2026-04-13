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
package io.smartdatalake.util.spark.dataset

import io.smartdatalake.testutils.spark.dataset.Collection._
import io.smartdatalake.testutils.{TestTool, TestUtil}
import io.smartdatalake.util.PrecisionDef
import io.smartdatalake.util.spark.GetSession.loggEnv
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class EqualityTest extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks
  with TestTool with Equality {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private implicit val spark: SparkSession = TestUtil.session

  import spark.implicits._

  loggEnv

  "getSymmetricDifference" should "return empty df if generated df is compared with itself" in {
    forAll(genA = genExactFrame) { df =>
      df.getSymmetricDifference(that = df).isEmpty should be(true)
    }
  }

  "hasAlmostEqualRows" should "return true if generated dataFrame is compared with itself" in {
    forAll(genA = genInexactFrame) { df =>
      df.hasAlmostEqualRows(that = df,
        precisionMap = Map("x" -> PrecisionDef()),
        showDiff = true,
        pk = List("id")) should be(true)
    }
  }

  "DataFrames almost equals" should "if dataFrame contains MapType still work" in {
    dfMap.almostEqual(dfMap) shouldBe true
  }

  "DataFrames almost equals" should "fail when difference in integer column" in {
    dsSimple1.almostEqual(dsSimple2, precision = 10.0) shouldBe false
  }

  "DataFrames almost equals" should "pass when integer column is compared imprecisely" in {
    val pdef = PrecisionDef(precision = 10.0, relThreshold = None)
    val precMap = Map("n" -> pdef, "x" -> pdef, "y" -> pdef)
    dsSimple1.almostEqual(dsSimple2, precisionMap = precMap,
      ignoreColumnOrder = true, ignoreNullability = true, showDiff = true, pk = Nil) shouldBe true
  }

  "DataFrames almost equals" should "fail when strict difference equals precision" in {
    dsSimple1.almostEqual(dsSimple2, precision = 0.1) shouldBe false
  }

  "DataFrames almost equals" should "pass when instrict difference equals precision" in {
    dsSimple1.drop("n").almostEqual(
      dsSimple2.drop("n"),
      precision = 0.1,
      relThreshold = None,
      strict = false) shouldBe true
  }

  "DataFrames almost equals" should "pass when difference smaller than precision" in {
    dsSimple1.drop("n").almostEqual(dsSimple2.drop("n"), precision = 0.2, relThreshold = None) shouldBe true
  }

  "DataFrames almost equals" should "fail when difference greater than precision" in {
    val ds2 = List(("A", 2, 0.1f, 9.7d)).toDF("id", "n", "x", "y").as[(String, Int, Float, Double)]
    dsSimple1.almostEqual(ds2, precision = 0.2) shouldBe false
  }

  "DataFrames almost equals" should "pass when dataFrame compared to itself instrictly with precision 0.0" in {
    dsSimple1.almostEqual(dsSimple1, precision = 0.0, strict = false) shouldBe true
  }

  "DataFrames almost equals" should "fail when dataFrame compared to itself strictly with precision 0.0" in {
    dsSimple1.almostEqual(dsSimple1, precision = 0.0) shouldBe false
  }

  "DataFrames almost equals" should "fail when dataFrame compared to itself with negative precision even instrictly" in {
    dsSimple1.almostEqual(dsSimple1, precision = -1.0, strict = false) shouldBe false
  }

  "DataFrames almost equals" should "pass even if dfs contains nulls" in {
    dsNull.almostEqual(dsNull) shouldBe true
  }

  "DataFrames almost equals" should "ignore column order by default" in {
    val dfReversed = dsSimple1.select(dsSimple1.columns.reverse.map(col): _*)
    dfSimple1.almostEqual(dfReversed) shouldBe true
  }

  "DataFrames almost equals" should "consider column order when asked" in {
    dfSimple1.almostEqual(dsSimple1.select(dsSimple1.columns.reverse.map(col): _*), ignoreColumnOrder = false) shouldBe false
  }

  "DataFrames almost equals" should "fail when some cols are compared precisely" in {
    dsSimple1.almostEqual(dsSimple2,
      precisionMap = Map("y" -> PrecisionDef(precision = 0.2, relThreshold = None)),
      ignoreColumnOrder = true, ignoreNullability = true, showDiff = true, pk = Nil) shouldBe false
  }

  "DataFrames almost equals" should "fail when differences in string column" in {
    val df2 = List(("B", 0.0, 10.0)).toDF("id", "x", "y")
    dfSimple1.almostEqual(df2) shouldBe false
  }

  "DataFrames almost equals" should "pass even when impreciseness hidden in a struct" in {
    val nestedStruct: StructType = StructType(Array(
      StructField(name = "msg", dataType = StringType, nullable = false),
      StructField(name = "i", dataType = DoubleType, nullable = false),
      StructField(name = "e", dataType = DoubleType, nullable = false)
    ))
    val schema: StructType = StructType(Array(
      StructField(name = "id", dataType = StringType, nullable = false),
      StructField(name = "x", dataType = DoubleType, nullable = false),
      StructField(name = "y", dataType = nestedStruct, nullable = false)
    ))
    val dfL: DataFrame = spark.createDataFrame(ArrayBuffer(Row("A", 10.0, Row("hello world", 2.0, 3.0))).asJava, schema)
    val dfR: DataFrame = spark.createDataFrame(ArrayBuffer(Row("A", 9.9, Row("hello world", 1.9, 3.1))).asJava, schema)

    dfL.almostEqual(dfR, precision = 0.2) shouldBe true
  }

  "DataFrames almost equals" should "pass for big values relatively and small values absolutely compared" in {
    val dfL = List(("A", 0.03D), ("B", 1000000D)).toDF("id", "x")
    val dfR = List(("A", 0.031D), ("B", 1001000D)).toDF("id", "x")
    dfL.almostEqual(dfR, precision = 0.02, relThreshold = Some(0.1)) shouldBe true
  }

  "DataFrames almost equals" should "fail for big and small values relatively compared" in {
    val dfL = List(("A", 0.03D), ("B", 1000000D)).toDF("id", "x")
    val dfR = List(("A", 0.031D), ("B", 1001000D)).toDF("id", "x")
    dfL.almostEqual(dfR, precision = 0.002, relThreshold = Some(0.1)) shouldBe false
  }

  "DataFrames almost equals" should "pass for dataFrames with small numbers when 6.25% difference allowed" in {
    val dfL = List(
      (1L, 1e-6D),
      (2L, 1e-6D)
    ).toDF("id", "x")
    val dfR = List(
      (1L, 1e-6D),
      (2L, 1e-7D)
    ).toDF("id", "x")

    dfL.almostEqual(dfR, precision = Math.scalb(1, -4)) shouldBe true
  }

  "DataFrames almost equals" should "pass even when doubles and floats are not numbers" in {
    val df: DataFrame = List(
      ("NaN", Float.NaN, Double.NaN),
      ("NegativeInfinity", Float.NegativeInfinity, Double.NegativeInfinity),
      ("PositiveInfinity", Float.PositiveInfinity, Double.PositiveInfinity)
    ).toDF("id", "f", "d")
    df.almostEqual(df) shouldBe true
  }

  "DataFrames almost equals" should "work with map columns" in {
    dfMap.almostEqual(dfMap2, relThreshold = Some(1d)) shouldBe true
  }

  "DataFrames almost equals" should "pass for real life example of DMDs FraktalAnalyse with default prescision" in {
    val dfL = List(("S_20160606162715",
      "ver",
      620155,
      -1.3828676232433006E-7,
      5.176815229947798,
      0.9530563708504358,
      -3.142413505843923E-7,
      5.176815897482991,
      0.5470147770732602,
      -1.9582389726266914E-7,
      5.176815403626741,
      0.942300960030822,
      -1.8625775691366671E-7,
      5.176815319229829,
      0.9634711459950145,
      -3.3769995876960966E-7,
      5.176816001623375,
      0.852070955856326,
      -2.2051084753807298E-7,
      5.176815405141741,
      0.9420750111091075,
      20160606)
    ).toDF("section", "v", "id",
      "x_0", "x_1", "x_2", "x_3", "x_4", "x_5", "x_6", "x_7", "x_8", "x_9",
      "x_10", "x_11", "x_12", "x_13", "x_14", "x_15", "x_16", "x_17", "x_18")

    val dfR = List(("S_20160606162715",
      "ver",
      620155,
      -1.3828676232433006E-7,
      5.176815229947798,
      0.9530563708504358,
      -3.142413505843923E-7,
      5.176815897482991,
      0.5470147770732602,
      -1.9582389726266914E-7,
      5.176815403626741,
      0.942300960030822,
      -1.8625775691366671E-7,
      5.176815319229829,
      0.9634711459950145,
      -3.3769995876960966E-7,
      5.176816001623375,
      0.852070955856326,
      -2.2051084765619643E-7,
      5.176815405141742,
      0.9420750099442966,
      20160606)
    ).toDF("section", "v", "id",
      "x_0", "x_1", "x_2", "x_3", "x_4", "x_5", "x_6", "x_7", "x_8", "x_9",
      "x_10", "x_11", "x_12", "x_13", "x_14", "x_15", "x_16", "x_17", "x_18")

    dfL.almostEqual(dfR) shouldBe true
  }

  "DataFrames almost equals" should "respect PK and thus fail" in {
    val dfL = List(("A", 1, 1d), ("B", 2, 2d)).toDF("id", "n", "x")
    val dfR = List(("A", 1, 2d), ("B", 2, 1d)).toDF("id", "n", "x")
    dfL.almostEqual(that = dfR, pk = List("id")) shouldBe false
  }

  "DataFrames almost equals" should "pass with decimals" in {
    dfIntDecimal.almostEqual(dfIntDecimal) shouldBe true
  }

  "DataFrames equals" should "pass with decimals" in {
    dfIntDecimal.equal(dfIntDecimal) shouldBe true
  }

  "DataFrames equals" should "pass even if dfs contains nulls" in {
    dsNull.equal(dsNull) shouldBe true
  }

  "DataFrames equals" should "fail if duplicates are present in one dataframe" in {
    val df1 = List(0.0, 0.0).toDF("x")
    val df2 = List(0.0).toDF("x")
    df1.equal(df2) shouldBe false
  }

  "DataFrames equals" should "show a helpful diff to find the difference" in {
    // note : this is rather for manual testing
    // in principle, one could test that certain messages are logged, but this seems
    // complicated

    val df1 = List(
      ((1.0, 2.0), List(3.0, 4.0)),
      ((5.0, 6.0), List(7.0, 8.0)),
      ((5.0, 6.0), List(7.0, 8.0))
    ).toDF("str", "arr")
      .withColumn("str", $"str".cast("struct<x:double,y:double>"))
      .withColumn("arr", $"arr".cast("array<double>"))

    val df2 = List(
      ((1.0, 2.0), List(3.1, 4.0)),
      ((5.0, 6.0), List(7.0, 8.0))
    ).toDF("str", "arr")
      .withColumn("str", $"str".cast("struct<x:double,y:double>"))
      .withColumn("arr", $"arr".cast("array<double>"))

    df1.equal(df2) shouldBe false
  }
}
