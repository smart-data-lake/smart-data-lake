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

package io.smartdatalake.util.spark.dataset

import io.smartdatalake.testutils.spark.dataset.Collection._
import io.smartdatalake.util.spark.GetSession.{createSparkSession, loggEnv}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.Double.{NaN, NegativeInfinity}

class QualityTest extends AnyFlatSpec with Matchers
  with Quality with Equality {
  @transient implicit private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private implicit val spark: SparkSession = createSparkSession()

  import spark.implicits._

  loggEnv

  "countDistinctRows" should "count distinct rows" in {
    val argument = List((0, 0d), (0, 0d), (1, 1d), (1, 1d), (2, 2d), (2, 2d)).toDF("id", "x")
    argument.createdLog("argument", debug = Some(true))
    argument.countDistinctRows should be(3)
  }

  "getDsStats" should "get stats on Doubles correctly" in {
    val actual = dfIdX.getStats
    // interesting: PositiveInfinity < NaN !
    val expected = List((17L, 17L, -4, 100, 16L, NegativeInfinity, NaN)).toDF("cnt_rows",
      "cnt_id", "min_id", "max_id",
      "cnt_x", "min_x", "max_x")
    actual.equal(expected) should be(true)
  }

  "getDsStats" should "not fail on arrays" in {
    val actual = dfArray2.getStats
    val expected = List((6L, 6L, Some(-2), Some(3), 5L, List[Int](), List(Some(0), Some(1)))).toDF("cnt_rows",
        "cnt_id", "min_id", "max_id",
        "cnt_arr", "min_arr", "max_arr")
      .select($"cnt_rows", $"cnt_id",
        $"min_id", $"max_id", $"cnt_arr",
        $"min_arr".cast(ArrayType(IntegerType, containsNull = true)).as("min_arr"), $"max_arr")
    actual.equal(expected) should be(true)
  }

  "setColumnComments" should "preserve type of Dataset" in {
    val ds: Dataset[TestCaseClass] = List(TestCaseClass(1, 1f, TestInnerClass(1, 1))).toDF().as[TestCaseClass]
    val dsCommented: Dataset[TestCaseClass] = ds
      .setColumnComments(Map("id" -> "desc_id", "x" -> "desc_x", "y" -> "desc_y"))
    ds.schema.map(_.dataType) shouldBe dsCommented.schema.map(_.dataType)
  }

  "setColumnComments" should "modify column metadata" in {
    val df = List(TestCaseClass(1, 1f, TestInnerClass(1, 1))).toDF()
    val dfCommented = df.setColumnComments(Map("id" -> "desc_id", "x" -> "desc_x", "y" -> "desc_y"))
    val actual = dfCommented.getColumnComments.select("comment").as[String].collect().toSet
    val expected = Set("desc_id", "desc_x", "desc_y")
    actual shouldBe expected
  }

  ///// tests treating gaps in axis (time or space) /////

  "fillGaps_next" should "fill the gaps taking value from next row" in {
    val actual = dfSnapshotsWithGaps.fillGaps(Seq("id"), Seq("Wert"), "dt")
    val expected = Seq(
      (Some(0), Some(20190101), Some(3.14), Some(-2.37)),
      (Some(0), Some(20190102), Some(3.14), Some(-2.37)),
      (Some(0), Some(20190103), Some(2.72), Some(4.57)),
      (Some(0), Some(20190104), Some(1.0), Some(3.0)),
      (Some(0), Some(20190106), Some(1.0), Some(3.0)),
      (Some(0), Some(20190201), Some(3.14), Some(2.5)),
      (Some(0), Some(20190207), Some(1.0), Some(2.5)),
      (Some(1), Some(20190101), Some(42.0), None),
      (Some(1), Some(20190102), Some(-21.3), None),
      (Some(1), Some(20190103), Some(-21.3), None),
      (Some(1), Some(20190104), Some(-21.3), None)).toDF("id", "dt", "x", "y")
    actual.equal(expected) should be(true)
  }

  "fillGaps_next" should "fill the gaps taking value from preivous row" in {
    val actual = dfSnapshotsWithGaps.fillGaps(Seq("id"), Seq("Wert"), "dt", takeNextValueFirst = false)
    val expected = Seq(
      (Some(0), Some(20190101), Some(3.14), Some(-2.37)),
      (Some(0), Some(20190102), Some(3.14), Some(-2.37)),
      (Some(0), Some(20190103), Some(2.72), Some(4.57)),
      (Some(0), Some(20190104), Some(2.72), Some(4.57)),
      (Some(0), Some(20190106), Some(1.0), Some(3.0)),
      (Some(0), Some(20190201), Some(3.14), Some(3.0)),
      (Some(0), Some(20190207), Some(1.0), Some(2.5)),
      (Some(1), Some(20190101), Some(42.0), None),
      (Some(1), Some(20190102), Some(42.0), None),
      (Some(1), Some(20190103), Some(-21.3), None),
      (Some(1), Some(20190104), Some(-21.3), None)).toDF("id", "dt", "x", "y")

    actual.equal(expected) should be(true)
  }

  ///// tests about nLets, unique keys, PK, nullness /////

  "getNonuniqueStats" should "return empty dataFrame if there are no nLets" in {
    val actual = dfHierarchy.getNonuniqueStats()
    val expected = dfHierarchy.where(lit(false)).withColumn("_cnt_", lit(0: Long))
    actual.equal(expected) should be(true)
  }

  "getNonuniqueStats" should "return nLets in projected DataFrame" in {
    val actual = dfHierarchy.getNonuniqueStats("parent")
    val zeilen_expected: Seq[(String, Long)] = Seq(("a", 2), ("c", 3), ("ca", 2))
    val expected = zeilen_expected.toDF("parent", "_cnt_")
    actual.equal(expected) should be(true)
  }

  "getNonuniqueStats" should "return nLets of a dataFrame which consists one column only of" in {
    val argument = List(0, 1, 2).toDF("id")
    val actual = argument.getNonuniqueStats()
    val expected = argument.where(lit(false)).withColumn("_cnt_", lit(0: Long))
    actual.equal(expected) should be(true)
  }

  "getNonuniqueStats" should "return nLets" in {
    val actual = dfnLets.getNonuniqueStats()
    val zeilen_expected: List[(String, String, Long)] = List(("2let", "doublet", 2),
      ("3let", "triplet", 3), ("4let", "quatriplet", 4))
    val expected = zeilen_expected.toDF("id", "name", "_cnt_")
    actual.equal(expected) should be(true)
  }

  "containsNull" should "return for dsComplex" in {
    val actual = dsComplex.containsNull()
    if (actual) {
      logger.error(s"actual = $actual")
      dsComplex.show(true)
    }
    actual shouldBe false
  }

  "containsNull" should "return for dsComplexWithNull" in {
    val actual = dsComplexWithNull.containsNull()
    if (!actual) {
      logger.error(s"actual = $actual")
      dsComplexWithNull.show(true)
    }
    assert(actual)
  }

  "getNulls" should "return for dsComplex" in {
    val actual = dsComplex.getNulls()
    val expected = dsComplex.where(lit(false))
    actual.equal(expected) should be(true)
  }

  "getNulls" should "return for dsComplexWithNull" in {
    val actual = dsComplexWithNull.getNulls()
    val rows_expected: List[(Option[Int], Option[List[(String, String, List[String])]])] = List(
      (Some(5), None), (None, None))
    val expected = rows_expected.toDF("id", "value").as[complexTypeWithNull]
    actual.equal(expected) should be(true)
  }

}
