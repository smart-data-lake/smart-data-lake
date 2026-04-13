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
package io.smartdatalake.util.spark.dataset

import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.testutils.spark.dataset.Collection._
import io.smartdatalake.util.spark.GetSession.loggEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.Double.{NaN, NegativeInfinity}

class DsQualityTest extends AnyFlatSpec with Matchers
    with Quality with Equality {
  @transient implicit private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private implicit val spark: SparkSession = TestUtil.session

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

  ///// tests treating gaps in axis (time or space) /////

  "fillGaps_next" should "fill the gaps taking value from next row" in {
    val actual = dfSnapshotsWithGaps.fillGaps(Seq("id"), Seq("Wert"), "dt")
    val expected = Seq(
      (Some(0), Some(20190101), Some(3.14),  Some(-2.37)),
      (Some(0), Some(20190102), Some(3.14),  Some(-2.37)),
      (Some(0), Some(20190103), Some(2.72),  Some(4.57)),
      (Some(0), Some(20190104), Some(1.0),   Some(3.0)),
      (Some(0), Some(20190106), Some(1.0),   Some(3.0)),
      (Some(0), Some(20190201), Some(3.14),  Some(2.5)),
      (Some(0), Some(20190207), Some(1.0),   Some(2.5)),
      (Some(1), Some(20190101), Some(42.0),  None),
      (Some(1), Some(20190102), Some(-21.3), None),
      (Some(1), Some(20190103), Some(-21.3), None),
      (Some(1), Some(20190104), Some(-21.3), None)
    ).toDF("id", "dt", "x", "y")
    actual.equal(expected) should be(true)
  }

  "fillGaps_next" should "fill the gaps taking value from previous row" in {
    val actual = dfSnapshotsWithGaps.fillGaps(Seq("id"), Seq("Wert"), "dt", takeNextValueFirst = false)
    val expected = Seq(
      (Some(0), Some(20190101), Some(3.14),  Some(-2.37)),
      (Some(0), Some(20190102), Some(3.14),  Some(-2.37)),
      (Some(0), Some(20190103), Some(2.72),  Some(4.57)),
      (Some(0), Some(20190104), Some(2.72),  Some(4.57)),
      (Some(0), Some(20190106), Some(1.0),   Some(3.0)),
      (Some(0), Some(20190201), Some(3.14),  Some(3.0)),
      (Some(0), Some(20190207), Some(1.0),   Some(2.5)),
      (Some(1), Some(20190101), Some(42.0),  None),
      (Some(1), Some(20190102), Some(42.0),  None),
      (Some(1), Some(20190103), Some(-21.3), None),
      (Some(1), Some(20190104), Some(-21.3), None)
    ).toDF("id", "dt", "x", "y")

    actual.equal(expected) should be(true)
  }

  "transformCommentCols" should "square column x only and add a comment" in {
    val argument = List((0, 0d), (1, 1d), (2, 2d), (3, 3d)).toDF("id", "x")

    def transformRenameCommentFun(cn: String): List[CommentedColumn] = List(
      CommentedColumn(colname = s"${cn}_square",
        definition = col(cn) * col(cn),
        comment = "square of x")
    )

    val actual = argument.transformCommentCols(transformRenameCommentFun, colFilter = _ == "x")
    val expected = List((0, 0d), (1, 1d), (2, 4d), (3, 9d)).toDF("id", "x_square")
    val expectedComments = List(("id", "int",    ""),
      ("x_square",                     "double", "square of x"))
      .toDF("column", "datatype", "comment")
      .as[(String, String, String)]

    actual.equal(expected) && actual.getColumnComments.equal(expectedComments) should be(true)
  }
}
