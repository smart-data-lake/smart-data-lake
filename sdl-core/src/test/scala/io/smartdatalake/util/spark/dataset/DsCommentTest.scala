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

import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.spark.GetSession.loggEnv
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

class DsCommentTest extends AnyFlatSpec with Matchers
  with Quality with Equality {
  @transient implicit private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
    private implicit val spark: SparkSession = TestUtil.session

  import spark.implicits._

  loggEnv

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

  "withComment" should "split alias from column name" in {
    val df = List(TestCaseClass(1, 1f, TestInnerClass(1, 1))).toDF()
    val dfCommented = df.as("a")
      .select(
        withComment("a.id", "desc_id"),
        withComment("x", "desc_x"),
        withComment("y", $"y", "desc_y")
      )
    val actual = dfCommented.getColumnComments.select("comment").as[String].collect().toSet
    val expected = Set("desc_id", "desc_x", "desc_y")
    actual shouldBe expected
    dfCommented.columns shouldBe df.columns
  }

  "DsColComment.withComment" should "add a comment fluently on a Column" in {
    val df = List(TestCaseClass(1, 1f, TestInnerClass(1, 1))).toDF()
    val dfCommented = df.select(col("id").withComment("desc_id"))
    dfCommented.getColumnComments.select("comment").as[String].collect().toSet shouldBe Set("desc_id")
  }

  "DsColComment.makeNotNullable" should "mark the column as not-nullable in the schema" in {
    val df = List(TestCaseClass(1, 1f, TestInnerClass(1, 1))).toDF()
    val dfNotNullable = df.select(col("id").makeNotNullable)
    dfNotNullable.schema("id").nullable shouldBe false
  }

  it should "throw at runtime if the column actually contains a null value" in {
    val df = List(Some(1), None).toDF("id")
    val dfNotNullable = df.select(col("id").makeNotNullable)
    a[Exception] should be thrownBy dfNotNullable.collect()
  }

}
