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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

class DsPkTest extends AnyFlatSpec with Matchers
  with Quality with Equality {
  @transient implicit private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private implicit val spark: SparkSession = createSparkSession()

  import spark.implicits._

  loggEnv

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
