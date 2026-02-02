/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.util.misc

import io.smartdatalake.testutils.TestUtil._
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataFrameUtilTest extends AnyFunSuite with Matchers with SmartDataLakeLogger {

  import session.implicits._

  // symmetric Difference and isEqual are used by tests of other functions
  test("symmetricDifference_no_difference") {
    val actual: DataFrame = dfComplex.symmetricDifference(dfComplex)
    val actualCount = actual.count()

    val resultat: Boolean = (actual.schema == dfComplex.schema.add("_in_first_df", BooleanType, nullable = false)) &&
      (0 == actualCount)
    if (!resultat) {
      logger.error(s"df_complex.schema = ${dfComplex.schema.simpleString}")
      logger.error(s"actual.schema     = ${actual.schema.simpleString}")
      actual.printSchema()
      actual.show()
      logger.error(s"actual.count()    = $actualCount")
    }
    assert(resultat)
  }

  test("symmetricDifference_with_difference") {
    val df_complex_2 = Seq(
      (1, Seq(("a", "A", Seq("a", "A")))),
      (2, Seq(("b", "B", Seq("b", "B")))),
      (3, Seq(("c", "C", Seq("c", "X")))),
      (4, Seq(("d", "D", Seq("d", "D")))),
      (5, Seq(("e", "E", Seq("e", "E"))))
    ).toDF("id", "value")
    val actual: DataFrame = dfComplex.symmetricDifference(df_complex_2, "df_complex")
    val actualCount = actual.count()
    val expected = Seq(
      (3, Seq(("c", "C", Seq("c", "C"))), true),
      (3, Seq(("c", "C", Seq("c", "X"))), false)
    ).toDF("id", "value", "_in_first_df")
    val resultat: Boolean = (actual.schema == dfComplex.schema.add("df_complex", BooleanType, nullable = false)) &&
      (2 == actualCount) && (actual.takeAsList(2) === expected.takeAsList(2))
    if (!resultat) {
      logger.error(s"df_complex.schema = ${dfComplex.schema.simpleString}")
      dfComplex.printSchema()
      dfComplex.show()
      logger.error(s"df_complex_2.schema = ${df_complex_2.schema.simpleString}")
      df_complex_2.printSchema()
      df_complex_2.show()
      logger.error(s"actual.count()  = $actualCount")
      logger.error(s"actual.schema   = ${actual.schema.simpleString}")
      actual.printSchema()
      actual.show()
      logger.error(s"expected.count()  = $actualCount")
      logger.error(s"expected.schema   = ${actual.schema.simpleString}")
      expected.printSchema()
      expected.show()
      logger.error(s"  Do schemata equal? ${actual.schema == expected.schema}")
    }
    assert(resultat)
  }

  test("symmetricDifference_withNull") {
    val df_complex_withNull_2 = Seq(
      (Some(1), Some(Seq(("a", "A", Seq("a", "A"))))),
      (Some(2), Some(Seq(("b", "B", Seq("b", "B"))))),
      (Some(3), Some(Seq(("c", "C", null)))),
      (Some(4), Some(Seq(("d", "D", Seq("d", "D"))))),
      (Some(5), Some(Seq(("e", "E", null))))
    ).toDF("id", "value")
    val actual: DataFrame = dfComplexWithNull.symmetricDifference(df_complex_withNull_2, "df_complex_withNull")
    val actualCount = actual.count().asInstanceOf[Int]
    val expected = Seq(
      (Some(5), None, true),
      (None, None, true),
      (Some(5), Some(Seq(("e", "E", null: Seq[String]))), false)
    ).toDF("id", "value", "df_complex_withNull")
    val expectedCount = expected.count().asInstanceOf[Int]

    val resultat: Boolean = (actual.schema == expected.schema) &&
      (actualCount == expectedCount) &&
      (actual.takeAsList(actualCount) === expected.takeAsList(expectedCount))

    if (!resultat) {
      logger.error(s"df_complex_withNull.schema = ${dfComplexWithNull.schema.simpleString}")
      dfComplexWithNull.printSchema()
      dfComplexWithNull.show()
      logger.error(s"df_complex_withNull_2.schema = ${df_complex_withNull_2.schema.simpleString}")
      df_complex_withNull_2.printSchema()
      df_complex_withNull_2.show()
      logger.error(s"actual.count()  = $actualCount")
      logger.error(s"actual.schema   = ${actual.schema.simpleString}")
      actual.printSchema()
      actual.show()
      logger.error(s"expected.count()  = $actualCount")
      logger.error(s"expected.schema   = ${actual.schema.simpleString}")
      expected.printSchema()
      expected.show()
      logger.error(s"  Do schemata equal? ${actual.schema == expected.schema}")
    }
    assert(resultat)
  }

  test("isEqual_true") {
    val actual: Boolean = dfComplex.isEqual(dfComplex)

    if (!actual) {
      logger.error(s"actual            = $actual")
      logger.error(s"df_complex.schema = ${dfComplex.schema.simpleString}")
      dfComplex.printSchema()
    }
    assert(actual)
  }

  test("isEqual_true_withNull") {
    val actual: Boolean = dfComplexWithNull.isEqual(dfComplexWithNull)

    if (!actual) {
      logger.error(s"actual                     = $actual")
      logger.error(s"df_complex_withNull.schema = ${dfComplexWithNull.schema.simpleString}")
      dfComplexWithNull.printSchema()
    }
    assert(actual)
  }

  test("isEqual_false_data") {
    val df_complex_2 = Seq(
      (1, Seq(("a", "A", Seq("a", "A")))),
      (2, Seq(("b", "B", Seq("b", "B")))),
      (3, Seq(("c", "C", Seq("c", "X")))),
      (4, Seq(("d", "D", Seq("d", "D")))),
      (5, Seq(("e", "E", Seq("e", "E"))))
    ).toDF("id", "Value")
    val actual: Boolean = dfComplex.isEqual(df_complex_2)
    if (actual) {
      logger.error("   symmetric Difference ")
      dfComplex.symmetricDifference(df_complex_2).show()
    }
    assert(!actual)
  }

}
