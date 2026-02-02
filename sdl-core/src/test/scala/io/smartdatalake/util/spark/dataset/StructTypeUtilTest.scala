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

import io.smartdatalake.testutils.TestTool
import io.smartdatalake.util.spark.GetSession.{createSparkSession, loggEnv}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.slf4j.{Logger, LoggerFactory}

class StructTypeUtilTest extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks
  with TestTool with Equality {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private implicit val spark: SparkSession = createSparkSession()

  loggEnv

  "schemataEqual equals" should "fail if column count differ" in {
    val schL: StructType = StructType(Array(
      StructField(name = "x", dataType = IntegerType, nullable = true),
      StructField(name = "y", dataType = IntegerType, nullable = true)))
    val schR: StructType = StructType(Array(StructField(name = "x", dataType = IntegerType, nullable = true)))
    schL.equal(schR) shouldBe false
  }

  "Schemata equal" should "pass even if column order differ" in {
    val schL: StructType = StructType(Array(
      StructField(name = "x", dataType = IntegerType, nullable = true),
      StructField(name = "y", dataType = IntegerType, nullable = true)))
    val schR: StructType = StructType(Array(
      StructField(name = "y", dataType = IntegerType, nullable = true),
      StructField(name = "x", dataType = IntegerType, nullable = true)))
    schL.equal(schR) shouldBe true
  }

  "Schemata equal" should "pass even if nullability differs" in {
    val schL: StructType = StructType(Array(
      StructField(name = "x", dataType = IntegerType, nullable = true),
      StructField(name = "y", dataType = IntegerType, nullable = true)))
    val schR: StructType = StructType(Array(
      StructField(name = "x", dataType = IntegerType, nullable = true),
      StructField(name = "y", dataType = IntegerType, nullable = false)))
    schL.equal(schR) shouldBe true
  }

  "Schemata equal" should "fail if types differ" in {
    val schL: StructType = StructType(Array(
      StructField(name = "x", dataType = IntegerType, nullable = true),
      StructField(name = "y", dataType = DoubleType, nullable = true)))
    val schR: StructType = StructType(Array(
      StructField(name = "x", dataType = IntegerType, nullable = true),
      StructField(name = "y", dataType = FloatType, nullable = true)))
    schL.equal(schR) shouldBe false
  }

  "Schemata equal" should "fail if nested types differ" in {
    val valuesL: StructType = StructType(Array(
      StructField(name = "x", dataType = IntegerType, nullable = true),
      StructField(name = "y", dataType = DoubleType, nullable = true)))
    val valuesR: StructType = StructType(Array(
      StructField(name = "x", dataType = IntegerType, nullable = true),
      StructField(name = "y", dataType = FloatType, nullable = true)))
    val schL: StructType = StructType(Array(
      StructField(name = "id", dataType = StringType, nullable = false),
      StructField(name = "values", dataType = valuesL, nullable = true)))
    val schR: StructType = StructType(Array(
      StructField(name = "id", dataType = StringType, nullable = false),
      StructField(name = "values", dataType = valuesR, nullable = true)))
    schL.equal(schR) shouldBe false
  }

  "Schemata equal" should "ignore nullability but not containsNull" in {
    val rightSchema = StructType(Array(
      StructField("code_d", ArrayType(StringType, containsNull = true), nullable = true)
    ))
    val leftSchema =
      StructType(Array(
        StructField("code_d", ArrayType(StringType, containsNull = false), nullable = true)
      ))
    leftSchema.equal(rightSchema) shouldBe false
  }

}
