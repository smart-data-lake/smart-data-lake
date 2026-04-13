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
import io.smartdatalake.util.spark.GetSession.loggEnv
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

class StructTypeUtilTest extends AnyFlatSpec with Matchers
    with TestTool with StructTypeUtil with Transform {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private implicit val spark: SparkSession = TestUtil.session

  loggEnv

  "createStruct" should "created a struct" in {
    val argument = Array[(String, DataType, Boolean)](("id", IntegerType, false),
      ("name",                                               StringType,  false), ("birthdate", DateType, true))
    val actual = createStruct(argument)
    val expected = StructType(Array(StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("birthdate", DateType, nullable = true)))
    actual shouldEqual expected
  }

  "euqal" should "fail if column count differ" in {
    val schL: StructType = StructType(Array(
        StructField(name = "x", dataType = IntegerType, nullable = true),
        StructField(name = "y", dataType = IntegerType, nullable = true)
      ))
    val schR: StructType = StructType(Array(StructField(name = "x", dataType = IntegerType, nullable = true)))
    schL.equal(schR) shouldBe false
  }

  "equal" should "pass even if column order differ" in {
    val schL: StructType = StructType(Array(
        StructField(name = "x", dataType = IntegerType, nullable = true),
        StructField(name = "y", dataType = IntegerType, nullable = true)
      ))
    val schR: StructType = StructType(Array(
        StructField(name = "y", dataType = IntegerType, nullable = true),
        StructField(name = "x", dataType = IntegerType, nullable = true)
      ))
    schL.equal(schR) shouldBe true
  }

  "equal" should "pass even if nullability differs" in {
    val schL: StructType = StructType(Array(
        StructField(name = "x", dataType = IntegerType, nullable = true),
        StructField(name = "y", dataType = IntegerType, nullable = true)
      ))
    val schR: StructType = StructType(Array(
        StructField(name = "x", dataType = IntegerType, nullable = true),
        StructField(name = "y", dataType = IntegerType, nullable = false)
      ))
    schL.equal(schR) shouldBe true
  }

  "equal" should "fail if types differ" in {
    val schL: StructType = StructType(Array(
        StructField(name = "x", dataType = IntegerType, nullable = true),
        StructField(name = "y", dataType = DoubleType, nullable = true)
      ))
    val schR: StructType = StructType(Array(
        StructField(name = "x", dataType = IntegerType, nullable = true),
        StructField(name = "y", dataType = FloatType, nullable = true)
      ))
    schL.equal(schR) shouldBe false
  }

  "equal" should "fail if nested types differ" in {
    val valuesL: StructType = StructType(Array(
        StructField(name = "x", dataType = IntegerType, nullable = true),
        StructField(name = "y", dataType = DoubleType, nullable = true)
      ))
    val valuesR: StructType = StructType(Array(
        StructField(name = "x", dataType = IntegerType, nullable = true),
        StructField(name = "y", dataType = FloatType, nullable = true)
      ))
    val schL: StructType = StructType(Array(
        StructField(name = "id", dataType = StringType, nullable = false),
        StructField(name = "values", dataType = valuesL, nullable = true)
      ))
    val schR: StructType = StructType(Array(
        StructField(name = "id", dataType = StringType, nullable = false),
        StructField(name = "values", dataType = valuesR, nullable = true)
      ))
    schL.equal(schR) shouldBe false
  }

  "equal" should "ignore nullability but not containsNull" in {
    val rightSchema = StructType(Array(
        StructField("code_d", ArrayType(StringType, containsNull = true), nullable = true)
      ))
    val leftSchema =
      StructType(Array(
          StructField("code_d", ArrayType(StringType, containsNull = false), nullable = true)
        ))
    leftSchema.equal(rightSchema) shouldBe false
  }

  "schema" should "be superset of itself" in {
    val argExpMap: Map[DataFrame, Boolean] = Map(
      dsComplex.asDf          -> true,
      dsComplexWithNull.asDf  -> true,
      dfHierarchy             -> true,
      dsNonUnique.asDf        -> true,
      dsTwoCandidateKeys.asDf -> true
    )
    val testFun: DataFrame => Boolean = df => df.schema.isSuperSetOf(df.schema)
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

  "schema" should "be subset of itself" in {
    val argExpMap: Map[DataFrame, Boolean] = Map(
      dsComplex.asDf          -> true,
      dsComplexWithNull.asDf  -> true,
      dfHierarchy             -> true,
      dsNonUnique.asDf        -> true,
      dsTwoCandidateKeys.asDf -> true
    )
    val testFun: DataFrame => Boolean = df => df.schema.isSubSetOf(df.schema)
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

  "schema" should "be superset of itself reversed" in {
    val argExpMap: Map[DataFrame, Boolean] = Map(
      dsComplex.asDf          -> true,
      dsComplexWithNull.asDf  -> true,
      dfHierarchy             -> true,
      dsNonUnique.asDf        -> true,
      dsTwoCandidateKeys.asDf -> true
    )
    val testFun: DataFrame => Boolean = df => df.schema.isSuperSetOf(new StructType(df.schema.fields.reverse))
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

  "schema" should "be subset of itself reversed" in {
    val argExpMap: Map[DataFrame, Boolean] = Map(
      dsComplex.asDf          -> true,
      dsComplexWithNull.asDf  -> true,
      dfHierarchy             -> true,
      dsNonUnique.asDf        -> true,
      dsTwoCandidateKeys.asDf -> true
    )
    val testFun: DataFrame => Boolean = df => df.schema.isSubSetOf(new StructType(df.schema.fields.reverse))
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

  "schema" should "be superset of itself with first column dropped" in {
    val argExpMap: Map[DataFrame, Boolean] = Map(
      dsComplex.asDf          -> true,
      dsComplexWithNull.asDf  -> true,
      dfHierarchy             -> true,
      dsNonUnique.asDf        -> true,
      dsTwoCandidateKeys.asDf -> true
    )
    val testFun: DataFrame => Boolean = df => df.schema.isSuperSetOf(new StructType(df.schema.drop(1).toArray))
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

}
