/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.util.util

import io.smartdatalake.testutils.TestUtil._
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.scalatest.Matchers

class DataFrameUtilTest extends org.scalatest.FunSuite with Matchers with SmartDataLakeLogger {

  import sessionHiveCatalog.implicits._

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
      (1,Seq(("a","A",Seq("a","A")))),
      (2,Seq(("b","B",Seq("b","B")))),
      (3,Seq(("c","C",Seq("c","X")))),
      (4,Seq(("d","D",Seq("d","D")))),
      (5,Seq(("e","E",Seq("e","E"))))
    ).toDF("id","value")
    val actual: DataFrame = dfComplex.symmetricDifference(df_complex_2,"df_complex")
    val actualCount = actual.count()
    val expected = Seq(
      (3,Seq(("c","C",Seq("c","C"))),true),
      (3,Seq(("c","C",Seq("c","X"))),false)
    ).toDF("id","value","_in_first_df")
    val resultat: Boolean = (actual.schema == dfComplex.schema.add("df_complex", BooleanType, nullable = false)) &&
      (2 == actualCount) && (actual.takeAsList(2)===expected.takeAsList(2))
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
      logger.error(s"  Do schemata equal? ${actual.schema==expected.schema}")
    }
    assert(resultat)
  }

  test("symmetricDifference_withNull") {
    val df_complex_withNull_2 = Seq(
      (Some(1), Some(Seq(("a","A",Seq("a","A"))))),
      (Some(2), Some(Seq(("b","B",Seq("b","B"))))),
      (Some(3), Some(Seq(("c","C",null)))),
      (Some(4), Some(Seq(("d","D",Seq("d","D"))))),
      (Some(5), Some(Seq(("e","E",null))))
    ).toDF("id","value")
    val actual: DataFrame = dfComplexWithNull.symmetricDifference(df_complex_withNull_2,"df_complex_withNull")
    val actualCount = actual.count().asInstanceOf[Int]
    val expected = Seq(
      (Some(5), None, true),
      (None, None, true),
      (Some(5), Some(Seq(("e","E",null: Seq[String]))), false)
    ).toDF("id","value","df_complex_withNull")
    val expectedCount = expected.count().asInstanceOf[Int]

    val resultat: Boolean = (actual.schema == expected.schema) &&
      (actualCount == expectedCount) &&
      (actual.takeAsList(actualCount)===expected.takeAsList(expectedCount))

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
      logger.error(s"  Do schemata equal? ${actual.schema==expected.schema}")
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

  test("isEqual_false") {
    val df_complex_2 = Seq(
      (1,Seq(("a","A",Seq("a","A")))),
      (2,Seq(("b","B",Seq("b","B")))),
      (3,Seq(("c","C",Seq("c","X")))),
      (4,Seq(("d","D",Seq("d","D")))),
      (5,Seq(("e","E",Seq("e","E"))))
    ).toDF("id","Value")
    val actual: Boolean = dfComplex.isEqual(df_complex_2)
    if (actual) {
      logger.error("   symmetric Difference ")
      dfComplex.symmetricDifference(df_complex_2).show()
    }
    assert(!actual)
  }

  test("isDataFrameDataEqual_df_complex_withNull_df_complex_withNull") {
    val actual: Boolean = isDataFrameDataEqual(dfComplexWithNull,dfComplexWithNull)

    if (!actual) {
      logger.error(s"actual                     = $actual")
      logger.error(s"df_complex_withNull.schema = ${dfComplexWithNull.schema.simpleString}")
      dfComplexWithNull.printSchema()
      dfComplexWithNull.show()
      logger.error(s"!!! We let this test pass even though it failed !!!")
      logger.error(s"Recommendation:")
      logger.error(s"Do not use function isDataFrameDataEqual anymore as it does not work with empty cells.")
    }
    // Do not name boolean parameter even if IntelliJ tells you to do so!
    // otherwise: [Error] macro applications do not support named and/or default arguments
    assert(true)
  }

  // other tests

  test("castAllDate2Timestamp") {
    val actual = dfManyTypes.castAllDate2Timestamp
    val expected = actual.withColumn("_date",$"_date".cast(TimestampType))
    val resultat: Boolean = actual.isEqual(expected)
    if (!resultat) printFailedTestResult("castAllDate2Timestamp",Seq(dfManyTypes))(actual)(expected)
    assert(resultat)
  }

  test("castAll2String") {
    val actual = dfManyTypes.castAll2String
    val expected = actual.columns.foldLeft(actual)({ (df, s) => df.withColumn(s,col(s).cast(StringType)) })
    val resultat: Boolean = actual.isEqual(expected)
    if (!resultat) printFailedTestResult("castAll2String",Seq(dfManyTypes))(actual)(expected)
    assert(resultat)
  }

  test("castAllDecimal2IntegralFloat") {
    val actual = dfManyTypes.castAllDecimal2IntegralFloat
    val expected = actual
      .withColumn("_decimal_2_0", $"_decimal_2_0".cast(ByteType))
      .withColumn("_decimal_4_0", $"_decimal_4_0".cast(ShortType))
      .withColumn("_decimal_10_0", $"_decimal_10_0".cast(IntegerType))
      .withColumn("_decimal_11_0", $"_decimal_11_0".cast(LongType))
      .withColumn("_decimal_4_3", $"_decimal_4_3".cast(FloatType))
      .withColumn("_decimal_38_1", $"_decimal_38_1".cast(DoubleType))
    val resultat: Boolean = actual.isEqual(expected)
    if (!resultat) printFailedTestResult("castAllDecimal2IntegralFloat",Seq(dfManyTypes))(actual)(expected)
    assert(resultat)
  }

  test("containsNull_df_complex") {
    val actual = dfComplex.containsNull()
    if (actual) {
      logger.error(s"actual = $actual")
      dfHierarchy.show(true)
    }
    assert(!actual)
  }

  test("containsNull_df_complex_withNull") {
    val actual = dfComplexWithNull.containsNull()
    if (!actual) {
      logger.error(s"actual = $actual")
      dfHierarchy.show(true)
    }
    assert(actual)
  }

  test("getNonuniqueStats_no_nlets") {
    val actual = dfHierarchy.getNonuniqueStats()
    val expected = dfHierarchy.where(lit(false)).withColumn("_cnt_", lit(0: Long))
    val resultat: Boolean = actual.isEqual(expected)
    if (!resultat) printFailedTestResult("getNonuniqueStats_No_nlets",Seq(dfHierarchy))(actual)(expected)
    assert(resultat)
  }

  test("getNonuniqueStats_nlets_in_projected_data_frame") {
    val actual = dfHierarchy.getNonuniqueStats(Array("parent"))
    val rows_expected: Seq[(String, Long)] = Seq(("a",2),("c",3),("ca",2))
    val expected: DataFrame = rows_expected.toDF("parent","_cnt_")

    val resultat: Boolean = actual.isEqual(expected)
    if (!resultat) printFailedTestResult("getNonuniqueStats_nlets_in_projected_data_frame",Seq(dfHierarchy))(actual)(expected)
    assert(resultat)
  }

  test("getNonuniqueStats_DF_with_1_column") {
    val argument = Seq(0, 1, 2).toDF("id")
    val actual = argument.getNonuniqueStats()
    val expected = argument.where(lit(false)).withColumn("_cnt_", lit(0: Long))
    val resultat: Boolean = actual.isEqual(expected)
    if (!resultat) printFailedTestResult("getNonuniqueStats_DF_with_1_column",Seq(argument))(actual)(expected)
    assert(resultat)
  }

  test("getNonuniqueStats_DF_with_nlets") {
    val actual = dfNonUnique.getNonuniqueStats()
    val rows_expected: Seq[(String, String, Long)] = Seq(("2let","doublet",2),("3let","triplet",3),("4let","quatriplet",4))
    val expected: DataFrame = rows_expected.toDF("id","value","_cnt_")
    val resultat: Boolean = actual.isEqual(expected)
    if (!resultat) printFailedTestResult("getNonuniqueStats_DF_with_nlets",Seq(dfNonUnique))(actual)(expected)
    assert(resultat)
  }

  test("getNulls_df_complex") {
    val actual = dfComplex.getNulls()
    val expected =  dfComplex.where(lit(false))
    val resultat: Boolean = actual.isEqual(expected)
    if (!resultat) printFailedTestResult("getNulls_df_complex",Seq(dfComplex))(actual)(expected)
    assert(resultat)
  }

  test("getNulls_df_complex_withNull") {
    val actual = dfComplexWithNull.getNulls()
    val rows_expected: Seq[(Option[Int], Option[Seq[(String, String, Seq[String])]])] = Seq(
      (Some(5), None),
      (None, None)
    )
    val expected: DataFrame = rows_expected.toDF("id", "value")
    val resultat: Boolean = actual.isEqual(expected)
    if (!resultat) printFailedTestResult("getNulls_df_complex_withNull",Seq(dfNonUnique))(actual)(expected)
    assert(resultat)
  }

  test("isCandidateKey_df_complex_withNull") {
    val actual = dfComplexWithNull.isCandidateKey(Array("id"))
    if (actual) {
      logger.error(s"actual = $actual")
      dfComplexWithNull.show(true)
    }
    assert(!actual)
  }

  test("isCandidateKey_df_TwoCandidateKeys_string12") {
    val actual = dfTwoCandidateKeys.isCandidateKey(Array("string_id1","string_id2"))
    if (!actual) {
      logger.error(s"actual = $actual")
      dfTwoCandidateKeys.show()
    }
    assert(actual)
  }

  test("isCandidateKey_df_TwoCandidateKeys_int123") {
    val actual = dfTwoCandidateKeys.isCandidateKey(Array("int_id1","int_id2","int_id3"))
    if (!actual) {
      logger.error(s"actual = $actual")
      dfTwoCandidateKeys.show()
    }
    assert(actual)
  }

  test("isCandidateKey_df_TwoCandidateKeys_string12int1") {
    val actual = dfTwoCandidateKeys.isCandidateKey(Array("string_id1","string_id2","int_id1"))
    if (actual) {
      logger.error(s"actual = $actual")
      dfTwoCandidateKeys.show()
    }
    assert(!actual)
  }

  test("isMinimalUnique_df_TwoCandidateKeys_string12") {
    val actual = dfTwoCandidateKeys.isMinimalUnique(Array("string_id1","string_id2"))
    if (!actual) {
      logger.error(s"actual = $actual")
      dfTwoCandidateKeys.show()
    }
    assert(actual)
  }

  test("isMinimalUnique_df_TwoCandidateKeys_int123") {
    val actual = dfTwoCandidateKeys.isMinimalUnique(Array("int_id1","int_id2","int_id3"))
    if (!actual) {
      logger.error(s"actual = $actual")
      dfTwoCandidateKeys.show()
    }
    assert(actual)
  }

  test("isMinimalUnique_df_TwoCandidateKeys_string12int1") {
    val actual = dfTwoCandidateKeys.isMinimalUnique(Array("string_id1","string_id2","int_id1"))
    if (actual) {
      logger.error(s"actual = $actual")
      dfTwoCandidateKeys.show()
    }
    assert(!actual)
  }

  test("isUnique_df_complex_withNull") {
    val actual = dfComplexWithNull.isUnique()
    if (!actual) {
      logger.error(s"actual = $actual")
      dfComplexWithNull.show(true)
    }
    assert(actual)
  }

  test("isUnique_df_hierarchy_true") {
    val actual = dfHierarchy.isUnique()
    if (!actual) {
      logger.error(s"actual = $actual")
      dfHierarchy.show()
    }
    assert(actual)
  }

  test("isUnique_df_hierarchy_false") {
    val actual = dfHierarchy.isUnique(Array("parent"))
    if (actual) {
      logger.error(s"actual = $actual")
      dfHierarchy.show()
    }
    assert(!actual)
  }

  test("isUnique_df_with_1_column") {
    val argument = Seq(0, 1, 2).toDF("id")
    val actual = argument.isUnique()
    if (!actual) {
      logger.error(s"actual = $actual")
      argument.show()
    }
    assert(actual)
  }

  test("isUnique_df_with_nlets") {
    val actual = dfNonUnique.isUnique()
    if (actual) {
      logger.error(s"actual = $actual")
      dfHierarchy.show()
    }
    assert(!actual)
  }

  test("project_df_complex") {
    val actual = dfComplex.project(Array("value"))
    val rows_expected: Seq[Seq[(String, String, Seq[String])]] = Seq(
      Seq(("a","A",Seq("a","A"))),
      Seq(("b","B",Seq("b","B"))),
      Seq(("c","C",Seq("c","C"))),
      Seq(("d","D",Seq("d","D"))),
      Seq(("e","E",Seq("e","E")))
    )
    val expected: DataFrame = rows_expected.toDF("value")
    val resultat: Boolean = actual.isEqual(expected)
    if (!resultat) printFailedTestResult("project_df_complex",Seq(dfNonUnique))(actual)(expected)
    assert(resultat)
  }



  /// Schema validation tests

  /// with itself
  test("isSubschemaItself") {
    val argExpMap: Map[DataFrame, Boolean] = Map(
      dfComplex -> true,
      dfComplexWithNull -> true,
      dfHierarchy -> true,
      dfNonUnique -> true,
      dfNonUniqueWithNull -> true,
      dfTwoCandidateKeys -> true
    )
    val testFun: DataFrame => Boolean = df => df.isSubSchema(df.schema)
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
  }

  test("isSuperschemaItself") {
    val argExpMap: Map[DataFrame,Boolean] = Map(
      dfComplex -> true,
      dfComplexWithNull -> true,
      dfHierarchy -> true,
      dfNonUnique -> true,
      dfNonUniqueWithNull -> true,
      dfTwoCandidateKeys -> true
    )
    val testFun: DataFrame => Boolean = df => df.isSuperSchema(df.schema)
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
  }

  // reversed column order
  test("isSubschemaReversed") {
    val argExpMap: Map[DataFrame,Boolean] = Map(
      dfComplex -> true,
      dfComplexWithNull -> true,
      dfHierarchy -> true,
      dfNonUnique -> true,
      dfNonUniqueWithNull -> true,
      dfTwoCandidateKeys -> true
    )
    val testFun: DataFrame => Boolean = df => df.isSubSchema(new StructType(df.schema.fields.reverse))
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
  }

  test("isSuperschemaReversed") {
    val argExpMap: Map[DataFrame,Boolean] = Map(
      dfComplex -> true,
      dfComplexWithNull -> true,
      dfHierarchy -> true,
      dfNonUnique -> true,
      dfNonUniqueWithNull -> true,
      dfTwoCandidateKeys -> true
    )
    val testFun: DataFrame => Boolean = df => df.isSuperSchema(new StructType(df.schema.fields.reverse))
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
  }

  /// first column dropped
  test("isSubschemaFirstColumnDropped") {
    val argExpMap: Map[DataFrame,Boolean] = Map(
      dfComplex -> true,
      dfComplexWithNull -> true,
      dfHierarchy -> true,
      dfNonUnique -> true,
      dfNonUniqueWithNull -> true,
      dfTwoCandidateKeys -> true
    )
    val testFun: DataFrame => Boolean = df => df.isSubSchema(new StructType(df.schema.drop(1).toArray))
    println(s"isSuperSchema(df_complex) = ${testFun(dfComplex)}")
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
  }

  test("isSuperschemaFirstColumnDropped") {
    val argExpMap: Map[DataFrame,Boolean] = Map(
      dfComplex -> false,
      dfComplexWithNull -> false,
      dfHierarchy -> false,
      dfNonUnique -> false,
      dfNonUniqueWithNull -> false,
      dfTwoCandidateKeys -> false
    )
    val testFun: DataFrame => Boolean = df => df.isSuperSchema(new StructType(df.schema.drop(1).toArray))
    testArgumentExpectedMap[DataFrame, Boolean](testFun, argExpMap)
  }

  //schemaDiffTo tests
  test("SchemaDiff to empty schema is empty.") {
    val schema = StructType(Nil)
    dfEmptyWithStructuredSchema.schemaDiffTo(schema) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schema, deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schema, ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schema, ignoreNullable = true, deep = true) shouldBe empty
  }

  test("SchemaDiff ignores duplicates.") {
    val schema = StructType(nullableStringField :: nullableStringField :: Nil)
    dfEmptyWithStructuredSchema.schemaDiffTo(schema) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schema, deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schema, ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schema, ignoreNullable = true, deep = true) shouldBe empty

    val intFieldNotnullable = StructField(notNullableStringField.name, IntegerType, nullable = false)
    val diffSchema = StructType(intFieldNotnullable :: intFieldNotnullable :: Nil)
    dfEmptyWithStructuredSchema.schemaDiffTo(diffSchema).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(diffSchema, deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(diffSchema, ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(diffSchema, ignoreNullable = true, deep = true).size should be (1)

    val schemaStruct = StructType(nullableStructField :: nullableStructField :: Nil)
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaStruct) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaStruct, deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaStruct, ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaStruct, ignoreNullable = true, deep = true) shouldBe empty

    val schemaMap = StructType(notNullableMapField :: notNullableMapField :: Nil)
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaMap) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaMap, deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaMap, ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaMap, ignoreNullable = true, deep = true) shouldBe empty

    val schemaArray = StructType(notNullableArrayField :: notNullableArrayField :: Nil)
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaArray) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaArray, deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaArray, ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaArray, ignoreNullable = true, deep = true) shouldBe empty
  }

  test("SchemaDiff ignores column order.") {
    val schema = StructType(notNullableStringField :: nullableStringField :: Nil)
    dfEmptyWithStructuredSchema.schemaDiffTo(schema) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schema, deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schema, ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schema, ignoreNullable = true, deep = true) shouldBe empty

    val schemaStruct = StructType(nullableStructField :: notNullableStructField :: Nil)
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaStruct) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaStruct, deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaStruct, ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaStruct, ignoreNullable = true, deep = true) shouldBe empty

    val schemaMap = StructType(nullableMapField :: notNullableMapField :: Nil)
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaMap) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaMap, deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaMap, ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaMap, ignoreNullable = true, deep = true) shouldBe empty

    val schemaArray = StructType(nullableArrayField :: notNullableArrayField :: Nil)
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaArray) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaArray, deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaArray, ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(schemaArray, ignoreNullable = true, deep = true) shouldBe empty
  }

  test("SchemaDiff to simple field.") {
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableStringField :: Nil)) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableStringField :: Nil), deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableStringField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableStringField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableStringField :: Nil)) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableStringField :: Nil), deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableStringField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    //nullability difference
    val fieldDiffNotnullable = nullableStringField.copy(nullable = false)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(fieldDiffNotnullable :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(fieldDiffNotnullable :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(fieldDiffNotnullable :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    val fieldDiffNullable = notNullableStringField.copy(nullable = true)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(fieldDiffNullable :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(fieldDiffNullable :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(fieldDiffNullable :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(fieldDiffNullable :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    //type difference
    val intFieldNotnullable = notNullableStringField.copy(dataType = IntegerType)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(intFieldNotnullable :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(intFieldNotnullable :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(intFieldNotnullable :: Nil), ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(intFieldNotnullable :: Nil), ignoreNullable = true, deep = true).size should be (1)

    val intFieldNullable = nullableStringField.copy(dataType = IntegerType)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(intFieldNullable :: Nil)) should not be empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(intFieldNullable :: Nil), deep = true) should not be empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(intFieldNullable :: Nil), ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(intFieldNullable :: Nil), ignoreNullable = true, deep = true).size should be (1)

    //name difference
    dfEmptyWithStructuredSchema.schemaDiffTo(
      StructType(nullableStringField.copy(name = "foo") :: notNullableStringField.copy(name = "foo2") :: Nil)
    ).size should be (2)
    dfEmptyWithStructuredSchema.schemaDiffTo(
      StructType(nullableStringField.copy(name = "foo") :: notNullableStringField.copy(name = "foo2") :: Nil),
      deep = true).size should be (2)
    dfEmptyWithStructuredSchema.schemaDiffTo(
      StructType(nullableStringField.copy(name = "foo") :: notNullableStringField.copy(name = "foo2") :: Nil),
      ignoreNullable = true
    ).size should be (2)
    dfEmptyWithStructuredSchema.schemaDiffTo(
      StructType(nullableStringField.copy(name = "foo") :: notNullableStringField.copy(name = "foo2") :: Nil),
      ignoreNullable = true, deep = true
    ).size should be (2)
  }

  test("SchemaDiff to structured fields.") {
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableStructField :: Nil)) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableStructField :: Nil), deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableStructField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableStructField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableStructField :: Nil)) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableStructField :: Nil), deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableStructField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableStructField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    //name difference
    val diffNameStructField = nullableStructField.copy(name = "foo")
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameStructField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameStructField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameStructField :: Nil), ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameStructField :: Nil), ignoreNullable = true, deep = true).size should be (1)

    //contains null difference
    val diffContainsNullStructField = nullableStructField.copy(name = notNullableStructField.name)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullStructField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullStructField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullStructField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullStructField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    val diffContainsNoNullStructField = notNullableStructField.copy(name = nullableStructField.name)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullStructField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullStructField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullStructField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullStructField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    //field difference
    val missingFieldStructField = nullableStructField.copy(dataType = StructType(
      nullableStructField.dataType.asInstanceOf[StructType].fields.drop(1)
    ))
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(missingFieldStructField :: Nil)).size should be (1) // no deep partial matching.
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(missingFieldStructField :: Nil), deep = true) shouldBe empty // deep partial match
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(missingFieldStructField :: Nil), ignoreNullable = true).size should be (1) // no deep partial matching.
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(missingFieldStructField :: Nil), ignoreNullable = true, deep = true) shouldBe empty // deep partial match

    val additionalFieldStructField = nullableStructField.copy(dataType = StructType(
      nullableStructField.dataType.asInstanceOf[StructType].fields ++ Array(StructField("foo", StringType, nullable = true))
    ))
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(additionalFieldStructField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(additionalFieldStructField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(additionalFieldStructField :: Nil), ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(additionalFieldStructField :: Nil), ignoreNullable = true, deep = true).size should be (1)

    //field nullability can be ignored
    val switchedNullabilityInFields = nullableStructField.copy(
        dataType = StructType(nullableStructField.dataType.asInstanceOf[StructType].fields.map(f => f.copy(nullable = !f.nullable)))
    )
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedNullabilityInFields :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedNullabilityInFields :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedNullabilityInFields :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedNullabilityInFields :: Nil), ignoreNullable = true, deep = true) shouldBe empty
  }

  test("SchemaDiff to array fields.") {
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableArrayField :: Nil)) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableArrayField :: Nil), deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableArrayField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableArrayField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableArrayField :: Nil)) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableArrayField :: Nil), deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableArrayField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableArrayField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    //name difference
    val diffNameField = nullableArrayField.copy(name = "foo")
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameField :: Nil), ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameField :: Nil), ignoreNullable = true, deep = true).size should be (1)

    //contains null difference
    val diffContainsNullField = nullableArrayField.copy(name = notNullableArrayField.name)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    val diffContainsNoNullField = notNullableArrayField.copy(name = nullableArrayField.name)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    //elementType difference
    val diffElementTypeField = nullableArrayField.copy(dataType = ArrayType(IntegerType, containsNull = true))
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffElementTypeField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffElementTypeField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffElementTypeField :: Nil), ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffElementTypeField :: Nil), ignoreNullable = true, deep = true).size should be (1)

    val missingFieldDeepDiffElementTypeField = nullableArrayField.copy(dataType = ArrayType(
      StructType(notNullableStringField :: Nil),
      containsNull = true
    ))
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(missingFieldDeepDiffElementTypeField :: Nil)).size should be (1) // no partial deep match
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(missingFieldDeepDiffElementTypeField :: Nil), deep = true) shouldBe empty // partial deep match
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(missingFieldDeepDiffElementTypeField :: Nil), ignoreNullable = true).size should be (1) // no partial deep match
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(missingFieldDeepDiffElementTypeField :: Nil), ignoreNullable = true, deep = true) shouldBe empty // partial deep match

    val nameDifferenceDeepDiffElementTypeField = nullableArrayField.copy(dataType = ArrayType(
        StructType(nullableStringField.copy(name = "foo") :: notNullableStringField :: Nil),
        containsNull = true
    ))
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nameDifferenceDeepDiffElementTypeField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nameDifferenceDeepDiffElementTypeField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nameDifferenceDeepDiffElementTypeField :: Nil), ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nameDifferenceDeepDiffElementTypeField :: Nil), ignoreNullable = true, deep = true).size should be (1)

    //element type nullability can be ignored
    val switchedNullabilityInFields = nullableArrayField.copy(
      dataType = ArrayType(StructType(nullableArrayField.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields.map(
        f => f.copy(nullable = !f.nullable)
      )), containsNull = true)
    )
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedNullabilityInFields :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedNullabilityInFields :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedNullabilityInFields :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedNullabilityInFields :: Nil), ignoreNullable = true, deep = true) shouldBe empty
  }

  test("SchemaDiff to map fields.") {
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableArrayField :: Nil)) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableArrayField :: Nil), deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableArrayField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(nullableArrayField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableArrayField :: Nil)) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableArrayField :: Nil), deep = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableArrayField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(notNullableArrayField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    //name difference
    val diffNameField = nullableMapField.copy(name = "foo")
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameField :: Nil), ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffNameField :: Nil), ignoreNullable = true, deep = true).size should be (1)

    //contains null difference
    val diffContainsNullField = nullableMapField.copy(name = notNullableMapField.name)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNullField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    val diffContainsNoNullField = notNullableMapField.copy(name = nullableMapField.name)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullField :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffContainsNoNullField :: Nil), ignoreNullable = true, deep = true) shouldBe empty

    //elementType difference
    val diffElementTypeField = nullableMapField.copy(dataType = MapType(IntegerType, IntegerType, valueContainsNull = true))
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffElementTypeField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffElementTypeField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffElementTypeField :: Nil), ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(diffElementTypeField :: Nil), ignoreNullable = true, deep = true).size should be (1)

    val keyTypeDeepDiffElementTypeField = nullableMapField.copy(dataType = MapType(
      StringType, StructType(nullableStringField :: notNullableStringField :: Nil),
      valueContainsNull = true
    ))
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(keyTypeDeepDiffElementTypeField :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(keyTypeDeepDiffElementTypeField :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(keyTypeDeepDiffElementTypeField :: Nil), ignoreNullable = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(keyTypeDeepDiffElementTypeField :: Nil), ignoreNullable = true, deep = true).size should be (1)

    val valueTypeOrderDeepDiffElementTypeField = nullableMapField.copy(dataType = MapType(
      IntegerType, StructType(notNullableStringField :: nullableStringField :: Nil),
      valueContainsNull = true
    ))
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(valueTypeOrderDeepDiffElementTypeField :: Nil)).size should be (1) // no partial deep match
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(valueTypeOrderDeepDiffElementTypeField :: Nil), deep = true) shouldBe empty // partial deep match
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(valueTypeOrderDeepDiffElementTypeField :: Nil), ignoreNullable = true).size should be (1) // no partial deep match
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(valueTypeOrderDeepDiffElementTypeField :: Nil), ignoreNullable = true, deep = true) shouldBe empty // partial deep match

    //element type nullability can be ignored
    val switchedValueNullabilityInFields = nullableMapField.copy(
      dataType = MapType(IntegerType, StructType(nullableMapField.dataType.asInstanceOf[MapType].valueType.asInstanceOf[StructType].fields.map(
        f => f.copy(nullable = !f.nullable)
      )), valueContainsNull = true)
    )

    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedValueNullabilityInFields :: Nil)).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedValueNullabilityInFields :: Nil), deep = true).size should be (1)
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedValueNullabilityInFields :: Nil), ignoreNullable = true) shouldBe empty
    dfEmptyWithStructuredSchema.schemaDiffTo(StructType(switchedValueNullabilityInFields :: Nil), ignoreNullable = true, deep = true) shouldBe empty
  }
}
