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

import io.smartdatalake.testutils.spark.dataset.Collection._
import io.smartdatalake.testutils.spark.dataset.TestToolDataset
import io.smartdatalake.testutils.{TestTool, TestUtil}
import io.smartdatalake.util.spark.GetSession.loggEnv
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class TransformTest extends AnyFlatSpec with Matchers
  with TestTool with TestToolDataset with Equality with StructTypeUtil {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  private implicit val spark: SparkSession = TestUtil.session

  import spark.implicits._

  loggEnv

  "transformCols" should "square column x only" in {
    val argument = List((0, 0d), (1, 1d), (2, 2d), (3, 3d)).toDF("id", "x")
    val actual = argument.transformCols(renameFun = cn => List(s"${cn}_square"),
      transformFun = cn => List(col(cn) * col(cn)),
      colFilter = _ == "x")
    val expected = List((0, 0d), (1, 1d), (2, 4d), (3, 9d)).toDF("id", "x_square")
    actual.equal(expected) should be(true)
  }

  "transformCols" should "add squared column x" in {
    val argument = List((0, 0d), (1, 1d), (2, 2d), (3, 3d)).toDF("id", "x")
    val actual = argument.transformCols(renameFun = cn => List(s"${cn}_square"),
      transformFun = cn => List(col(cn) * col(cn)),
      colFilter = _ == "x",
      keepOriginalCols = true)
    val expected = List((0, 0d, 0d), (1, 1d, 1d), (2, 2d, 4d), (3, 3d, 9d))
      .toDF("id", "x", "x_square")
    actual.equal(expected) should be(true)
  }

  "castColumnTo" should "cast the type of the column" in {
    val actual = dfTimeRanges.castColumnTo(ByteType)("id")
    val rowsExpected: List[(Byte, Double, String, String)] = List(
      (0, 3.14, "2019-01-01 00:00:00.123456789", "2019-01-05 12:34:56.123456789"),
      (0, 2.72, "2019-01-05 12:34:56.123456789", "2019-02-01 02:34:56.1235"),
      (0, 42.0, "2019-02-01 02:34:56.1235", "2019-02-01 02:34:56.1245"),
      (0, 13.0, "2019-02-01 02:34:56.1245", "2019-03-03 00:00:0"),
      (0, 12.0, "2019-03-03 00:00:0", "2019-04-04 00:00:0"),
      (0, 42.0, "2019-09-05 02:34:56.1231", "2019-09-05 02:34:56.1239"),
      (0, 18.17, "2020-01-01 01:00:0", "9999-12-31 23:59:59.999999999"),
      (1, -1.0, "2019-01-01 00:00:0.123456789", "2019-02-02 00:00:00"),
      (1, -2.0, "2019-03-03 01:00:0", "2021-12-01 02:34:56.1"))
    val expected = rowsExpected.map(makeRowsWithTimeRanges[Byte, Double])
      .toDF("id", "Wert", "valid_from", "valid_to")
    actual.equal(expected) should be(true)
  }

  "castAll2String" should "cast all columns to StringType" in {
    val actual = dfManyTypes.castAll2String
    val expected = actual.columns.foldLeft(actual)({ (df, s) => df.withColumn(s, col(s).cast(StringType)) })
    actual.equal(expected) should be(true)
  }

  "castAllDate2Timestamp" should "cast all DateType columns to TimestampType" in {
    val actual = dfManyTypes.castAllDate2Timestamp
    val expected = actual.withColumn("_date", $"_date".cast(TimestampType))
    actual.equal(expected) should be(true)
  }

  "castColumnsOfTypeTo" should "cast the type of double columns to float" in {
    val actual = dsSimple1.castColumnsOfTypeTo(FloatType)(DoubleType)
    val schemaExpected: StructType = StructType(Array(
      StructField(name = "id", dataType = StringType, nullable = false),
      StructField(name = "n", dataType = IntegerType, nullable = false),
      StructField(name = "x", dataType = FloatType, nullable = false),
      StructField(name = "y", dataType = FloatType, nullable = false)))
    val expected = spark.createDataFrame(ArrayBuffer(Row("A", 1, 0f, 10f)).asJava, schemaExpected)
    actual.equal(expected) should be(true)
  }

  "castDecimalsToIntegralType" should "cast unscaled decimals to an IntegralType" in {
    val actual = dfIntDecimal.castDecimalsToIntegralType()
    val rowsExpected = ArrayBuffer(
      Row(-1, -99.toByte, -9999.toShort, -999999999, -999999999999999999L, BigDecimal(Long.MinValue), BigDecimal(-1.1)),
      Row(0, 12.toByte, 1234.toShort, 123456789, 123456789012345678L, BigDecimal(Long.MaxValue) + java.math.BigDecimal.ONE, BigDecimal(0.123)),
      Row(1, 99.toByte, 9999.toShort, 999999999, 999999999999999999L, BigDecimal(Long.MaxValue), BigDecimal(1.2345))
    ).asJava
    val schema_expected = createStruct(Array[(String, DataType)](("id", IntegerType),
      ("deci_byte", ByteType), ("deci_short", ShortType), ("deci_int", IntegerType),
      ("deci_long", LongType), ("deci_tolong", DecimalType(19, 0)), ("deci", DecimalType(8, 4)))
    )
    val expected = spark.createDataFrame(rowsExpected, schema_expected)
    actual.equal(expected) should be(true)
  }

  "castDecimalsToIntegralType" should "cast dataFrame with many types properly" in {
    val actual = dfManyTypes.castAllDecimal2IntegralFloat
    val expected = actual
      .withColumn("_decimal_2_0", $"_decimal_2_0".cast(ByteType))
      .withColumn("_decimal_4_0", $"_decimal_4_0".cast(ShortType))
      .withColumn("_decimal_10_0", $"_decimal_10_0".cast(IntegerType))
      .withColumn("_decimal_11_0", $"_decimal_11_0".cast(LongType))
      .withColumn("_decimal_4_3", $"_decimal_4_3".cast(FloatType))
      .withColumn("_decimal_38_1", $"_decimal_38_1".cast(DoubleType))
    actual.equal(expected) should be(true)
  }

  "castDecimalsToIntegralType with parameter strict=false" should "cast BigDecimal( Long.MaxValue + 1) to Long.MinValue" in {
    val actual = dfIntDecimal.castDecimalsToIntegralType(strict = false)
    val rowsExpected = ArrayBuffer(
      Row(-1, -99.toByte, -9999.toShort, -999999999, -999999999999999999L, Long.MinValue, BigDecimal(-1.1)),
      Row(0, 12.toByte, 1234.toShort, 123456789, 123456789012345678L, None, BigDecimal(0.123)),
      Row(1, 99.toByte, 9999.toShort, 999999999, 999999999999999999L, Long.MaxValue, BigDecimal(1.2345))
    ).asJava
    val schema_expected = createStruct(Array[(String, DataType)](("id", IntegerType),
      ("deci_byte", ByteType), ("deci_short", ShortType), ("deci_int", IntegerType),
      ("deci_long", LongType), ("deci_tolong", LongType), ("deci", DecimalType(8, 4)))
    )
    val expected = spark.createDataFrame(rowsExpected, schema_expected)
    actual.equal(expected) should be(true)
  }

  "castDecimalsToIntegralType" should "cast unscaled decimals to DecimalType(38, 0)" in {
    val actual = dfIntDecimal.castDecimalsToIntegralType(typOpt = Some(DecimalType(38, 0)))
    val schema_expected = createStruct(Array[(String, DataType)](("id", IntegerType),
      ("deci_byte", DecimalType(38, 0)), ("deci_short", DecimalType(38, 0)), ("deci_int", DecimalType(38, 0)),
      ("deci_long", DecimalType(38, 0)), ("deci_tolong", DecimalType(38, 0)), ("deci", DecimalType(8, 4)))
    )
    val expected = spark.createDataFrame(rowsIntDecimal, schema_expected)
    actual.equal(expected) should be(true)
  }

  "castMapsToArrays" should "cast its columns of MapType to ArrayType" in {
    val actual = dfMapIntInt.castMapsToArrays
    actual.equal(dfArray) shouldBe true
  }

  "decomposeArrayColumn" should "decompose array column without exception" in {
    val indexMap = SortedMap(0 -> "element_0", 1 -> "element_1", 2 -> "element_2", 3 -> "element_3")
    val actual = dfArray2.decomposeArrayColumn(arrayCol = $"arr", indexMap = indexMap)
    val expectedSchema = createStruct(Array[(String, DataType)](
      ("id", IntegerType), ("arr", ArrayType(IntegerType, containsNull = true)),
      ("element_0", IntegerType), ("element_1", IntegerType),
      ("element_2", IntegerType), ("element_3", IntegerType)))
    val rowsExpected: java.util.List[Row] = ArrayBuffer(
      Row(-2, None, None, None, None, None),
      Row(-1, Some(List(None)), None, None, None, None),
      Row(0, Some(Nil), None, None, None, None),
      Row(1, List(Some(0)), Some(0), None, None, None),
      Row(2, Some(List(Some(0), Some(1))), Some(0), Some(1), None, None),
      Row(3, Some(List(None, Some(1), Some(2))), None, Some(1), Some(2), None)
    ).asJava
    val expected = spark.createDataFrame(rowsExpected, expectedSchema)
    actual.equal(expected) should be(true)
  }

  "explodeArrays" should "explode its columns of ArrayType" in {
    val actual = dfArray.select($"exponent", $"powerfun", $"powerfun".as("array_2")).explodeArrays
    val expectedSchema = StructType(Array(
      StructField("exponent", IntegerType, nullable = false),
      StructField("powerfun",
        StructType(List(
          StructField("key", IntegerType, nullable = false),
          StructField("value", IntegerType, nullable = false))),
        nullable = false),
      StructField("array_2",
        StructType(List(
          StructField("key", IntegerType, nullable = false),
          StructField("value", IntegerType, nullable = false))),
        nullable = false)))
    val expectedRows: java.util.List[Row] = ArrayBuffer(
      Row(1, (1, 1), (1, 1)),
      Row(1, (1, 1), (2, 2)),
      Row(1, (1, 1), (3, 3)),
      Row(1, (2, 2), (1, 1)),
      Row(1, (2, 2), (2, 2)),
      Row(1, (2, 2), (3, 3)),
      Row(1, (3, 3), (1, 1)),
      Row(1, (3, 3), (2, 2)),
      Row(1, (3, 3), (3, 3)),
      Row(2, (1, 1), (1, 1)),
      Row(2, (1, 1), (2, 4)),
      Row(2, (1, 1), (3, 9)),
      Row(2, (2, 4), (1, 1)),
      Row(2, (2, 4), (2, 4)),
      Row(2, (2, 4), (3, 9)),
      Row(2, (3, 9), (1, 1)),
      Row(2, (3, 9), (2, 4)),
      Row(2, (3, 9), (3, 9)),
      Row(3, (1, 1), (1, 1)),
      Row(3, (1, 1), (2, 8)),
      Row(3, (1, 1), (3, 27)),
      Row(3, (2, 8), (1, 1)),
      Row(3, (2, 8), (2, 8)),
      Row(3, (2, 8), (3, 27)),
      Row(3, (3, 27), (1, 1)),
      Row(3, (3, 27), (2, 8)),
      Row(3, (3, 27), (3, 27))
    ).asJava
    val expected = spark.createDataFrame(expectedRows, expectedSchema)
    actual.equal(expected) shouldBe true
  }

  "explodeMaps" should "explode its columns of MapType" in {
    val actual = dfMap.explodeMaps
    val expectedSchema = StructType(Array(
      StructField("id", StringType, nullable = false),
      StructField("xMap_key", IntegerType, nullable = false),
      StructField("xMap_value", DoubleType, nullable = true)))
    val expectedRows: java.util.List[Row] = ArrayBuffer(
      Row("A", 0, Some(0d)),
      Row("A", 1, Some(1d))).asJava
    val expected = spark.createDataFrame(expectedRows, expectedSchema)

    actual.equal(expected) shouldBe true
  }

  "explodeArrays(castMapsToArrays)" should "explode its columns of ArrayType and MapType" in {
    val actual = dfMapIntInt.castMapsToArrays.explodeArrays

    val expectedSchema = StructType(List(
      StructField("exponent", IntegerType, nullable = false),
      StructField("powerfun",
        StructType(List(
          StructField("key", IntegerType, nullable = false),
          StructField("value", IntegerType, nullable = false))),
        nullable = false)))
    val expectedRows: java.util.List[Row] = ArrayBuffer(
      Row(1, (1, 1)),
      Row(1, (2, 2)),
      Row(1, (3, 3)),
      Row(2, (1, 1)),
      Row(2, (2, 4)),
      Row(2, (3, 9)),
      Row(3, (1, 1)),
      Row(3, (2, 8)),
      Row(3, (3, 27))
    ).asJava
    val expected = spark.createDataFrame(expectedRows, expectedSchema)

    actual.equal(expected) shouldBe true
  }

  "given a wide dataFrame which has nullable columns, colsToRows" should "transform it to a long dataframe and keep nulls" in {

    val wideDF = List(
      (1, Option(1), Option(10)),
      (2, Option(2), None),
      (3, Option(3), Option(30))
    ).toDF("id", "a", "b")

    val expectedDF = List(
      (1, "a", Option(1)),
      (2, "a", Option(2)),
      (3, "a", Option(3)),
      (1, "b", Option(10)),
      (2, "b", None),
      (3, "b", Option(30))
    ).toDF("id", "feature", "value")

    val longDF = wideDF.colsToRows(keyName = "feature", idCols = List("id"))

    longDF.equal(expectedDF) shouldBe true
  }

  "given a wide dataFrame which has nullable and non-nullable columns, colsToRows" should "transform it to a long dataframe with a nullable columns and keep nulls" in {

    val wideDF = List(
      (1, 1, Option(10)),
      (2, 2, None),
      (3, 3, Option(30))
    ).toDF("id", "a", "b")

    val expectedDF = List(
      (1, "a", Option(1)),
      (2, "a", Option(2)),
      (3, "a", Option(3)),
      (1, "b", Option(10)),
      (2, "b", None),
      (3, "b", Option(30))
    ).toDF("id", "feature", "value")

    val longDF = wideDF.colsToRows(keyName = "feature", idCols = List("id"))

    longDF.equal(expectedDF) shouldBe true
    // note : this implies that the feature column is nullable
  }

  "given a wide dataFrame with various data-types, colsToRows" should "fail because data-types have been mixed in the wide dataFrame" in {
    val wideDF = List(
      (1, "a", 1)
    ).toDF("id", "char", "num")

    assertThrows[java.lang.IllegalArgumentException](wideDF.colsToRows(idCols = List("id")))
  }

  "enumerateGroups" should "enumerate the gtg straenge" in {
    val inputDF = List(
      ("S0_A", 1L, "GTGA"),
      ("S0_A", 2L, "GTGA"),
      ("S0_A", 3L, "GTGB"),
      ("S0_A", 4L, "GTGB"),
      ("S0_A", 5L, "GTGC"),
      ("S0_A", 6L, "GTGC"),
      ("S0_A", 7L, "GTGC"),
      ("S0_B", 1L, "GTGA"),
      ("S0_B", 2L, "GTGA")
    ).toDF("id_section", "sample_nb", "id_gtg_strang")

    // this represents the calculation of e.g. gtgstrang_List_nb
    val enumeratedDf = inputDF.enumerateGroups(
      attr = "id_gtg_strang",
      keyCols = List($"id_section"),
      orderCols = List($"sample_nb"),
      condition = $"id_gtg_strang_prev" === $"id_gtg_strang"
    )

    val expectedDF = List(
      ("S0_A", 1L, "GTGA", 1L),
      ("S0_A", 2L, "GTGA", 1L),
      ("S0_A", 3L, "GTGB", 2L),
      ("S0_A", 4L, "GTGB", 2L),
      ("S0_A", 5L, "GTGC", 3L),
      ("S0_A", 6L, "GTGC", 3L),
      ("S0_A", 7L, "GTGC", 3L),
      ("S0_B", 1L, "GTGA", 1L),
      ("S0_B", 2L, "GTGA", 1L)
    ).toDF("id_section", "sample_nb", "id_gtg_strang", "nb")

    enumeratedDf.equal(expectedDF) should be
    true
  }

  "given a dataframe with nulls in attr, enumerateGroups" should "enumerate the gtg straenge" in {
    val inputDF = List(
      ("S0_A", 1L, "GTGA"),
      ("S0_A", 2L, "GTGA"),
      ("S0_A", 3L, "GTGB"),
      ("S0_A", 4L, "GTGB"),
      ("S0_A", 5L, null),
      ("S0_A", 6L, null),
      ("S0_A", 7L, null)
    ).toDF("id_section", "sample_nb", "id_gtg_strang")

    // this represents the calculation of e.g. gtgstrang_List_nb
    val enumeratedDf = inputDF.enumerateGroups(
      attr = "id_gtg_strang",
      keyCols = List($"id_section"),
      orderCols = List($"sample_nb"),
      // null is not equal to null, each entry gets its own nb for samples 5L,6L,7L
      condition = $"id_gtg_strang_prev" === $"id_gtg_strang"
    )

    val expectedDF = List(
      ("S0_A", 1L, "GTGA", 1L),
      ("S0_A", 2L, "GTGA", 1L),
      ("S0_A", 3L, "GTGB", 2L),
      ("S0_A", 4L, "GTGB", 2L),
      ("S0_A", 5L, null, 3L),
      ("S0_A", 6L, null, 4L),
      ("S0_A", 7L, null, 5L)
    ).toDF("id_section", "sample_nb", "id_gtg_strang", "nb")

    enumeratedDf.equal(expectedDF) should be
    true
  }

  "given a datafraAme with nulls in keyCol, enumerateGroups" should "enumerate the gtg straenge" in {
    val inputDF = List(
      ("S0_A", 1L, "GTGA"),
      ("S0_A", 2L, "GTGA"),
      (null, 3L, "GTGB"),
      (null, 4L, "GTGB"),
      (null, 5L, "GTGE"),
      ("S0_A", 6L, "GTGC"),
      ("S0_A", 7L, "GTGC")
    ).toDF("id_section", "sample_nb", "id_gtg_strang")

    // this represents the calculation
    val enumeratedDf = inputDF.enumerateGroups(
      attr = "id_gtg_strang",
      keyCols = List($"id_section"),
      orderCols = List($"sample_nb"),
      condition = $"id_gtg_strang_prev" === $"id_gtg_strang"
    )

    // null acts as a single key
    val expectedDF = List(
      ("S0_A", 1L, "GTGA", 1L),
      ("S0_A", 2L, "GTGA", 1L),
      (null, 3L, "GTGB", 1L),
      (null, 4L, "GTGB", 1L),
      (null, 5L, "GTGE", 2L),
      ("S0_A", 6L, "GTGC", 2L),
      ("S0_A", 7L, "GTGC", 2L)
    ).toDF("id_section", "sample_nb", "id_gtg_strang", "nb")

    enumeratedDf.equal(expectedDF) should be
    true
  }

  "renameCols" should "rename all columns" in {
    val actual = dfArray2.renameCols(renameFun = cn => s"abc_$cn")
    val expected = dfArray2
      .select($"id".as("abc_id"), $"arr".as("abc_arr"))
    actual.equal(expected) should be(true)
  }

  "renameCols" should "add all renamed columns" in {
    val actual = dfArray2.renameCols(renameFun = cn => s"abc_$cn", keepOriginalCols = true)
    val expected = dfArray2
      .select($"id",
        $"id".as("abc_id"),
        $"arr",
        $"arr".as("abc_arr"))
    actual.equal(expected) should be(true)
  }

  "renameCols" should "rename column arr only" in {
    val actual = dfArray2.renameCols(renameFun = cn => s"abc_$cn", List("arr").contains)
    val expected = dfArray2
      .select($"id", $"arr".as("abc_arr"))
    actual.equal(expected) should be(true)
  }

  "data frame structs" should "be unfolded" in {
    val actual = df_struct.unfoldStructs(fullSubcolName = false)
    val expectedSchema: StructType = createStruct(Array[(String, DataType)](
      ("n", IntegerType),
      ("is_prime", BooleanType),
      ("is_square", BooleanType),
      ("is_even", BooleanType),
      ("divided_by", ArrayType(numberTyp, containsNull = false)),
      ("smaller_coprime", ArrayType(numberTyp, containsNull = false))))
    val rowsExpected: java.util.List[Row] = ArrayBuffer(
      Row(1, false, true, false, List(oneRow), List(oneRow)),
      Row(2, true, false, true, List(oneRow, twoRow), List(oneRow)),
      Row(3, true, false, false, List(oneRow, threeRow), List(oneRow, twoRow)),
      Row(4, false, true, true, List(oneRow, fourRow), List(oneRow, threeRow)),
      Row(5, true, false, false, List(oneRow, fiveRow), List(oneRow, twoRow, threeRow, fourRow)),
      Row(6, false, false, true, List(oneRow, twoRow, threeRow), List(oneRow, fiveRow))
    ).asJava
    val expected = spark.createDataFrame(rowsExpected, expectedSchema)
    actual.equal(expected) shouldBe true
  }

  "data frame structs" should "be unfolded but not nested structs" in {
    val actual = df_struct.unfoldStructs(nested = false)
    val expectedSchema: StructType = createStruct(Array[(String, DataType)](
      ("number·n", IntegerType),
      ("number·property", propertyTyp),
      ("relation·divided_by", ArrayType(numberTyp, containsNull = false)),
      ("relation·smaller_coprime", ArrayType(numberTyp, containsNull = false))))
    val rowsExpected: java.util.List[Row] = ArrayBuffer(
      Row(1, Row(false, true, false), List(oneRow), List(oneRow)),
      Row(2, Row(true, false, true), List(oneRow, twoRow), List(oneRow)),
      Row(3, Row(true, false, false), List(oneRow, threeRow), List(oneRow, twoRow)),
      Row(4, Row(false, true, true), List(oneRow, fourRow), List(oneRow, threeRow)),
      Row(5, Row(true, false, false), List(oneRow, fiveRow), List(oneRow, twoRow, threeRow, fourRow)),
      Row(6, Row(false, false, true), List(oneRow, twoRow, threeRow), List(oneRow, fiveRow))
    ).asJava
    val expected = spark.createDataFrame(rowsExpected, expectedSchema)
    actual.equal(expected) shouldBe true
  }

  "fromUtc" should "convert all timestamps from UTC to Zurich" in {
    val actual = dfTemporalPeriods.fromUtc()
    val expected = List(
      (1, Some(0.0), Some(1.2), 3.14, Timestamp.valueOf("2020-01-01 01:00:00"), doomsTime),
      (1, Some(0.7), Some(1.4), -0.17, Timestamp.valueOf("2020-01-01 01:00:00"), Timestamp.valueOf("2020-07-01 01:59:59.999")),
      (1, Some(1.2), Some(1.4), 42.0, Timestamp.valueOf("2020-07-01 02:00:00"), doomsTime),
      (1, Some(1.4), Some(2.0), 0.0, Timestamp.valueOf("2020-01-01 01:00:00"), Timestamp.valueOf("2020-10-01 01:59:59.999")),
      (1, Some(1.4), Some(3.0), 13.17, Timestamp.valueOf("2020-10-01 02:00:00"), doomsTime),
      (1, Some(3.0), Some(4.0), -2.72, Timestamp.valueOf("2020-01-01 01:00:00"), doomsTime),
      (1, None, Some(5.0), 100.1, Timestamp.valueOf("2020-01-01 01:00:00"), Timestamp.valueOf("2020-04-01 01:59:59.999")),
      (1, Some(4.0), Some(5.0), 9.87, Timestamp.valueOf("2020-04-01 02:00:00"), doomsTime)
    ).toDF("id", "x", "y", "wert", "valid_from", "valid_to")
    actual.equal(expected) should be(true)
  }

  "toUtc" should "convert all timestamps from Zurich to UTC" in {
    val actual = dfTemporalPeriods.toUtc()
    val expected = List(
      (1, Some(0.0), Some(1.2), 3.14, Timestamp.valueOf("2019-12-31 23:0:0"), doomsTime),
      (1, Some(0.7), Some(1.4), -0.17, Timestamp.valueOf("2019-12-31 23:0:0"), Timestamp.valueOf("2020-06-30 21:59:59.999")),
      (1, Some(1.2), Some(1.4), 42.0, Timestamp.valueOf("2020-06-30 22:00:00"), doomsTime),
      (1, Some(1.4), Some(2.0), 0.0, Timestamp.valueOf("2019-12-31 23:0:0"), Timestamp.valueOf("2020-09-30 21:59:59.999")),
      (1, Some(1.4), Some(3.0), 13.17, Timestamp.valueOf("2020-09-30 22:00:00"), doomsTime),
      (1, Some(3.0), Some(4.0), -2.72, Timestamp.valueOf("2019-12-31 23:0:0"), doomsTime),
      (1, None, Some(5.0), 100.1, Timestamp.valueOf("2019-12-31 23:0:0"), Timestamp.valueOf("2020-03-31 21:59:59.999")),
      (1, Some(4.0), Some(5.0), 9.87, Timestamp.valueOf("2020-03-31 22:00:00"), doomsTime)
    ).toDF("id", "x", "y", "wert", "valid_from", "valid_to")
    actual.equal(expected) should be(true)
  }


  /** * tests for unpivotCast, transpose et al ** */

  "colsToRows" should "transform it to a long dataframe" in {

    val wideDF = List(
      (1, 1, 10),
      (2, 2, 20),
      (3, 3, 30)
    ).toDF("id", "a", "b")

    val expectedDF = List(
      (1, "a", 1),
      (2, "a", 2),
      (3, "a", 3),
      (1, "b", 10),
      (2, "b", 20),
      (3, "b", 30)
    ).toDF("id", "feature", "value")

    val longDF = wideDF.colsToRows(keyName = "feature", idCols = List("id"))

    // note, this also implies that the feature-column is non-nullable
    longDF.equal(expectedDF) shouldBe true
  }

  "unpivotCast" should "unpivotCast df_SnapshotsWithGaps" in {
    val actual = dfSnapshotsWithGaps.unpivotCast(keys = Array($"id", $"dt"), colNamesToPivot = Array("x", "y"))
    val expectedSchema: StructType = createStruct(Array[(String, DataType, Boolean)](("id", IntegerType, false),
      ("dt", IntegerType, false),
      ("x", StringType, false),
      ("y", DoubleType, true)))
    val rowsExpected: java.util.List[Row] = ArrayBuffer(
      Row(0, 20190101, "x", Some(3.14)),
      Row(0, 20190102, "x", Some(3.14)),
      Row(0, 20190103, "x", Some(2.72)),
      Row(0, 20190104, "x", None),
      Row(0, 20190106, "x", Some(1.0)),
      Row(0, 20190201, "x", Some(3.14)),
      Row(0, 20190207, "x", Some(1.0)),
      Row(1, 20190101, "x", Some(42.0)),
      Row(1, 20190102, "x", None),
      Row(1, 20190103, "x", Some(-21.3)),
      Row(1, 20190104, "x", None),
      Row(0, 20190101, "y", None),
      Row(0, 20190102, "y", Some(-2.37)),
      Row(0, 20190103, "y", Some(4.57)),
      Row(0, 20190104, "y", None),
      Row(0, 20190106, "y", Some(3.0)),
      Row(0, 20190201, "y", None),
      Row(0, 20190207, "y", Some(2.5)),
      Row(1, 20190101, "y", None),
      Row(1, 20190102, "y", None),
      Row(1, 20190103, "y", None),
      Row(1, 20190104, "y", None))
      .asJava
    val expected = spark.createDataFrame(rowsExpected, expectedSchema)
    actual.equal(expected) shouldBe true
  }

  "unpivotCast" should "unpivotCast DataFrame with different types" in {
    val argument = dfSnapshotsWithGaps.select($"id", $"dt", $"x", $"y".cast(IntegerType))
    val actual = argument.unpivotCast(keys = Array($"id", $"dt"), colNamesToPivot = Array("x", "y"))
    val expectedSchema: StructType = createStruct(Array[(String, DataType, Boolean)](("id", IntegerType, false),
      ("dt", IntegerType, false),
      ("x", StringType, false),
      ("y", DoubleType, true)))
    val rowsExpected: java.util.List[Row] = ArrayBuffer(
      Row(0, 20190101, "x", Some(3.14)),
      Row(0, 20190102, "x", Some(3.14)),
      Row(0, 20190103, "x", Some(2.72)),
      Row(0, 20190104, "x", None),
      Row(0, 20190106, "x", Some(1.0)),
      Row(0, 20190201, "x", Some(3.14)),
      Row(0, 20190207, "x", Some(1.0)),
      Row(1, 20190101, "x", Some(42.0)),
      Row(1, 20190102, "x", None),
      Row(1, 20190103, "x", Some(-21.3)),
      Row(1, 20190104, "x", None),
      Row(0, 20190101, "y", None),
      Row(0, 20190102, "y", Some(-2.0)),
      Row(0, 20190103, "y", Some(4.0)),
      Row(0, 20190104, "y", None),
      Row(0, 20190106, "y", Some(3.0)),
      Row(0, 20190201, "y", None),
      Row(0, 20190207, "y", Some(2.0)),
      Row(1, 20190101, "y", None),
      Row(1, 20190102, "y", None),
      Row(1, 20190103, "y", None),
      Row(1, 20190104, "y", None))
      .asJava
    val expected = spark.createDataFrame(rowsExpected, expectedSchema)
    actual.equal(expected) shouldBe true
  }

  "unpivotCast" should "unpivotCast DataFrame with column of type string" in {
    val argument = dfSnapshotsWithGaps.castColumnTo(StringType)("dt")
    val actual = argument.unpivotCast(keys = Array($"id"), colNamesToPivot = Array("dt", "x"))
    val expectedSchema: StructType = createStruct(Array[(String, DataType, Boolean)](("id", IntegerType, false),
      ("x", StringType, false),
      ("y", StringType, true)))
    val rowsExpected: java.util.List[Row] = ArrayBuffer(
      Row(0, "dt", Some("20190101")),
      Row(0, "dt", Some("20190102")),
      Row(0, "dt", Some("20190103")),
      Row(0, "dt", Some("20190104")),
      Row(0, "dt", Some("20190106")),
      Row(0, "dt", Some("20190201")),
      Row(0, "dt", Some("20190207")),
      Row(0, "x", None),
      Row(0, "x", Some("1.0")),
      Row(0, "x", Some("1.0")),
      Row(0, "x", Some("2.72")),
      Row(0, "x", Some("3.14")),
      Row(0, "x", Some("3.14")),
      Row(0, "x", Some("3.14")),
      Row(1, "dt", Some("20190101")),
      Row(1, "dt", Some("20190102")),
      Row(1, "dt", Some("20190103")),
      Row(1, "dt", Some("20190104")),
      Row(1, "x", None),
      Row(1, "x", None),
      Row(1, "x", Some("-21.3")),
      Row(1, "x", Some("42.0"))
    ).asJava
    val expected = spark.createDataFrame(rowsExpected, expectedSchema)
    actual.equal(expected) shouldBe true
  }

  "transpose" should "transpose empty DataFrame" in {
    val actual = dfSnapshotsWithGaps.where(lit(false)).transposeCustom(11)
    val expectedSchema: StructType = createStruct(Array[(String, DataType, Boolean)](("_column", StringType, false)))
    val rowsExpected: java.util.List[Row] = ArrayBuffer(
      Row("id"), Row("dt"), Row("x"), Row("y")).asJava
    val expected = spark.createDataFrame(rowsExpected, expectedSchema)
    actual.equal(expected) shouldBe true
  }

  "transpose" should "transpose the first 11 rows of a DataFrame" in {
    val actual = dfSnapshotsWithGaps.transposeCustom(11)
    val expectedSchema: StructType = createStruct(Array[(String, DataType, Boolean)](
      ("_column", StringType, false),
      ("_000", DoubleType, true),
      ("_001", DoubleType, true),
      ("_002", DoubleType, true),
      ("_003", DoubleType, true),
      ("_004", DoubleType, true),
      ("_005", DoubleType, true),
      ("_006", DoubleType, true),
      ("_007", DoubleType, true),
      ("_008", DoubleType, true),
      ("_009", DoubleType, true),
      ("_010", DoubleType, true)))
    val rowsExpected: java.util.List[Row] = ArrayBuffer(
      Row("id", Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(0.0), Some(1.0), Some(1.0), Some(1.0), Some(1.0)),
      Row("dt", Some(20190101.0), Some(20190102.0), Some(20190103.0), Some(20190104.0), Some(20190106.0), Some(20190201.0), Some(20190207.0), Some(20190101.0), Some(20190102.0), Some(20190103.0), Some(20190104.0)),
      Row("x", Some(3.14), Some(3.14), Some(2.72), None, Some(1.0), Some(3.14), Some(1.0), Some(42.0), None, Some(-21.3), None),
      Row("y", None, Some(-2.37), Some(4.57), None, Some(3.0), None, Some(2.5), None, None, None, None)
    ).asJava
    val expected = spark.createDataFrame(rowsExpected, expectedSchema)
    actual.equal(expected) shouldBe true
  }


  /** * tests for dataset to curry frame ** */

  "dataSet2curryFrame" should "return curried dataFrame" in {
    val argument = List((-2, 7, 9, 8, -2798f),
      (0, 0, 0, 0, 0f), (0, 0, 1, 2, 12f), (0, 0, 4, 2, 42f), (0, 1, 0, 0, 100f),
      (1, 2, 3, 4, 1234f), (1, 7, 0, 0, 1700f)
    ).toDF("x", "y", "z", "t", "val")
    val actual = SortedMap[Int, Map[Int, Map[Int, Map[Int, Float]]]]() ++
      argument.dataSet2curryFrame(pkCols = List("x", "y", "z", "t"))
        .as[Map[Int, Map[Int, Map[Int, Map[Int, Float]]]]].head
    val expected = SortedMap[Int, Map[Int, Map[Int, Map[Int, Float]]]]() ++ Map(
      -2 -> Map(7 -> Map(9 -> Map(8 -> -2798f))),
      0 -> Map(
        0 -> Map(0 -> Map(0 -> 0f), 1 -> Map(2 -> 12f), 4 -> Map(2 -> 42f)),
        1 -> Map(0 -> Map(0 -> 100f))),
      1 -> Map(2 -> Map(3 -> Map(4 -> 1234f)), 7 -> Map(0 -> Map(0 -> 1700f))))
    actual should be(expected)
  }

  "dataSet2curryFrame" should "return ordinary map if dataset contains one PK col only" in {
    val argument = dfIdX.where(org.apache.spark.sql.functions.not($"x".isNaN))
    val actual = SortedMap[Int, Option[Double]]() ++
      argument.dataSet2curryFrame(pkCols = List("id"))
        .as[Map[Int, Option[Double]]].head
    val expected = SortedMap[Int, Option[Double]]() ++
      dfIdXRows.filter { case (_, y) => y.forall(!_.isNaN) }.toMap
    actual should be(expected)
  }

  "dataSet2curryFrame" should "work on Dataset with 2 PK cols" in {
    val argument = List((-2, 7, -27f),
      (0, 0, 0f), (0, 1, 1f), (1, 2, 12f), (1, 7, 17f)
    ).toDF("x", "y", "val")
    val actual = SortedMap[Int, Map[Int, Float]]() ++
      argument.dataSet2curryFrame(pkCols = List("x", "y"))
        .as[Map[Int, Map[Int, Float]]].head
    val expected = SortedMap[Int, Map[Int, Float]]() ++ Map(
      -2 -> Map(7 -> -27f),
      0 -> Map(0 -> 0f, 1 -> 1f),
      1 -> Map(2 -> 12f, 7 -> 17f)
    )
    actual should be(expected)
  }

}
