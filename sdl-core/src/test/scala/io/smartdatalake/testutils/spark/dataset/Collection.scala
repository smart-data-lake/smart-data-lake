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

package io.smartdatalake.testutils.spark.dataset

import io.smartdatalake.app.AppUtil
import io.smartdatalake.util.spark.dataset.Types
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalacheck.Gen
import org.scalacheck.Gen.{alphaStr, gaussian, nonEmptyListOf, poisson}

import java.sql.{Date, Timestamp}
import scala.Double.{NaN, NegativeInfinity, PositiveInfinity}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * A collection of DataSets
 * mainly used for testing
 * */
object Collection extends Types {

  private val spark = AppUtil.createSparkSession(name = "TransformTest", enableHive = false)

  import spark.implicits._

  val doomsTime: Timestamp = Timestamp.valueOf("9999-12-31 00:00:00")

  /** * Some simple DataSets ** */
  private val schemaSimple: StructType = StructType(Array(
    StructField(name = "id", dataType = StringType, nullable = false),
    StructField(name = "n", dataType = IntegerType, nullable = false),
    StructField(name = "x", dataType = FloatType, nullable = false),
    StructField(name = "y", dataType = DoubleType, nullable = false)))
  val dfSimple1: DataFrame = spark.createDataFrame(ArrayBuffer(Row("A", 1, 0f, 10d)).asJava, schemaSimple)
  val dsSimple1: Dataset[(String, Int, Float, Double)] = dfSimple1.as[(String, Int, Float, Double)]
  val dsSimple2: Dataset[(String, Int, Float, Double)] = spark.createDataFrame(ArrayBuffer(Row("A", 2, 0.1f, 9.9d)).asJava, schemaSimple)
    .as[(String, Int, Float, Double)]

  val schemaNull: StructType = StructType(Array(
    StructField(name = "id", dataType = StringType, nullable = false),
    StructField(name = "x", dataType = IntegerType, nullable = true)))
  val dsNull: Dataset[(String, Int)] = spark.createDataFrame(ArrayBuffer(Row("A", None)).asJava, schemaNull).as[(String, Int)]

  type complexType = (Int, List[(String, String, List[String])])
  val rowsComplex: List[complexType] = List(
    (1, List(("a", "A", List("a", "A")))),
    (2, List(("b", "B", List("b", "B")))),
    (3, List(("c", "C", List("c", "C")))),
    (4, List(("d", "D", List("d", "D")))),
    (5, List(("e", "E", List("e", "E"))))
  )
  val dsComplex: Dataset[complexType] = rowsComplex
    .toDF("id", "value")
    .as[complexType]

  type complexTypeWithNull = (Option[Int], Option[List[(String, String, Option[List[String]])]])
  val rowsComplexWithNull: List[complexTypeWithNull] = List(
    (Some(1), Some(List(("a", "A", Some(List("a", "A")))))),
    (Some(2), Some(List(("b", "B", Some(List("b", "B")))))),
    (Some(3), Some(List(("c", "C", None)))),
    (Some(4), Some(List(("d", "D", Some(List("d", "D")))))),
    (Some(5), None),
    (None, None)
  )
  val dsComplexWithNull: Dataset[complexTypeWithNull] = rowsComplexWithNull
    .toDF("id", "value").as[(Option[Int], Option[List[(String, String, Option[List[String]])]])]

  val dfTemporalPeriods: DataFrame = List(
    (1, Some(0.0), Some(1.2), 3.14, Timestamp.valueOf("2020-01-01 00:00:00"), doomsTime),
    (1, Some(0.7), Some(1.4), -0.17, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-06-30 23:59:59.999")),
    (1, Some(1.2), Some(1.4), 42.0, Timestamp.valueOf("2020-07-01 00:00:00"), doomsTime),
    (1, Some(1.4), Some(2.0), 0.0, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-09-30 23:59:59.999")),
    (1, Some(1.4), Some(3.0), 13.17, Timestamp.valueOf("2020-10-01 00:00:00"), doomsTime),
    (1, Some(3.0), Some(4.0), -2.72, Timestamp.valueOf("2020-01-01 00:00:00"), doomsTime),
    (1, None, Some(5.0), 100.1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-03-31 23:59:59.999")),
    (1, Some(4.0), Some(5.0), 9.87, Timestamp.valueOf("2020-04-01 00:00:00"), doomsTime)
  ).toDF("id", "x", "y", "wert", "valid_from", "valid_to")
  val dfSnapshotsWithGaps: DataFrame = List(
    (0, 20190101, Some(3.14), None),
    (0, 20190102, Some(3.14), Some(-2.37)),
    (0, 20190103, Some(2.72), Some(4.57)),
    (0, 20190104, None, None),
    (0, 20190106, Some(1.0), Some(3.0)),
    (0, 20190201, Some(3.14), None),
    (0, 20190207, Some(1.0), Some(2.5)),
    (1, 20190101, Some(42.0), None),
    (1, 20190102, None, None),
    (1, 20190103, Some(-21.3), None),
    (1, 20190104, None, None))
    .toDF("id", "dt", "x", "y")

  val dfIdXRows: List[(Int, Option[Double])] = List((-4, Some(NegativeInfinity)), (-3, Some(PositiveInfinity)),
    (-2, Some(NaN)), (-1, None),
    (0, Some(0.1)), (2, Some(0.9999)), (4, Some(1d)),
    (1, Some(-0.1)), (3, Some(-0.9999)), (5, Some(-1d)),
    (6, Some(1.0001)), (8, Some(3d)), (10, Some(42d)),
    (7, Some(-1.0001)), (9, Some(-3d)), (11, Some(-42d)),
    (100, Some(0d))
  )
  val dfIdX: DataFrame = dfIdXRows.toDF("id", "x")

  // hierarchical data frame
  val rowsHierarchy: List[(Byte, String, String)] = List(
    (1, "a", "ab"), (2, "a", "ac"), (3, "ac", "aca"), (4, "b", "ba"),
    (5, "c", "ca"), (6, "ca", "caa"), (7, "ca", "cab"), (8, "c", "cb"),
    (9, "cb", "X"), (10, "c", "cc"), (11, "cc", "X"), (12, "X", "Y"), (13, "Y", "Z"))
  val dfHierarchy: DataFrame = rowsHierarchy.toDF("id", "parent", "child")

  // DataFrame with nLets
  val rowsnLets: List[(String, String)] = List(("1let", "Unilet"),
    ("2let", "doublet"), ("2let", "doublet"),
    ("3let", "triplet"), ("3let", "triplet"), ("3let", "triplet"),
    ("4let", "quatriplet"), ("4let", "quatriplet"), ("4let", "quatriplet"), ("4let", "quatriplet"))
  val dfnLets: DataFrame = rowsnLets.toDF("id", "name")


  /** * DataFrame with Decimals ** */
  val rowsIntDecimal: java.util.List[Row] = ArrayBuffer(
    Row(-1, BigDecimal(-99), BigDecimal(-9999), BigDecimal(-999999999), BigDecimal(-999999999999999999L), BigDecimal(Long.MinValue), BigDecimal(-1.1)),
    Row(0, BigDecimal(12), BigDecimal(1234), BigDecimal(123456789), BigDecimal(123456789012345678L), BigDecimal(Long.MaxValue) + java.math.BigDecimal.ONE, BigDecimal(0.123)),
    Row(1, BigDecimal(99), BigDecimal(9999), BigDecimal(999999999), BigDecimal(999999999999999999L), BigDecimal(Long.MaxValue), BigDecimal(1.2345))
  ).asJava
  private val schemaIntDecimal = createStruct(Array[(String, DataType)](("id", IntegerType),
    ("deci_byte", DecimalType(2, 0)), ("deci_short", DecimalType(4, 0)), ("deci_int", DecimalType(9, 0)),
    ("deci_long", DecimalType(18, 0)), ("deci_tolong", DecimalType(19, 0)), ("deci", DecimalType(8, 4)))
  )
  val dfIntDecimal: DataFrame = spark.createDataFrame(rowsIntDecimal, schemaIntDecimal)

  /** * DataFrame with Time Range ** */
  def makeRowsWithTimeRanges[A, B](zeile: (A, B, String, String)): (A, B, Timestamp, Timestamp) = (zeile._1, zeile._2, Timestamp.valueOf(zeile._3), Timestamp.valueOf(zeile._4))

  private val rowsTimeRanges: List[(Int, Double, String, String)] = List(
    (0, 3.14, "2019-01-01 00:00:00.123456789", "2019-01-05 12:34:56.123456789"),
    (0, 2.72, "2019-01-05 12:34:56.123456789", "2019-02-01 02:34:56.1235"),
    (0, 42.0, "2019-02-01 02:34:56.1235", "2019-02-01 02:34:56.1245"),
    (0, 13.0, "2019-02-01 02:34:56.1245", "2019-03-03 00:00:0"),
    (0, 12.0, "2019-03-03 00:00:0", "2019-04-04 00:00:0"),
    (0, 42.0, "2019-09-05 02:34:56.1231", "2019-09-05 02:34:56.1239"),
    (0, 18.17, "2020-01-01 01:00:0", "9999-12-31 23:59:59.999999999"),
    (1, -1.0, "2019-01-01 00:00:0.123456789", "2019-02-02 00:00:00"),
    (1, -2.0, "2019-03-03 01:00:0", "2021-12-01 02:34:56.1"))
  val dfTimeRanges: DataFrame = rowsTimeRanges.map(makeRowsWithTimeRanges[Int, Double])
    .toDF("id", "Wert", "valid_from", "valid_to")

  /** * DataFrames with Map ** */
  private val schemaMap: StructType = StructType(Array(
    StructField(name = "id", dataType = StringType, nullable = false),
    StructField(name = "xMap", dataType = MapType(IntegerType, DoubleType), nullable = true)))
  val dfMap: DataFrame = spark.createDataFrame(
    ArrayBuffer(Row("A", Map(0 -> 0d, 1 -> 1d))).asJava, schemaMap)
  val dfMap2: DataFrame = spark.createDataFrame(
    ArrayBuffer(Row("A", Map(0 -> Math.scalb(1d, -17), 1 -> 1d))).asJava, schemaMap)

  private val schemaMapIntInt: StructType = StructType(Array(
    StructField(name = "exponent", dataType = IntegerType, nullable = false),
    StructField(name = "powerfun",
      dataType = MapType(IntegerType, IntegerType, valueContainsNull = false), nullable = false)))
  private val dfMapIntIntRows = ArrayBuffer(
    Row(1, Map(1 -> 1, 2 -> 2, 3 -> 3)),
    Row(2, Map(1 -> 1, 2 -> 4, 3 -> 9)),
    Row(3, Map(1 -> 1, 2 -> 8, 3 -> 27))
  ).asJava
  val dfMapIntInt: DataFrame = spark.createDataFrame(dfMapIntIntRows, schemaMapIntInt)

  /** * DataFrames with Array ** */
  private val dfArraySchema = StructType(List(
    StructField("exponent", IntegerType, nullable = false),
    StructField("powerfun",
      ArrayType(
        StructType(List(
          StructField("key", IntegerType, nullable = false),
          StructField("value", IntegerType, nullable = false))),
        containsNull = false),
      nullable = false)))
  private val dfArrayRows: java.util.List[Row] = ArrayBuffer(
    Row(1, List((1, 1), (2, 2), (3, 3))),
    Row(2, List((1, 1), (2, 4), (3, 9))),
    Row(3, List((1, 1), (2, 8), (3, 27)))
  ).asJava
  val dfArray: DataFrame = spark.createDataFrame(dfArrayRows, dfArraySchema)
  val dfArray2: DataFrame = {
    List((-2, None),
      (-1, Some(List(None))),
      (0, Some(Nil)),
      (1, Some(List(Some(0)))),
      (2, Some(List(Some(0), Some(1)))),
      (3, Some(List(None, Some(1), Some(2))))
    ).toDF("id", "arr")
  }

  /** * DataFrame with Structs ** */
  val propertyTyp: StructType = createStruct(Array[(String, DataType)](
    ("is_prime", BooleanType), ("is_square", BooleanType), ("is_even", BooleanType)))
  val numberTyp: StructType = createStruct(Array[(String, DataType)](("n", IntegerType), ("property", propertyTyp)))
  val schemaStruct: StructType = createStruct(Array[(String, DataType)](
    ("number", numberTyp),
    ("relation",
      createStruct(Array[(String, DataType)](
        ("divided_by", ArrayType(numberTyp, containsNull = false)),
        ("smaller_coprime", ArrayType(numberTyp, containsNull = false))))
    )))
  //                                    (prime,square,even)
  val oneRow: Row = Row(1, Row(false, true, false))
  val twoRow: Row = Row(2, Row(true, false, true))
  val threeRow: Row = Row(3, Row(true, false, false))
  val fourRow: Row = Row(4, Row(false, true, true))
  val fiveRow: Row = Row(5, Row(true, false, false))
  val sixRow: Row = Row(6, Row(false, false, true))
  val rowStruct: java.util.List[Row] = ArrayBuffer(
    //     n        Teiler            teilerfremd
    Row(oneRow, Row(List(oneRow), List(oneRow))),
    Row(twoRow, Row(List(oneRow, twoRow), List(oneRow))),
    Row(threeRow, Row(List(oneRow, threeRow), List(oneRow, twoRow))),
    Row(fourRow, Row(List(oneRow, fourRow), List(oneRow, threeRow))),
    Row(fiveRow, Row(List(oneRow, fiveRow), List(oneRow, twoRow, threeRow, fourRow))),
    Row(sixRow, Row(List(oneRow, twoRow, threeRow), List(oneRow, fiveRow)))
  ).asJava
  val df_struct: DataFrame = spark.createDataFrame(rowStruct, schemaStruct)


  def makeRowManyTypes(r: (Boolean, Int, Int, Int, Int, String, String, String, String, String, String, Double, Double, String, String, String)): Row = {
    Row(r._1, r._2.byteValue(), r._3.shortValue(), r._4, r._5.longValue(), // BooleanType - LongType
      Decimal(new java.math.BigDecimal(r._6), 2, 0),
      Decimal(new java.math.BigDecimal(r._7), 4, 0),
      Decimal(new java.math.BigDecimal(r._8), 10, 0),
      Decimal(new java.math.BigDecimal(r._9), 11, 0),
      Decimal(new java.math.BigDecimal(r._10), 4, 3),
      Decimal(new java.math.BigDecimal(r._11), 38, 1),
      r._12.floatValue(), r._13, Date.valueOf(r._14), Timestamp.valueOf(r._15), r._16) // FloatType - StringType
  }

  val rowsManyTypes: List[(Boolean, Int, Int, Int, Int, String, String, String, String, String, String, Double, Double, String, String, String)] = List(
    (false, 0, 0, 0, 0, "0", "0", "0", "0", "0.0", "0.0", 0.0, 0.0, "1970-01-01", "1970-01-01 02:34:56.789", "zero"),
    (true, 127, 32767, Int.MaxValue, Int.MaxValue, "99", "9999", "9999999999", "99999999999", "1.234", "1234567890123456789012345678901234567.8",
      Float.MaxValue, Double.MaxValue, "2020-02-29", "2020-02-29 12:34:56.789", "maximal")
  )

  def dfManyTypes: DataFrame = {
    val schemaManyTypes: StructType = StructType(
      StructField("_boolean", BooleanType, nullable = true) ::
        StructField("_byte", ByteType, nullable = true) ::
        StructField("_short", ShortType, nullable = true) ::
        StructField("_integer", IntegerType, nullable = true) ::
        StructField("_long", LongType, nullable = true) ::
        StructField("_decimal_2_0", DecimalType(2, 0), nullable = true) ::
        StructField("_decimal_4_0", DecimalType(4, 0), nullable = true) ::
        StructField("_decimal_10_0", DecimalType(10, 0), nullable = true) ::
        StructField("_decimal_11_0", DecimalType(11, 0), nullable = true) ::
        StructField("_decimal_4_3", DecimalType(4, 3), nullable = true) ::
        StructField("_decimal_38_1", DecimalType(38, 1), nullable = true) ::
        StructField("_float", FloatType, nullable = true) ::
        StructField("_double", DoubleType, nullable = true) ::
        StructField("_date", DateType, nullable = true) ::
        StructField("_timestamp", TimestampType, nullable = true) ::
        StructField("_string", StringType, nullable = true) ::
        Nil)
    spark.createDataFrame(rowsManyTypes.map(makeRowManyTypes).asJava, schemaManyTypes): DataFrame
  }

  /** Generated DataFrames */

  val genExactRows: Gen[List[(Int, String)]] = nonEmptyListOf(
    g = Gen.zip(poisson(rate = 256d), alphaStr))
  lazy val genExactFrame: Gen[DataFrame] = genExactRows.map { rows: List[(Int, String)] =>
    rows
      .zipWithIndex.map(ri => (ri._2, ri._1._1, ri._1._2))
      .toDF("id", "n", "str")
  }
  val genInexactRows: Gen[List[(Int, String, Double)]] = nonEmptyListOf(
    g = Gen.zip(poisson(rate = 256d), alphaStr, gaussian(mean = 0d, stdDev = 16d)))
  lazy val genInexactFrame: Gen[DataFrame] = genInexactRows.map { rows: List[(Int, String, Double)] =>
    rows
      .zipWithIndex.map(ri => (ri._2, ri._1._1, ri._1._2, ri._1._3))
      .toDF("id", "n", "str", "x")
  }
}
