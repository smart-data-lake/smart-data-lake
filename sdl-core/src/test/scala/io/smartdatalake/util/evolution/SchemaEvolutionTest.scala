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
package io.smartdatalake.util.evolution

import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, _}
import org.scalatest.FunSuite
import org.scalatestplus.scalacheck.Checkers


/**
  * Unit tests for historization
  *
  */
class SchemaEvolutionTest extends FunSuite with Checkers with SmartDataLakeLogger {

  implicit lazy val session: SparkSession = TestUtil.session

  test("Schema with same column names and types need to be identical") {
    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", IntegerType)))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", IntegerType)))
    assert(SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew, Environment.caseSensitive))
  }

  test("Schema with different columns") {
    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", IntegerType)))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", IntegerType), StructField("c", IntegerType)))
    assert(!SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew, Environment.caseSensitive))
  }

  test("Different Schema: same column names but different types") {
    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", IntegerType)))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", StringType)))
    assert(!SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew, Environment.caseSensitive))
  }

  test("Old and new schema with different sorting are identical, no matter in which order, but newDf is sorted according to oldDf") {
    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType),
      StructField("SF_STR_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("SF_STR_5", StringType),
      StructField("SF_STR_6", StringType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType),
      StructField("SF_STR_5", StringType),
      StructField("SF_STR_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("SF_STR_6", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    // old -> new
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldDf, newDf))
    assert(SchemaEvolution.newColumns(oldDf, newDf).isEmpty)
    assert(SchemaEvolution.deletedColumns(oldDf, newDf).isEmpty)

    // new -> old
    assert(SchemaEvolution.hasSameColNamesAndTypes(newDf, oldDf))
    assert(SchemaEvolution.newColumns(newDf, oldDf).isEmpty)
    assert(SchemaEvolution.deletedColumns(newDf, oldDf).isEmpty)

    // schema evolution sorts newDf according to oldDf
    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(oldEvoDf.columns.toSeq == newEvoDf.columns.toSeq)
  }

  test("New columns: new column exists in addition to existing columns") {
    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType),
      StructField("SF_STR_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("SF_STR_5", StringType),
      StructField("SF_STR_6", StringType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType),
      StructField("SF_STR_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("SF_STR_5", StringType),
      StructField("SF_STR_6", StringType),
      StructField("SF_NEW_STR_1", StringType),
      StructField("SF_NEW_DOUBLE_1", DoubleType),
      StructField("SF_NEW_DOUBLE_2", DoubleType),
      StructField("SF_NEW_DOUBLE", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    assert(SchemaEvolution.newColumns(oldDf, newDf).toSet == Set("SF_NEW_STR_1", "SF_NEW_DOUBLE_1", "SF_NEW_DOUBLE_2", "SF_NEW_DOUBLE"))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))

    assert(oldEvoDf.columns.toSet == schemaNew.map(_.name).toSet)
    assert(newEvoDf.columns.toSet == schemaNew.map(_.name).toSet)

    assert(oldEvoDf.count()>0)
    assert(newEvoDf.count()>0)
  }

  test("DataFrame columns should be sorted in a specific order") {
    val schema = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("dl_ts_delimited", StringType),
      StructField("dl_ts_captured", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType)
    ))

    val df = TestUtil.arbitraryDataFrame(schema)

    val order = Seq(
      "SF_NR_3",
      "SF_NR_1",
      "SF_NR_2",
      "SF_STR_1",
      "SF_STR_2",
      "SF_NR_4",
      "SF_NR_5",
      "SF_NR_6",
      "SF_STR_3",
      "dl_ts_captured",
      "dl_ts_delimited"
    )
    val colSortedDf = SchemaEvolution.sortColumns(df, order)

    assert(colSortedDf.columns.map(c => c).toSeq == order)
  }

  test("DataFrame with same column names but different datatypes are recognized") {
    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", StringType),
      StructField("SF_NR_2", StringType),
      StructField("SF_NR_3", StringType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", StringType),
      StructField("SF_NR_5", StringType),
      StructField("SF_NR_6", StringType),
      StructField("SF_STR_3", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldDf, newDf))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
  }

  test("Column dropped: dropped column still used but with empty values and ignored according to config") {
    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType),
      StructField("SF_STR_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("SF_STR_5", StringType),
      StructField("SF_STR_6", StringType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    assert(SchemaEvolution.deletedColumns(oldDf, newDf).toSet == Set("SF_STR_4", "SF_TIME_1", "SF_STR_5", "SF_STR_6"))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))

    assert(oldEvoDf.columns.toSet == schemaOld.map(_.name).toSet)
    assert(newEvoDf.columns.toSet == schemaOld.map(_.name).toSet)

    val (oldEvoDf2, newEvoDf2) = SchemaEvolution.process(oldDf, newDf, ignoreOldDeletedColumns = true)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf2, newEvoDf2))

    assert(oldEvoDf2.columns.toSet == schemaNew.map(_.name).toSet)
    assert(newEvoDf2.columns.toSet == schemaNew.map(_.name).toSet)
  }

  test("Cornercase renamed column: column with old name still exists but empty, new column inserted") {
    // column renamed?
    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType),
      StructField("SF_STR_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("SF_STR_5", StringType),
      StructField("SF_STR_6", StringType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType),
      StructField("SF_STR_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("SF_STR_5", StringType),
      StructField("SF_STR_6_1", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))

    val deletedCols = SchemaEvolution.deletedColumns(oldDf, newDf)
    assert(oldEvoDf.columns.toSet == schemaNew.map(_.name).toSet ++ deletedCols )
    assert(newEvoDf.columns.toSet == schemaNew.map(_.name).toSet ++ deletedCols)
  }

  test("New columns and technical cols to ignore") {

    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType),
      StructField("dl_ts_captured", TimestampType),
      StructField("dl_ts_delimited", TimestampType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("SF_STR_1", StringType),
      StructField("SF_STR_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("SF_STR_3", StringType),
      StructField("SF_STR_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("SF_STR_5", StringType),
      StructField("SF_STR_6", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val colsToIgnore = Seq("dl_ts_captured", "dl_ts_delimited")
    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, colsToIgnore)
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))

    assert(oldEvoDf.columns.toSet == schemaNew.map(_.name).toSet ++ colsToIgnore)
    assert(newEvoDf.columns.toSet == schemaNew.map(_.name).toSet)
  }

  test("Numerical columns can be cast to String") {

    val schemaOld = StructType(List(
      StructField("sf_nr_1", IntegerType),
      StructField("sf_nr_2", LongType),
      StructField("sf_nr_3", DoubleType)
    ))

    val schemaNew = StructType(List(
      StructField("sf_nr_1", StringType),
      StructField("sf_nr_2", StringType),
      StructField("sf_nr_3", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema.map(s => s.dataType).distinct == Seq(StringType))

    assert(oldEvoDf.count()>0)
    assert(newEvoDf.count()>0)
  }

  test("Columns of result are ordered by default according to oldDf, then newColumns, then cols2Ignore") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", IntegerType), StructField("c", IntegerType),StructField("dl_ts_captured", TimestampType),StructField("dl_ts_delimited", TimestampType)))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", IntegerType), StructField("d", IntegerType)))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val colsToIgnore = Seq("dl_ts_captured", "dl_ts_delimited")
    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, colsToIgnore)
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))

    val tgtColOrder = Seq("a","b","c","d")
    assert(oldEvoDf.columns.toSeq == tgtColOrder ++ colsToIgnore)
    assert(newEvoDf.columns.toSeq == tgtColOrder)
  }

  test("New column in struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType),StructField("b3", IntegerType))))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema("b").dataType.asInstanceOf[StructType]("b3").dataType == IntegerType)

    oldEvoDf.cache()
    assert(oldEvoDf.count()>0)
    assert(oldEvoDf.where(col("b.b3").isNull).count()>0)
    assert(newEvoDf.count()>0)
  }

  test("Changed data type in struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", StructType(List(StructField("b1", IntegerType),StructField("b2", StringType))))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema("b").dataType.asInstanceOf[StructType]("b2").dataType == StringType)

    assert(oldEvoDf.count()>0)
    assert(newEvoDf.count()>0)
  }

  test("Changed data type of array type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", ArrayType(FloatType))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", ArrayType(DoubleType))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema("b").dataType.asInstanceOf[ArrayType].elementType == DoubleType)

    assert(oldEvoDf.select(explode(col("b"))).count()>0)
    assert(newEvoDf.select(explode(col("b"))).count()>0)
  }

  test("New column in array type of struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType)))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType),StructField("b3", IntegerType)))))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema("b").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]("b3").dataType == IntegerType)

    assert(oldEvoDf.select(explode(col("b.b3"))).count()>0)
    assert(newEvoDf.select(explode(col("b.b3"))).count()>0)
  }

  test("Deleted column in array type of struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType),StructField("b3", IntegerType)))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType)))))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, ignoreOldDeletedNestedColumns = false)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(newEvoDf.schema("b").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]("b3").dataType == IntegerType)

    assert(oldEvoDf.select(explode(col("b.b3"))).count()>0)
    assert(newEvoDf.select(explode(col("b.b3"))).count()>0)
  }

  test("CaseSensitive: Old and new schema with different sorting are identical, no matter in which order, but newDf is sorted according to oldDf") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType),
      StructField("sf_str_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("sf_str_5", StringType),
      StructField("sf_str_6", StringType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType),
      StructField("sf_str_5", StringType),
      StructField("sf_str_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("sf_str_6", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    // old -> new
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldDf, newDf, Environment.caseSensitive))
    assert(SchemaEvolution.newColumns(oldDf, newDf).isEmpty)
    assert(SchemaEvolution.deletedColumns(oldDf, newDf).isEmpty)

    // new -> old
    assert(SchemaEvolution.hasSameColNamesAndTypes(newDf, oldDf, Environment.caseSensitive))
    assert(SchemaEvolution.newColumns(newDf, oldDf).isEmpty)
    assert(SchemaEvolution.deletedColumns(newDf, oldDf).isEmpty)

    // schema evolution sorts newDf according to oldDf
    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(oldEvoDf.columns.toSeq == newEvoDf.columns.toSeq)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)
  }


  test("CaseSensitive: New columns: new column exists in addition to existing columns") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType),
      StructField("sf_str_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("sf_str_5", StringType),
      StructField("sf_str_6", StringType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType),
      StructField("sf_str_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("sf_str_5", StringType),
      StructField("sf_str_6", StringType),
      StructField("SF_NEW_STR_1", StringType),
      StructField("sf_new_double_1", DoubleType),
      StructField("sf_new_double_2", DoubleType),
      StructField("sf_new_double", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    assert(SchemaEvolution.newColumns(oldDf, newDf).toSet == Set("SF_NEW_STR_1", "sf_new_double_1", "sf_new_double_2", "sf_new_double"))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    assert(oldEvoDf.columns.toSet == schemaNew.map(_.name).toSet)
    assert(newEvoDf.columns.toSet == schemaNew.map(_.name).toSet)

    assert(oldEvoDf.count() > 0)
    assert(newEvoDf.count() > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)
  }

  test("CaseSensitive: DataFrame columns should be sorted in a specific order") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schema = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("dl_ts_delimited", StringType),
      StructField("dl_ts_captured", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType)
    ))

    val df = TestUtil.arbitraryDataFrame(schema)

    val order = Seq(
      "SF_NR_3",
      "SF_NR_1",
      "SF_NR_2",
      "sf_str_1",
      "sf_str_2",
      "SF_NR_4",
      "SF_NR_5",
      "SF_NR_6",
      "sf_str_3",
      "dl_ts_captured",
      "dl_ts_delimited"
    )
    val colSortedDf = SchemaEvolution.sortColumns(df, order, Environment.caseSensitive)

    assert(colSortedDf.columns.map(c => c).toSeq == order)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)
  }

  test("CaseSensitive: DataFrame with same column names but different datatypes are recognized") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", StringType),
      StructField("SF_NR_2", StringType),
      StructField("SF_NR_3", StringType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", StringType),
      StructField("SF_NR_5", StringType),
      StructField("SF_NR_6", StringType),
      StructField("sf_str_3", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldDf, newDf, Environment.caseSensitive))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)
  }

  test("CaseSensitive: Column dropped: dropped column still used but with empty values and ignored according to config") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType),
      StructField("sf_str_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("sf_str_5", StringType),
      StructField("sf_str_6", StringType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    assert(SchemaEvolution.deletedColumns(oldDf, newDf).toSet == Set("sf_str_4", "SF_TIME_1", "sf_str_5", "sf_str_6"))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    assert(oldEvoDf.columns.toSet == schemaOld.map(_.name).toSet)
    assert(newEvoDf.columns.toSet == schemaOld.map(_.name).toSet)

    val (oldEvoDf2, newEvoDf2) = SchemaEvolution.process(oldDf, newDf, ignoreOldDeletedColumns = true, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf2, newEvoDf2, Environment.caseSensitive))

    assert(oldEvoDf2.columns.toSet == schemaNew.map(_.name).toSet)
    assert(newEvoDf2.columns.toSet == schemaNew.map(_.name).toSet)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)

  }

  test("CaseSensitive: Cornercase renamed column: column with old name still exists but empty, new column inserted") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    // column renamed?
    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType),
      StructField("sf_str_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("sf_str_5", StringType),
      StructField("sf_str_6", StringType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType),
      StructField("sf_str_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("sf_str_5", StringType),
      StructField("sf_str_6_1", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    val deletedCols = SchemaEvolution.deletedColumns(oldDf, newDf)
    assert(oldEvoDf.columns.toSet == schemaNew.map(_.name).toSet ++ deletedCols)
    assert(newEvoDf.columns.toSet == schemaNew.map(_.name).toSet ++ deletedCols)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)
  }

  test("CaseSensitive: New columns and technical cols to ignore") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType),
      StructField("dl_ts_captured", TimestampType),
      StructField("dl_ts_delimited", TimestampType)
    ))

    val schemaNew = StructType(List(
      StructField("SF_NR_1", IntegerType),
      StructField("SF_NR_2", IntegerType),
      StructField("SF_NR_3", IntegerType),
      StructField("sf_str_1", StringType),
      StructField("sf_str_2", StringType),
      StructField("SF_NR_4", IntegerType),
      StructField("SF_NR_5", IntegerType),
      StructField("SF_NR_6", IntegerType),
      StructField("sf_str_3", StringType),
      StructField("sf_str_4", StringType),
      StructField("SF_TIME_1", TimestampType),
      StructField("sf_str_5", StringType),
      StructField("sf_str_6", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val colsToIgnore = Seq("dl_ts_captured", "dl_ts_delimited")
    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, colsToIgnore, caseSensitiveComparison = Environment.caseSensitive)
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    assert(oldEvoDf.columns.toSet == schemaNew.map(_.name).toSet ++ colsToIgnore)
    assert(newEvoDf.columns.toSet == schemaNew.map(_.name).toSet)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)
  }

  test("CaseSensitive: Numerical columns can be cast to String") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(
      StructField("sf_nr_1", IntegerType),
      StructField("SF_NR_2", LongType),
      StructField("sf_nr_3", DoubleType)
    ))

    val schemaNew = StructType(List(
      StructField("sf_nr_1", StringType),
      StructField("SF_NR_2", StringType),
      StructField("sf_nr_3", StringType)
    ))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))
    assert(oldEvoDf.schema.map(s => s.dataType).distinct == Seq(StringType))

    assert(oldEvoDf.count() > 0)
    assert(newEvoDf.count() > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)

  }

  test("CaseSensitive: Columns of result are ordered by default according to oldDf, then newColumns, then cols2Ignore") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(StructField("A", StringType), StructField("b", IntegerType), StructField("C", IntegerType), StructField("dl_ts_captured", TimestampType), StructField("dl_ts_delimited", TimestampType)))
    val schemaNew = StructType(List(StructField("A", StringType), StructField("b", IntegerType), StructField("d", IntegerType)))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val colsToIgnore = Seq("dl_ts_captured", "dl_ts_delimited")
    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, colsToIgnore, caseSensitiveComparison = Environment.caseSensitive)
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    val tgtColOrder = Seq("A", "b", "C", "d")
    assert(oldEvoDf.columns.toSeq == tgtColOrder ++ colsToIgnore)
    assert(newEvoDf.columns.toSeq == tgtColOrder)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)

  }

  test("CaseSensitive: New column in struct type") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(StructField("A", StringType), StructField("b", StructType(List(StructField("b1", IntegerType), StructField("B2", IntegerType))))))
    val schemaNew = StructType(List(StructField("A", StringType), StructField("b", StructType(List(StructField("b1", IntegerType), StructField("B2", IntegerType), StructField("B3", IntegerType))))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))
    assert(oldEvoDf.schema("b").dataType.asInstanceOf[StructType]("B3").dataType == IntegerType)

    oldEvoDf.cache()
    assert(oldEvoDf.count() > 0)
    assert(oldEvoDf.where(col("b.B3").isNull).count() > 0)
    assert(newEvoDf.count() > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)
  }

  test("CaseSensitive: Changed data type in struct type") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(StructField("A", StringType), StructField("b", StructType(List(StructField("b1", IntegerType), StructField("B2", IntegerType))))))
    val schemaNew = StructType(List(StructField("A", StringType), StructField("b", StructType(List(StructField("b1", IntegerType), StructField("B2", StringType))))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))
    assert(oldEvoDf.schema("b").dataType.asInstanceOf[StructType]("B2").dataType == StringType)

    assert(oldEvoDf.count() > 0)
    assert(newEvoDf.count() > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)
  }

  test("CaseSensitive: Changed data type of array type") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(StructField("a", StringType), StructField("B", ArrayType(FloatType))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("B", ArrayType(DoubleType))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))
    assert(oldEvoDf.schema("B").dataType.asInstanceOf[ArrayType].elementType == DoubleType)

    assert(oldEvoDf.select(explode(col("B"))).count() > 0)
    assert(newEvoDf.select(explode(col("B"))).count() > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(SQLConf.CASE_SENSITIVE.key, previousCaseSensitive)

  }

}
