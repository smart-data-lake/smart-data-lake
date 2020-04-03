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

import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers


/**
  * Unit tests for historization
  *
  */
class SchemaEvolutionTest extends FunSuite with Checkers with SmartDataLakeLogger {

  implicit lazy val session: SparkSession = TestUtil.sessionWithoutHive

  test("Schema with same column names and types need to be identical") {
    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", IntegerType)))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", IntegerType)))
    assert(SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew))
  }

  test("Schema with different columns") {
    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", IntegerType)))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", IntegerType), StructField("c", IntegerType)))
    assert(!SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew))
  }

  test("Different Schema: same column names but different types") {
    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", IntegerType)))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", StringType)))
    assert(!SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew))
  }

  test("Old and new schema with different sorting are identical, no matter in which order") {
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

    assert(oldEvoDf.count>0)
    assert(newEvoDf.count>0)
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

    assert(oldEvoDf.count>0)
    assert(newEvoDf.count>0)
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

    oldEvoDf.cache
    assert(oldEvoDf.count>0)
    assert(oldEvoDf.where(col("b.b3").isNull).count>0)
    assert(newEvoDf.count>0)
  }

  test("Changed data type in struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", StructType(List(StructField("b1", IntegerType),StructField("b2", StringType))))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema("b").dataType.asInstanceOf[StructType]("b2").dataType == StringType)

    assert(oldEvoDf.count>0)
    assert(newEvoDf.count>0)
  }

  test("Changed data type of array type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", ArrayType(FloatType))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", ArrayType(DoubleType))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema("b").dataType.asInstanceOf[ArrayType].elementType == DoubleType)

    assert(oldEvoDf.select(explode(col("b"))).count>0)
    assert(newEvoDf.select(explode(col("b"))).count>0)
  }

  test("New column in array type of struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType)))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType),StructField("b3", IntegerType)))))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema("b").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]("b3").dataType == IntegerType)

    assert(oldEvoDf.select(explode(col("b.b3"))).count>0)
    assert(newEvoDf.select(explode(col("b.b3"))).count>0)
  }

  test("Deleted column in array type of struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType),StructField("b3", IntegerType)))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType)))))))

    val oldDf = TestUtil.arbitraryDataFrame(schemaOld)
    val newDf = TestUtil.arbitraryDataFrame(schemaNew)

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, ignoreOldDeletedNestedColumns = false)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(newEvoDf.schema("b").dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]("b3").dataType == IntegerType)

    assert(oldEvoDf.select(explode(col("b.b3"))).count>0)
    assert(newEvoDf.select(explode(col("b.b3"))).count>0)
  }
}
