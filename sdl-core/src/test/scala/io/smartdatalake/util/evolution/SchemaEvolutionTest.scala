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
package io.smartdatalake.util.evolution

import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.DataFrameFunctions
import io.smartdatalake.workflow.dataframe.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.Checkers


/**
  * Unit tests for historization
  *
  */
class SchemaEvolutionTest extends AnyFunSuite with Checkers with SmartDataLakeLogger {

  implicit lazy val session: SparkSession = TestUtil.session

  implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(SparkSubFeed.subFeedType)

  import functions._


  test("Schema with same column names and types need to be identical") {
    val schemaOld = SparkSchema(StructType(List(StructField("a", StringType), StructField("b", IntegerType))))
    val schemaNew = SparkSchema(StructType(List(StructField("a", StringType), StructField("b", IntegerType))))
    assert(SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew, Environment.caseSensitive))
  }

  test("Schema with same column names which differs with upper/lowercase and same types need to be identical in case-insensitive mode") {
    val schemaOld = SparkSchema(StructType(List(StructField("a", StringType), StructField("b", IntegerType))))
    val schemaNew = SparkSchema(StructType(List(StructField("a", StringType), StructField("B", IntegerType))))
    assert(SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew, caseSensitiveComparison = false))
  }

  test("Schema with same column names which differs with upper/lowercase and same types need to be different in case-sensitive mode") {
    val schemaOld = SparkSchema(StructType(List(StructField("a", StringType), StructField("b", IntegerType))))
    val schemaNew = SparkSchema(StructType(List(StructField("a", StringType), StructField("B", IntegerType))))
    assert(!SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew, caseSensitiveComparison = true))
  }

  test("Schema with different columns") {
    val schemaOld = SparkSchema(StructType(List(StructField("a", StringType), StructField("b", IntegerType))))
    val schemaNew = SparkSchema(StructType(List(StructField("a", StringType), StructField("b", IntegerType), StructField("c", IntegerType))))
    assert(!SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew, Environment.caseSensitive))
  }

  test("Different Schema: same column names but different types") {
    val schemaOld = SparkSchema(StructType(List(StructField("a", StringType), StructField("b", IntegerType))))
    val schemaNew = SparkSchema(StructType(List(StructField("a", StringType), StructField("b", StringType))))
    assert(!SchemaEvolution.hasSameColNamesAndTypes(schemaOld, schemaNew, Environment.caseSensitive))
  }

  test("Schema with different columns and difference with upper/lowercase and same types should only result in the actual new columns in case-insensitive mode") {
    val schemaOld = StructType(List(
      StructField("a", StringType),
      StructField("b", IntegerType)
    ))
    val schemaNew = StructType(List(
      StructField("a", StringType),
      StructField("B", IntegerType),
      StructField("c", IntegerType)
    ))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldDf, newDf))
    assert(SchemaEvolution.newColumns(oldDf, newDf, caseSensitive = false).toSet == Set("c"))
    assert(SchemaEvolution.deletedColumns(oldDf, newDf).isEmpty)
  }

  test("Schema with different columns and difference with upper/lowercase and same types should result in counting uppercase as new column as well in case-sensitive mode") {
    val schemaOld = StructType(List(
      StructField("a", StringType),
      StructField("b", IntegerType)
    ))
    val schemaNew = StructType(List(
      StructField("a", StringType),
      StructField("B", IntegerType),
      StructField("c", IntegerType)
    ))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldDf, newDf, caseSensitiveComparison = true))
    assert(SchemaEvolution.newColumns(oldDf, newDf, caseSensitive = true).toSet == Set("B", "c"))
    assert(SchemaEvolution.deletedColumns(oldDf, newDf, caseSensitive = true).toSet == Set("b"))
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

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
    assert(oldEvoDf.columns == newEvoDf.columns)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    //column names are delivered lower case in case-insensitive mode
    assert(SchemaEvolution.newColumns(oldDf, newDf).toSet == Set("sf_new_str_1", "sf_new_double_1", "sf_new_double_2", "sf_new_double"))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))

    // compare in lowercase as new columns are delivered in lowercase in case-insensitive mode
    assert(oldEvoDf.columns.map(_.toLowerCase()).toSet == schemaNew.map(_.name.toLowerCase()).toSet)
    assert(newEvoDf.columns.map(_.toLowerCase()).toSet == schemaNew.map(_.name.toLowerCase()).toSet)

    assert(oldEvoDf.count > 0)
    assert(newEvoDf.count > 0)
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

    val df = SparkDataFrame(TestUtil.arbitraryDataFrame(schema))

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

    assert(colSortedDf.columns.map(c => c) == order)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    //column names are delivered lower case in case-insensitive
    assert(SchemaEvolution.deletedColumns(oldDf, newDf).toSet == Set("sf_str_4", "sf_time_1", "sf_str_5", "sf_str_6"))

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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))

    // compare in lowercase as new columns are delivered in lowercase in case-insensitive mode
    val deletedCols = SchemaEvolution.deletedColumns(oldDf, newDf)
    assert(oldEvoDf.columns.map(_.toLowerCase()).toSet == schemaNew.map(_.name.toLowerCase()).toSet ++ deletedCols )
    assert(newEvoDf.columns.map(_.toLowerCase()).toSet == schemaNew.map(_.name.toLowerCase()).toSet ++ deletedCols)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val colsToIgnore = Seq("dl_ts_captured", "dl_ts_delimited")
    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, colsToIgnore)
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))

    // compare in lowercase as new columns are delivered in lowercase in case-insensitive mode
    assert(oldEvoDf.columns.map(_.toLowerCase()).toSet == schemaNew.map(_.name.toLowerCase()).toSet ++ colsToIgnore)
    assert(newEvoDf.columns.map(_.toLowerCase()).toSet == schemaNew.map(_.name.toLowerCase()).toSet)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema.fields.map(s => s.dataType).distinct.forall(_.typeName == "string"))

    assert(oldEvoDf.count > 0)
    assert(newEvoDf.count > 0)
  }

  test("Columns of result are ordered by default according to oldDf, then newColumns, then cols2Ignore") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", IntegerType), StructField("c", IntegerType),StructField("dl_ts_captured", TimestampType),StructField("dl_ts_delimited", TimestampType)))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", IntegerType), StructField("d", IntegerType)))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val colsToIgnore = Seq("dl_ts_captured", "dl_ts_delimited")
    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, colsToIgnore)
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))

    val tgtColOrder = Seq("a","b","c","d")
    assert(oldEvoDf.columns == tgtColOrder ++ colsToIgnore)
    assert(newEvoDf.columns == tgtColOrder)
  }

  test("New column in struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType),StructField("b3", IntegerType))))))

    schemaNew.fields.foreach(f => println(s"${f.name}: typeName=${f.dataType.typeName} simpleName==${f.dataType.simpleString} sql===${f.dataType.sql}"))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema.getDataType("b").asInstanceOf[SparkStructDataType].getDataType("b3").typeName.equalsIgnoreCase("int"))

    oldEvoDf.cache
    assert(oldEvoDf.count > 0)
    assert(oldEvoDf.where(col("b.b3").isNull).count > 0)
    assert(newEvoDf.count > 0)
  }

  test("Changed data type in struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", StructType(List(StructField("b1", IntegerType),StructField("b2", StringType))))))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema.getDataType("b").asInstanceOf[SparkStructDataType].getDataType("b2").typeName == "string")

    assert(oldEvoDf.count > 0)
    assert(newEvoDf.count > 0)
  }

  test("Changed data type of array type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", ArrayType(FloatType))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", ArrayType(DoubleType))))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema.getDataType("b").asInstanceOf[SparkArrayDataType].elementDataType.typeName == "double")

    assert(oldEvoDf.select(explode(col("b"))).count > 0)
    assert(newEvoDf.select(explode(col("b"))).count > 0)
  }

  test("New column in array type of struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType)))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType),StructField("b3", IntegerType)))))))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(oldEvoDf.schema.getDataType("b").asInstanceOf[SparkArrayDataType].elementDataType.asInstanceOf[SparkStructDataType].getDataType("b3").typeName == "int")

    assert(oldEvoDf.select(explode(col("b.b3"))).count > 0)
    assert(newEvoDf.select(explode(col("b.b3"))).count > 0)
  }

  test("Deleted column in array type of struct type") {

    val schemaOld = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType),StructField("b3", IntegerType)))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("b", ArrayType(StructType(List(StructField("b1", IntegerType),StructField("b2", IntegerType)))))))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, ignoreOldDeletedNestedColumns = false)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf))
    assert(newEvoDf.schema.getDataType("b").asInstanceOf[SparkArrayDataType].elementDataType.asInstanceOf[SparkStructDataType].getDataType("b3").typeName.equalsIgnoreCase("int"))

    assert(oldEvoDf.select(explode(col("b.b3"))).count > 0)
    assert(newEvoDf.select(explode(col("b.b3"))).count > 0)
  }

  test("CaseSensitive: Old and new schema with different sorting are identical, no matter in which order, but newDf is sorted according to oldDf") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

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
    assert(oldEvoDf.columns == newEvoDf.columns)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)
  }


  test("CaseSensitive: New columns: new column exists in addition to existing columns") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    assert(SchemaEvolution.newColumns(oldDf, newDf).toSet == Set("SF_NEW_STR_1", "sf_new_double_1", "sf_new_double_2", "sf_new_double"))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    assert(oldEvoDf.columns.toSet == schemaNew.map(_.name).toSet)
    assert(newEvoDf.columns.toSet == schemaNew.map(_.name).toSet)

    assert(oldEvoDf.count > 0)
    assert(newEvoDf.count > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)
  }

  test("CaseSensitive: DataFrame columns should be sorted in a specific order") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
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

    val df = SparkDataFrame(TestUtil.arbitraryDataFrame(schema))

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

    assert(colSortedDf.columns.map(c => c) == order)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)
  }

  test("CaseSensitive: DataFrame with same column names but different datatypes are recognized") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldDf, newDf, Environment.caseSensitive))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)
  }

  test("CaseSensitive: Column dropped: dropped column still used but with empty values and ignored according to config") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

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
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)

  }

  test("CaseSensitive: Cornercase renamed column: column with old name still exists but empty, new column inserted") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    val deletedCols = SchemaEvolution.deletedColumns(oldDf, newDf)
    assert(oldEvoDf.columns.toSet == schemaNew.map(_.name).toSet ++ deletedCols)
    assert(newEvoDf.columns.toSet == schemaNew.map(_.name).toSet ++ deletedCols)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)
  }

  test("CaseSensitive: New columns and technical cols to ignore") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val colsToIgnore = Seq("dl_ts_captured", "dl_ts_delimited")
    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, colsToIgnore, caseSensitiveComparison = Environment.caseSensitive)
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    assert(oldEvoDf.columns.toSet == schemaNew.map(_.name).toSet ++ colsToIgnore)
    assert(newEvoDf.columns.toSet == schemaNew.map(_.name).toSet)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)
  }

  test("CaseSensitive: Numerical columns can be cast to String") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
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

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))
    assert(oldEvoDf.schema.fields.map(s => s.dataType.typeName).distinct == Seq("string"))

    assert(oldEvoDf.count > 0)
    assert(newEvoDf.count > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)

  }

  test("CaseSensitive: Columns of result are ordered by default according to oldDf, then newColumns, then cols2Ignore") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(StructField("A", StringType), StructField("b", IntegerType), StructField("C", IntegerType), StructField("dl_ts_captured", TimestampType), StructField("dl_ts_delimited", TimestampType)))
    val schemaNew = StructType(List(StructField("A", StringType), StructField("b", IntegerType), StructField("d", IntegerType)))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val colsToIgnore = Seq("dl_ts_captured", "dl_ts_delimited")
    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, colsToIgnore, caseSensitiveComparison = Environment.caseSensitive)
    assert(!SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))

    val tgtColOrder = Seq("A", "b", "C", "d")
    assert(oldEvoDf.columns == tgtColOrder ++ colsToIgnore)
    assert(newEvoDf.columns == tgtColOrder)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)

  }

  test("CaseSensitive: New column in struct type") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(StructField("A", StringType), StructField("b", StructType(List(StructField("b1", IntegerType), StructField("B2", IntegerType))))))
    val schemaNew = StructType(List(StructField("A", StringType), StructField("b", StructType(List(StructField("b1", IntegerType), StructField("B2", IntegerType), StructField("B3", IntegerType))))))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))
    assert(oldEvoDf.schema.getDataType("b").asInstanceOf[SparkStructDataType].getDataType("B3").typeName == "int")

    oldEvoDf.cache
    assert(oldEvoDf.count > 0)
    assert(oldEvoDf.where(col("b.B3").isNull).count > 0)
    assert(newEvoDf.count > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)
  }

  test("CaseSensitive: Changed data type in struct type") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(StructField("A", StringType), StructField("b", StructType(List(StructField("b1", IntegerType), StructField("B2", IntegerType))))))
    val schemaNew = StructType(List(StructField("A", StringType), StructField("b", StructType(List(StructField("b1", IntegerType), StructField("B2", StringType))))))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))
    assert(oldEvoDf.schema.getDataType("b").asInstanceOf[SparkStructDataType].getDataType("B2").typeName == "string")

    assert(oldEvoDf.count > 0)
    assert(newEvoDf.count > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)
  }

  test("CaseSensitive: Changed data type of array type") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(StructField("a", StringType), StructField("B", ArrayType(FloatType))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("B", ArrayType(DoubleType))))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))
    assert(oldEvoDf.schema.getDataType("B").asInstanceOf[SparkArrayDataType].elementDataType.typeName == "double")

    assert(oldEvoDf.select(explode(col("B"))).count > 0)
    assert(newEvoDf.select(explode(col("B"))).count > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)

  }

  test("CaseSensitive: New column in array type of struct type") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(StructField("A", StringType), StructField("B", ArrayType(StructType(List(StructField("b1", IntegerType), StructField("b2", IntegerType)))))))
    val schemaNew = StructType(List(StructField("A", StringType), StructField("B", ArrayType(StructType(List(StructField("b1", IntegerType), StructField("b2", IntegerType), StructField("B3", IntegerType)))))))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))
    assert(oldEvoDf.schema.getDataType("B").asInstanceOf[SparkArrayDataType].elementDataType.asInstanceOf[SparkStructDataType].getDataType("B3").typeName == "int")

    assert(oldEvoDf.select(explode(col("B.B3"))).count > 0)
    assert(newEvoDf.select(explode(col("B.B3"))).count > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)

  }

  test("CaseSensitive: Deleted column in array type of struct type") {

    // Prepare case sensitivity
    val previousCaseSensitive = session.conf.get(SQLConf.CASE_SENSITIVE.key)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = true)
    Environment._caseSensitive = Some(true)

    val schemaOld = StructType(List(StructField("a", StringType), StructField("B", ArrayType(StructType(List(StructField("b1", IntegerType), StructField("b2", IntegerType), StructField("B3", IntegerType)))))))
    val schemaNew = StructType(List(StructField("a", StringType), StructField("B", ArrayType(StructType(List(StructField("b1", IntegerType), StructField("b2", IntegerType)))))))

    val oldDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaOld))
    val newDf = SparkDataFrame(TestUtil.arbitraryDataFrame(schemaNew))

    val (oldEvoDf, newEvoDf) = SchemaEvolution.process(oldDf, newDf, ignoreOldDeletedNestedColumns = false, caseSensitiveComparison = Environment.caseSensitive)
    assert(SchemaEvolution.hasSameColNamesAndTypes(oldEvoDf, newEvoDf, Environment.caseSensitive))
    assert(newEvoDf.schema.getDataType("B").asInstanceOf[SparkArrayDataType].elementDataType.asInstanceOf[SparkStructDataType].getDataType("B3").typeName == "int")

    assert(oldEvoDf.select(explode(col("B.B3"))).count > 0)
    assert(newEvoDf.select(explode(col("B.B3"))).count > 0)

    // clean up case sensitivity
    Environment._caseSensitive = Some(previousCaseSensitive.toBoolean)
    session.conf.set(key = SQLConf.CASE_SENSITIVE.key, value = previousCaseSensitive)

  }

}
