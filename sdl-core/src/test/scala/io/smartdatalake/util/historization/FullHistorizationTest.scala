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
package io.smartdatalake.util.historization

import io.smartdatalake.definitions.TechnicalTableColumn
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.historization.HistorizationTestUtils._
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Unit tests for historization
 *
 */
class FullHistorizationTest extends FunSuite with BeforeAndAfter with SmartDataLakeLogger {

  private implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  test("History unchanged with new columns but unchanged data") {

    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing)
    logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy", "Test"), (124, "Erna", 27, "healthy", null))
    val dfNewFeed = toDataDf(baseColumnsNewFeed, colNames :+ "new_col1")
    logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val (oldEvolvedDf, newEvolvedDf) = SchemaEvolution.process(dfOldHist, dfNewFeed,
      colsToIgnore = Seq(TechnicalTableColumn.captured, TechnicalTableColumn.delimited))

    val dfHistorized = Historization.fullHistorize(oldEvolvedDf, newEvolvedDf, primaryKeyColumns, referenceTimestampNew, None, None)
    logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    //ID 123 has new value and so old record is closed with newcol1=null
    val dfResultExistingRowNonNull = toHistorizedDf(baseColumnsOldHist.filter(_._1==123), HistorizationPhase.UpdatedOld)
      .withColumn("new_col1", lit(null).cast(StringType))

    //ID 123 has new value and so new record is created with newcol1="Test"
    val dfResultNewRowNonNull = toHistorizedDf(baseColumnsNewFeed.filter(_._1==123), HistorizationPhase.UpdatedNew, colNames :+ "new_col1")

    //ID 124 has null new value and so old record is left unchanged but with additional column
    val dfResultExistingRowNull = toHistorizedDf(baseColumnsOldHist.filter(_._1==124), HistorizationPhase.Existing)
      .withColumn("new_col1", lit(null).cast(StringType))

    val dfExpected = dfResultExistingRowNonNull
      .unionByName(dfResultNewRowNonNull)
      .unionByName(dfResultExistingRowNull)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("History unchanged with new columns but unchanged data")(dfHistorized)(dfExpected)
    assert(result)
  }

  ignore("History unchanged when deleting columns but unchanged data") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing)
    logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)

    val dfNewFeedWithDeletedCols = dfNewFeed.drop("health_state")
    logger.debug(s"New feed:\n${dfNewFeedWithDeletedCols.showString()}")

    val dfHistorized = Historization.fullHistorize(dfOldHist, dfNewFeedWithDeletedCols, primaryKeyColumns, referenceTimestampNew, None, None)
    logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUpdatedOld = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfExpected = toHistorizedDf(baseColumnsUpdatedOld, HistorizationPhase.Existing)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("History unchanged when deleting columns but unchanged data")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("The history should stay unchanged when using the current load again") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing)
    logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.fullHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
    logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUnchanged = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(baseColumnsUnchanged, HistorizationPhase.Existing)

    val dfExpected = dfUnchanged

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("The history should stay unchanged when using the current load again")(dfHistorized)(dfExpected)
    assert(result)
  }


  test("History should stay unchanged when using current load but with different column sorting") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing)
    logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed).select($"age", $"health_state", $"id", $"name")
    logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.fullHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
    logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUnchanged = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(baseColumnsUnchanged, HistorizationPhase.Existing)

    val dfExpected = dfUnchanged

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("History should stay unchanged when using current load but with different column sorting")(dfHistorized)(dfExpected)
    assert(result)
  }


  test("When updating 1 record, the history should contain the old and the new version of the values") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing)
    logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "sick"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.fullHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
    logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUpdatedOld = List((123, "Egon", 23, "healthy"))
    val dfUpdatedOld = toHistorizedDf(baseColumnsUpdatedOld, HistorizationPhase.UpdatedOld)

    val baseColumnsUpdatedNew = List((123, "Egon", 23, "sick"))
    val dfUpdatedNew = toHistorizedDf(baseColumnsUpdatedNew, HistorizationPhase.UpdatedNew)

    val baseColumnsUnchanged = List((124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(baseColumnsUnchanged, HistorizationPhase.Existing)

    val dfExpected = dfUpdatedNew.union(dfUpdatedOld).union(dfUnchanged)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("When updating 1 record, the history should contain the old and the new version of the values")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When deleting 1 record (technical deletion) the dl_ts_delimited column should be updated") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing)
    logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.fullHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
    logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUpdatedOld = List((123, "Egon", 23, "healthy"))
    val dfUpdatedOld = toHistorizedDf(baseColumnsUpdatedOld, HistorizationPhase.UpdatedOld)

    val baseColumnsUnchanged = List((124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(baseColumnsUnchanged, HistorizationPhase.Existing)

    val dfExpected = dfUpdatedOld.union(dfUnchanged)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("When deleting 1 record (technical deletion) the dl_ts_delimited column should be updated")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When adding 1 record, the history should contain the new record") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing)
    logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"),
      (125, "Edeltraut", 54, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.fullHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
    logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUnchanged = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(baseColumnsUnchanged, HistorizationPhase.Existing)

    val baseColumnsAdded = List((125, "Edeltraut", 54, "healthy"))
    val dfAdded = toHistorizedDf(baseColumnsAdded, HistorizationPhase.NewlyAdded)

    val dfExpected = dfAdded.union(dfUnchanged)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("When adding 1 record, the history should contain the new record")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When adding 1 record that was technically deleted in the past already, the history should contain the new version") {
    val baseColumnsOldExistingHist = List((123, "Egon", 23, "healthy"))
    val dfOldExistingHist = toHistorizedDf(baseColumnsOldExistingHist, HistorizationPhase.Existing)

    val baseColumnsOldDeletedHist = List((124, "Erna", 27, "healthy"))
    val dfOldDeletedHist = toHistorizedDf(baseColumnsOldDeletedHist, HistorizationPhase.TechnicallyDeleted)

    val dfOldHist = dfOldExistingHist.union(dfOldDeletedHist)
    logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 28, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.fullHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
    logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUnchangedExistingHist = List((123, "Egon", 23, "healthy"))
    val dfUnchangedExistingHist = toHistorizedDf(baseColumnsUnchangedExistingHist, HistorizationPhase.Existing)

    val baseColumnsUnchangedDeletedHist = List((124, "Erna", 27, "healthy"))
    val dfUnchangedDeletedHist = toHistorizedDf(baseColumnsUnchangedDeletedHist, HistorizationPhase.TechnicallyDeleted)

    val dfUnchanged = dfUnchangedExistingHist.union(dfUnchangedDeletedHist)

    val baseColumnsAdded = List((124, "Erna", 28, "healthy"))
    val dfAdded = toHistorizedDf(baseColumnsAdded, HistorizationPhase.NewlyAdded)

    val dfExpected = dfAdded.union(dfUnchanged)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("When adding 1 record that was technically deleted in the past already, the history should contain the new version")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("Exchanging non-null value and null value between columns should create a new history entry") {

    val schemaValues = List(StructField("id", IntegerType, nullable = false),
      StructField("col_A", StringType, nullable = true),
      StructField("col_B", StringType, nullable = true))

    val schemaHistory = schemaValues ++ List(
      StructField("captured", StringType, nullable = false),
      StructField("delimited", StringType, nullable = true))

    val existingData = Seq(Row.fromSeq(Seq(1, null, "value", erfasstTimestampOldHist.toString, doomsday.toString)))
    val newData = Seq(Row.fromSeq(Seq(1, "value", null)))
    val expectedResult = Seq(Row.fromSeq(Seq(1, null, "value", erfasstTimestampOldHist.toString, referenceTimestampOld.toString)),
      Row.fromSeq(Seq(1, "value", null, referenceTimestampNew.toString, doomsday.toString)))

    val parseTimestampColumns : DataFrame => DataFrame = df =>
      df.withColumn(TechnicalTableColumn.captured, to_timestamp(col("captured")))
        .withColumn(TechnicalTableColumn.delimited, to_timestamp(col("delimited")))
        .drop("captured")
        .drop("delimited")

    val dfHistory = parseTimestampColumns(session.createDataFrame(session.sparkContext.parallelize(existingData), StructType(schemaHistory)))
    val dfExpected = parseTimestampColumns(session.createDataFrame(session.sparkContext.parallelize(expectedResult), StructType(schemaHistory)))
    val dfNew = session.createDataFrame(session.sparkContext.parallelize(newData), StructType(schemaValues))

    val dfHistorized = Historization.fullHistorize(dfHistory, dfNew, Seq("id"), referenceTimestampNew, None, None)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("Exchanging non-null value and null value between columns should create a new history entry")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("comparing rows") {

    def rowsEqual(r1: Row, r2: Row): Boolean = {
      r1.hashCode() == r2.hashCode()
    }
    assert(rowsEqual(Row.fromSeq(Seq(1, null, "value")),Row.fromSeq(Seq(1, null, "value"))))
    assert(!rowsEqual(Row.fromSeq(Seq(1, null, "value")),Row.fromSeq(Seq(1, "value", null)))) // switched field content
    assert(!rowsEqual(Row.fromSeq(Seq(1, null, "value")),Row.fromSeq(Seq(1, null, "value", "test")))) // additional field
    assert(!rowsEqual(Row.fromSeq(Seq(1, null, "value")),Row.fromSeq(Seq(1, null, "value", null)))) // additional null field

    def internalRowsEqual(r1: InternalRow, r2: InternalRow): Boolean = {
      r1.hashCode() == r2.hashCode()
    }
    assert(internalRowsEqual(InternalRow(1, null, "value"),InternalRow(1, null, "value")))
    assert(!internalRowsEqual(InternalRow(1, null, "value"),InternalRow(1, "value", null))) // switched field content
    assert(!internalRowsEqual(InternalRow(1, null, "value"),InternalRow(1, null, "value", "test"))) // additional field
    assert(!internalRowsEqual(InternalRow(1, null, "value"),InternalRow(1, null, "value", null))) // additional null field
  }

}
