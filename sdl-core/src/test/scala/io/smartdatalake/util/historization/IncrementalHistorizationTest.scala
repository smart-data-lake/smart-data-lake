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
import io.smartdatalake.util.historization.HistorizationTestUtils._
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Unit tests for historization
 *
 */
class IncrementalHistorizationTest extends FunSuite with BeforeAndAfter with SmartDataLakeLogger {

  private implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  // here fullHistorize and incrementalHistorize differ:
  // - incrementalHistorize closes existing record and creates new record if schema changes (but only for current records).
  // - fullHistorize leaves data unchanged
  test("History changed with new columns but unchanged data") {
    val dataOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(dataOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val dataNewFeed = List((123, "Egon", 23, "healthy", "Test"), (124, "Erna", 27, "healthy", null))
    val dfNewFeed = toDataDf(dataNewFeed, colNames :+ "new_col1")
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
      .drop($"dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val dataExpected = Seq(
      (123, "Egon", 23, "healthy", "Test", HistorizationRecordOperations.updateClose, erfasstTimestampOldHistTs, referenceTimestampOldTs),
      (123, "Egon", 23, "healthy", "Test", HistorizationRecordOperations.insertNew, referenceTimestampNewTs, doomsdayTs),
      (124, "Erna", 27, "healthy", null, HistorizationRecordOperations.updateClose, erfasstTimestampOldHistTs, referenceTimestampOldTs),
      (124, "Erna", 27, "healthy", null, HistorizationRecordOperations.insertNew, referenceTimestampNewTs, doomsdayTs)
    )
    val dfExpected = toDataDf(dataExpected, colNames ++ Seq("new_col1", Historization.historizeOperationColName, TechnicalTableColumn.captured, TechnicalTableColumn.delimited))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("History unchanged with new columns but unchanged data")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("The history should stay unchanged when using the current load again") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    // nothing to do if unchanged
    assert(dfHistorized.isEmpty)
  }

  test("History should stay unchanged when using current load but with different column sorting") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed).select($"age", $"health_state", $"id", $"name")
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    // nothing to do if unchanged
    assert(dfHistorized.isEmpty)
  }


  test("When updating 1 record, the history should contain the old and the new version of the values") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "sick"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
      .drop($"dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUpdatedOld = List((123, "Egon", 23, "sick")) // note that incremental historization uses attribute values of the new records (health_state=sick) for UpdatedOld, but it will only update dl_delimited in the target table (and not use the value of health_state).
    val dfUpdatedOld = toHistorizedDf(baseColumnsUpdatedOld, HistorizationPhase.UpdatedOld, withOperation = true)
    val baseColumnsUpdatedNew = List((123, "Egon", 23, "sick"))
    val dfUpdatedNew = toHistorizedDf(baseColumnsUpdatedNew, HistorizationPhase.UpdatedNew, withOperation = true)
    val dfExpected = dfUpdatedNew.union(dfUpdatedOld)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("When updating 1 record, the history should contain the old and the new version of the values")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When deleting 1 record (technical deletion) the dl_ts_delimited column should be updated") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
      .drop($"dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUpdatedOld: List[(Int, String, java.lang.Integer, String)] = List((123, "Egon", null, null)) // note that incremental historization has no attribute values for UpdatedOld, but it will only update dl_delimited in the target table.
    val dfExpected = toHistorizedDf(baseColumnsUpdatedOld, HistorizationPhase.UpdatedOld, withOperation = true)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("When deleting 1 record (technical deletion) the dl_ts_delimited column should be updated")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When adding 1 record, the history should contain the new record") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"), (125, "Edeltraut", 54, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
      .drop($"dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsAdded = List((125, "Edeltraut", 54, "healthy"))
    val dfExpected = toHistorizedDf(baseColumnsAdded, HistorizationPhase.NewlyAdded, withOperation = true)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("When adding 1 record, the history should contain the new record")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When adding 1 record that was technically deleted in the past already, the history should contain the new version") {
    val baseColumnsOldExistingHist = List((123, "Egon", 23, "healthy"))
    val dfOldExistingHist = toHistorizedDf(baseColumnsOldExistingHist, HistorizationPhase.Existing, withHashCol = true)

    val baseColumnsOldDeletedHist = List((124, "Erna", 27, "healthy"))
    val dfOldDeletedHist = toHistorizedDf(baseColumnsOldDeletedHist, HistorizationPhase.TechnicallyDeleted, withHashCol = true)

    val dfOldHist = dfOldExistingHist.union(dfOldDeletedHist)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 28, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNew, None, None)
      .drop($"dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsAdded = List((124, "Erna", 28, "healthy"))
    val dfExpected = toHistorizedDf(baseColumnsAdded, HistorizationPhase.NewlyAdded, withOperation = true)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("When adding 1 record that was technically deleted in the past already, the history should contain the new version")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("Exchanging non-null value and null value between columns should create a new history entry") {

    val baseColumnsOldExistingHist: List[(Int, String, java.lang.Integer, String)] = List((123, "Egon", null, "healthy"))
    val dfHistory = toHistorizedDf(baseColumnsOldExistingHist, HistorizationPhase.Existing, withHashCol = true)

    val baseColumnsNewFeed: List[(Int, String, java.lang.Integer, String)] = List((123, "Egon", 23, null))
    val dfNew = toDataDf(baseColumnsNewFeed)

    val dfHistorized = Historization.incrementalHistorize(dfHistory, dfNew, Seq("id"), referenceTimestampNew, None, None)
      .drop($"dl_hash")

    val dfExpected = toHistorizedDf(baseColumnsNewFeed, HistorizationPhase.UpdatedOld, withOperation = true) // note that incremental historization uses attribute values of the new records for UpdatedOld, but it will only update dl_delimited in the target table (and not use the value of health_state).
      .union(toHistorizedDf(baseColumnsNewFeed, HistorizationPhase.NewlyAdded, withOperation = true))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("Exchanging non-null value and null value between columns should create a new history entry")(dfHistorized)(dfExpected)
    assert(result)
  }

}
