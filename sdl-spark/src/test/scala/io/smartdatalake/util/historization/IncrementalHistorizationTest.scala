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
package io.smartdatalake.util.historization

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.spark.{SparkTestTool, SparkTestUtil}
import io.smartdatalake.util.historization.HistorizationTestUtils._
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

import java.sql.Timestamp
import java.time.Duration

/**
 * Unit tests for historization
 */
class IncrementalHistorizationTest extends AnyFunSuite with BeforeAndAfter with SmartDataLakeLogger
  with SparkTestTool {

  private implicit val loggerImpl: Logger = logger
  private implicit val session: SparkSession = SparkTestUtil.session

  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  implicit val functions: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(SparkSubFeed.subFeedType)
  implicit val actionPipelineContext: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext
  import functions._

  test("History changed with new columns but unchanged data") {
    val dataOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(dataOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val dataNewFeed = List((123, "Egon", 23, "healthy", "Test"), (124, "Erna", 27, "healthy", null))
    val dfNewFeed = toDataDf(dataNewFeed, colNames :+ "new_col1")
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNewTs, defaultTimeAxisUnit, None, None, addExistingDfHashColumn = false)
      .drop("dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    // change for Egon, but no change for Erna, because the new column is null.
    val dataExpected = Seq(
      (123, "Egon", 23, "healthy", "Test", HistorizationRecordOperations.updateClose, erfasstTimestampOldHistTs, getReferenceTimestampOldTs()),
      (123, "Egon", 23, "healthy", "Test", HistorizationRecordOperations.insertNew, referenceTimestampNewTs, doomsdayTs),
    )
    val dfExpected = toDataDf(dataExpected, colNames ++ Seq("new_col1", Historization.historizeOperationColName, Environment.capturedColumnName, Environment.delimitedColumnName))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("History unchanged with new columns but unchanged data")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("The history should stay unchanged when using the current load again") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNewTs, defaultTimeAxisUnit, None, None, addExistingDfHashColumn = false)
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    // nothing to do if unchanged
    assert(dfHistorized.isEmpty)
  }

  test("History should stay unchanged when using current load but with different column sorting") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed).select(Seq(col("age"), col("health_state"), col("id"), col("name")))
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNewTs, defaultTimeAxisUnit, None, None, addExistingDfHashColumn = false)
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

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNewTs, defaultTimeAxisUnit, None, None, addExistingDfHashColumn = false)
      .drop("dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUpdatedOld = List((123, "Egon", 23, "sick")) // note that incremental historization uses attribute values of the new records (health_state=sick) for UpdatedOld, but it will only update dl_delimited in the target table (and not use the value of health_state).
    val dfUpdatedOld = toHistorizedDf(baseColumnsUpdatedOld, HistorizationPhase.UpdatedOld, withOperation = true)
    val baseColumnsUpdatedNew = List((123, "Egon", 23, "sick"))
    val dfUpdatedNew = toHistorizedDf(baseColumnsUpdatedNew, HistorizationPhase.UpdatedNew, withOperation = true)
    val dfExpected = dfUpdatedNew.unionByName(dfUpdatedOld)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("When updating 1 record, the history should contain the old and the new version of the values")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When deleting 1 record (technical deletion) the dl_ts_delimited column should be updated") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNewTs, defaultTimeAxisUnit, None, None, addExistingDfHashColumn = false)
      .drop("dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUpdatedOld: List[(Int, String, java.lang.Integer, String)] = List((123, "Egon", null, null)) // note that incremental historization has no attribute values for UpdatedOld, but it will only update dl_delimited in the target table.
    val dfExpected = toHistorizedDf(baseColumnsUpdatedOld, HistorizationPhase.UpdatedOld, withOperation = true)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("When deleting 1 record (technical deletion) the dl_ts_delimited column should be updated")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When adding 1 record, the history should contain the new record") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"), (125, "Edeltraut", 54, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNewTs, defaultTimeAxisUnit, None, None, addExistingDfHashColumn = false)
      .drop("dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsAdded = List((125, "Edeltraut", 54, "healthy"))
    val dfExpected = toHistorizedDf(baseColumnsAdded, HistorizationPhase.NewlyAdded, withOperation = true)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("When adding 1 record, the history should contain the new record")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When adding 1 record that was technically deleted in the past already, the history should contain the new version") {
    val baseColumnsOldExistingHist = List((123, "Egon", 23, "healthy"))
    val dfOldExistingHist = toHistorizedDf(baseColumnsOldExistingHist, HistorizationPhase.Existing, withHashCol = true)

    val baseColumnsOldDeletedHist = List((124, "Erna", 27, "healthy"))
    val dfOldDeletedHist = toHistorizedDf(baseColumnsOldDeletedHist, HistorizationPhase.TechnicallyDeleted, withHashCol = true)

    val dfOldHist = dfOldExistingHist.unionByName(dfOldDeletedHist)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 28, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNewTs, defaultTimeAxisUnit, None, None, addExistingDfHashColumn = false)
      .drop("dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsAdded = List((124, "Erna", 28, "healthy"))
    val dfExpected = toHistorizedDf(baseColumnsAdded, HistorizationPhase.NewlyAdded, withOperation = true)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("When adding 1 record that was technically deleted in the past already, the history should contain the new version")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("Exchanging non-null value and null value between columns should create a new history entry") {

    val baseColumnsOldExistingHist: List[(Int, String, java.lang.Integer, String)] = List((123, "Egon", null, "healthy"))
    val dfHistory = toHistorizedDf(baseColumnsOldExistingHist, HistorizationPhase.Existing, withHashCol = true)

    val baseColumnsNewFeed: List[(Int, String, java.lang.Integer, String)] = List((123, "Egon", 23, null))
    val dfNew = toDataDf(baseColumnsNewFeed)

    val dfHistorized = Historization.incrementalHistorize(dfHistory, dfNew, Seq("id"), referenceTimestampNewTs, defaultTimeAxisUnit, None, None, addExistingDfHashColumn = false)
      .drop("dl_hash")

    val dfExpected = toHistorizedDf(baseColumnsNewFeed, HistorizationPhase.UpdatedOld, withOperation = true) // note that incremental historization uses attribute values of the new records for UpdatedOld, but it will only update dl_delimited in the target table (and not use the value of health_state).
      .unionByName(toHistorizedDf(baseColumnsNewFeed, HistorizationPhase.NewlyAdded, withOperation = true))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("Exchanging non-null value and null value between columns should create a new history entry")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When timeAxisUnit=0, history with half-open intervals should be created") {
    val timeAxisUnitNone: Option[Duration] = None

    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(baseColumnsOldHist, HistorizationPhase.Existing, withHashCol = true)
    if (logger.isDebugEnabled) logger.debug(s"History at beginning:\n${dfOldHist.showString()}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "sick"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toDataDf(baseColumnsNewFeed)
    if (logger.isDebugEnabled) logger.debug(s"New feed:\n${dfNewFeed.showString()}")

    val dfHistorized = Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNewTs, timeAxisUnitNone, None, None, addExistingDfHashColumn = false)
      .drop("dl_hash")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val baseColumnsUpdatedOld = List((123, "Egon", 23, "sick")) // note that incremental historization uses attribute values of the new records (health_state=sick) for UpdatedOld, but it will only update dl_delimited in the target table (and not use the value of health_state).
    val dfUpdatedOld = toHistorizedDf(baseColumnsUpdatedOld, HistorizationPhase.UpdatedOld, withOperation = true, timeUnitAxis = timeAxisUnitNone)
    val baseColumnsUpdatedNew = List((123, "Egon", 23, "sick"))
    val dfUpdatedNew = toHistorizedDf(baseColumnsUpdatedNew, HistorizationPhase.UpdatedNew, withOperation = true, timeUnitAxis = timeAxisUnitNone)
    val dfExpected = dfUpdatedNew.unionByName(dfUpdatedOld)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("When timeAxisUnit=0, history with half-open intervals should be created")(dfHistorized)(dfExpected)
    assert(result)

    assert(dfHistorized.as("a").join(dfHistorized.as("b"), col("a." + Environment.delimitedColumnName) === col("b." + Environment.capturedColumnName), "inner").count == 1)
  }

  test("When using a source timestamp column, the validity of the new version starts at the source timestamp") {
    val sourceTs = Timestamp.valueOf(referenceTimestampNew.minusDays(1)) // after the existing version was captured
    val dfOldHist = toHistorizedDf(List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy")),
      HistorizationPhase.Existing, withHashCol = true)
    val dfNewFeed = toDataDf(List((123, "Egon", 23, "sick", sourceTs), (124, "Erna", 27, "healthy", sourceTs)),
      colNames :+ sourceTsColName)

    val dfHistorized = historize(dfOldHist, dfNewFeed).drop(Historization.historizeHashColName)

    // the versions are delimited relative to the source timestamp, not to the reference timestamp of the run.
    // Note that record 124 did not change: the source timestamp is not used for change detection.
    val sourceTsPreviousTick = Timestamp.from(sourceTs.toInstant.minusMillis(1))
    val dfExpected = toDataDf(Seq(
      (123, "Egon", 23, "sick", sourceTs, HistorizationRecordOperations.updateClose, erfasstTimestampOldHistTs, sourceTsPreviousTick),
      (123, "Egon", 23, "sick", sourceTs, HistorizationRecordOperations.insertNew, sourceTs, doomsdayTs)
    ), historizedColNames)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("Validity of new versions starts at the source timestamp")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When using a source timestamp column, deleted records are delimited with the reference timestamp") {
    val sourceTs = Timestamp.valueOf(referenceTimestampNew.minusDays(1))
    val dfOldHist = toHistorizedDf(List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy")),
      HistorizationPhase.Existing, withHashCol = true)
    val dfNewFeed = toDataDf(List((124, "Erna", 27, "healthy", sourceTs)), colNames :+ sourceTsColName)

    val dfHistorized = historize(dfOldHist, dfNewFeed).drop(Historization.historizeHashColName)

    // a record deleted in the source system has no source timestamp, so the reference timestamp of the run is used
    val dfExpected = toDataDf(Seq[(Int, String, java.lang.Integer, String, Timestamp, String, Timestamp, Timestamp)](
      (123, "Egon", null, null, null, HistorizationRecordOperations.updateClose, erfasstTimestampOldHistTs,
        getReferenceTimestampOldTs())
    ), historizedColNames)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("Deleted records are delimited with the reference timestamp")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("When a record arrives late, its new version starts at the next tick after the version it replaces") {
    val sourceTs = Timestamp.valueOf(erfasstTimestampOldHist.minusDays(1)) // before the existing version was captured
    val dfOldHist = toHistorizedDf(List((123, "Egon", 23, "healthy")), HistorizationPhase.Existing, withHashCol = true)
    val dfNewFeed = toDataDf(List((123, "Egon", 23, "sick", sourceTs)), colNames :+ sourceTsColName)

    val dfHistorized = historize(dfOldHist, dfNewFeed).drop(Historization.historizeHashColName)

    // the new version is delayed, otherwise the existing version would be delimited before it was captured
    val existingCapturedNextTick = Timestamp.from(erfasstTimestampOldHistTs.toInstant.plusMillis(1))
    val dfExpected = toDataDf(Seq(
      (123, "Egon", 23, "sick", sourceTs, HistorizationRecordOperations.updateClose, erfasstTimestampOldHistTs, erfasstTimestampOldHistTs),
      (123, "Egon", 23, "sick", sourceTs, HistorizationRecordOperations.insertNew, existingCapturedNextTick, doomsdayTs)
    ), historizedColNames)

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("Late arriving records start at the next tick")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("The initial history starts at the source timestamp") {
    val sourceTs1 = Timestamp.valueOf(referenceTimestampNew.minusDays(1))
    val sourceTs2 = Timestamp.valueOf(referenceTimestampNew.minusDays(5))
    val dfNewFeed = toDataDf(List((123, "Egon", 23, "healthy", sourceTs1), (124, "Erna", 27, "healthy", sourceTs2)),
      colNames :+ sourceTsColName)

    val dfHistorized = Historization.getInitialHistoryWithHashCol(dfNewFeed, referenceTimestampNewTs, None, None,
      Some(sourceTsColName)).drop(Historization.historizeHashColName)

    val dfExpected = toDataDf(Seq(
      (123, "Egon", 23, "healthy", sourceTs1, sourceTs1, doomsdayTs, HistorizationRecordOperations.insertNew),
      (124, "Erna", 27, "healthy", sourceTs2, sourceTs2, doomsdayTs, HistorizationRecordOperations.insertNew)
    ), colNames ++ Seq(sourceTsColName, Environment.capturedColumnName, Environment.delimitedColumnName,
      Historization.historizeOperationColName))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("The initial history starts at the source timestamp")(dfHistorized)(dfExpected)
    assert(result)
  }

  private val sourceTsColName = "last_updated"

  private val historizedColNames = colNames ++ Seq(sourceTsColName, Historization.historizeOperationColName,
    Environment.capturedColumnName, Environment.delimitedColumnName)

  private def historize(dfOldHist: GenericDataFrame, dfNewFeed: GenericDataFrame,
                        timeAxisUnit: Option[Duration] = defaultTimeAxisUnit) =
    Historization.incrementalHistorize(dfOldHist, dfNewFeed, primaryKeyColumns, referenceTimestampNewTs, timeAxisUnit,
      None, None, addExistingDfHashColumn = false, sourceTimestampColName = Some(sourceTsColName))
}
