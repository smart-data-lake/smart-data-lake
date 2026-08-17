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
import io.smartdatalake.definitions.{CdcChangeType, Environment}
import io.smartdatalake.testutils.spark.{SparkTestTool, SparkTestUtil}
import io.smartdatalake.util.historization.HistorizationTestUtils._
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataframe.spark.{SparkSimpleDataType, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

import java.sql.Timestamp

/**
 * Unit tests for historization with incrementalCDCHistorize. incrementalCDCHistorize is much
 * different from incrementalHistorize because it doesn't need an existing DataFrame
 */
class IncrementalCDCHistorizationTest extends AnyFunSuite with BeforeAndAfter with SmartDataLakeLogger
    with SparkTestTool {

  private implicit val loggerImpl: Logger = logger
  private implicit val session: SparkSession = SparkTestUtil.session

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  implicit val functions: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(SparkSubFeed.subFeedType)
  implicit val actionPipelineContext: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext
  import functions._

  test("New/updated record creates updateClose and insertNew records for merge statement") {
    val dataNewFeed = List((123, "Egon", 23, "healthy", "new"))
    val dfNewFeed = toDataDf(dataNewFeed, colNames :+ "operation")

    val dfHistorized =
      Historization.incrementalCDCHistorize(dfNewFeed, col("operation") === lit("deleted"), referenceTimestampNewTs, defaultTimeAxisUnit)
        .drop("operation")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val dataExpected = Seq(
      (123, "Egon", 23, "healthy", true,  HistorizationRecordOperations.updateClose, null,                    getReferenceTimestampOldTs()),
      (123, "Egon", 23, "healthy", false, HistorizationRecordOperations.insertNew,   referenceTimestampNewTs, doomsdayTs)
    )
    val dfExpected = toDataDf(dataExpected,
      colNames ++ Seq("dl_dummy", Historization.historizeOperationColName, Environment.capturedColumnName, Environment.delimitedColumnName))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result)
      printFailedTestResult("New/updated record creates updateClose and insertNew records for merge statement")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("Deleted record creates updateClose record for merge statement") {
    val dataNewFeed = List((123, "Egon", 23, "healthy", "deleted"))
    val dfNewFeed = toDataDf(dataNewFeed, colNames :+ "operation")

    val dfHistorized =
      Historization.incrementalCDCHistorize(dfNewFeed, col("operation") === lit("deleted"), referenceTimestampNewTs, defaultTimeAxisUnit)
        .drop("operation")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val dataExpected = Seq(
      (123, "Egon", 23, "healthy", true, HistorizationRecordOperations.updateClose, null, getReferenceTimestampOldTs())
    )
    val dfExpected = toDataDf(dataExpected,
      colNames ++ Seq("dl_dummy", Historization.historizeOperationColName, Environment.capturedColumnName, Environment.delimitedColumnName))
      .withColumn(Environment.capturedColumnName, col(Environment.capturedColumnName).cast(SparkSimpleDataType(TimestampType)))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("Deleted record creates updateClose record for merge statement")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("prepareCdcInput removes preimages and keeps the last event per primary key") {
    val dataNewFeed = List(
      (123, "Egon", 23, "healthy", CdcChangeType.insert, 0),
      (123, "Egon", 24, "healthy", CdcChangeType.updatePreimage, 1),
      (123, "Egon", 24, "sick", CdcChangeType.updatePostimage, 2),
      (124, "Erika", 30, "healthy", CdcChangeType.updatePreimage, 3), // an event of another primary key is not mixed up
      (124, "Erika", 31, "healthy", CdcChangeType.updatePostimage, 4)
    )
    val cdcColNames = colNames ++ Seq(Environment.cdcChangeTypeColumnName, Environment.cdcChangeOrdinalColumnName)
    val dfNewFeed = toDataDf(dataNewFeed, cdcColNames)

    val dfPrepared = Historization.prepareCdcInput(dfNewFeed, Seq("id"), Environment.cdcChangeTypeColumnName,
      Some(Environment.cdcChangeOrdinalColumnName))

    val dfExpected = toDataDf(Seq(
      (123, "Egon", 24, "sick", CdcChangeType.updatePostimage, 2),
      (124, "Erika", 31, "healthy", CdcChangeType.updatePostimage, 4)
    ), cdcColNames)

    val result = dfExpected.isEqual(dfPrepared)
    if (!result) printFailedTestResult("prepareCdcInput removes preimages and keeps the last event per primary key")(dfPrepared)(dfExpected)
    assert(result)
  }

  test("prepareCdcInput keeps a delete as last event of a primary key") {
    val cdcColNames = colNames ++ Seq(Environment.cdcChangeTypeColumnName, Environment.cdcChangeOrdinalColumnName)
    val dfNewFeed = toDataDf(List(
      (123, "Egon", 23, "healthy", CdcChangeType.updatePostimage, 0),
      (123, "Egon", 23, "healthy", CdcChangeType.delete, 1)
    ), cdcColNames)

    val dfPrepared = Historization.prepareCdcInput(dfNewFeed, Seq("id"), Environment.cdcChangeTypeColumnName,
      Some(Environment.cdcChangeOrdinalColumnName))

    val dfExpected = toDataDf(Seq((123, "Egon", 23, "healthy", CdcChangeType.delete, 1)), cdcColNames)

    val result = dfExpected.isEqual(dfPrepared)
    if (!result) printFailedTestResult("prepareCdcInput keeps a delete as last event of a primary key")(dfPrepared)(dfExpected)
    assert(result)
  }

  test("prepareCdcInput without order column only removes preimages") {
    val cdcColNames = colNames :+ Environment.cdcChangeTypeColumnName
    val dfNewFeed = toDataDf(List(
      (123, "Egon", 23, "healthy", CdcChangeType.updatePreimage),
      (123, "Egon", 24, "healthy", CdcChangeType.updatePostimage),
      (124, "Erika", 30, "healthy", CdcChangeType.insert)
    ), cdcColNames)

    val dfPrepared = Historization.prepareCdcInput(dfNewFeed, Seq("id"), Environment.cdcChangeTypeColumnName, None)

    val dfExpected = toDataDf(Seq(
      (123, "Egon", 24, "healthy", CdcChangeType.updatePostimage),
      (124, "Erika", 30, "healthy", CdcChangeType.insert)
    ), cdcColNames)

    val result = dfExpected.isEqual(dfPrepared)
    if (!result) printFailedTestResult("prepareCdcInput without order column only removes preimages")(dfPrepared)(dfExpected)
    assert(result)
  }

  test("Validity of new versions starts at the commit timestamp of the source system") {
    val commitTs = Timestamp.valueOf(referenceTimestampNew.minusDays(2))
    val cdcColNames = colNames ++ Seq(Environment.cdcChangeTypeColumnName, Environment.cdcCommitTimestampColumnName)
    val dfNewFeed = toDataDf(List((123, "Egon", 23, "healthy", CdcChangeType.updatePostimage, commitTs)), cdcColNames)

    val dfHistorized = Historization.incrementalCDCHistorize(
      dfNewFeed,
      col(Environment.cdcChangeTypeColumnName) === lit(CdcChangeType.delete),
      referenceTimestampNewTs,
      defaultTimeAxisUnit,
      Some(Environment.cdcCommitTimestampColumnName)
    ).drop(Environment.cdcChangeTypeColumnName).drop(Environment.cdcCommitTimestampColumnName)

    // the versions are delimited relative to the commit timestamp, not to the reference timestamp of the run
    val commitTsPreviousTick = Timestamp.from(commitTs.toInstant.minusMillis(1))
    val dfExpected = toDataDf(Seq(
      (123, "Egon", 23, "healthy", true, HistorizationRecordOperations.updateClose, null, commitTsPreviousTick),
      (123, "Egon", 23, "healthy", false, HistorizationRecordOperations.insertNew, commitTs, doomsdayTs)
    ), colNames ++ Seq("dl_dummy", Historization.historizeOperationColName, Environment.capturedColumnName,
      Environment.delimitedColumnName))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResult("Validity of new versions starts at the commit timestamp of the source system")(dfHistorized)(dfExpected)
    assert(result)
  }
}
