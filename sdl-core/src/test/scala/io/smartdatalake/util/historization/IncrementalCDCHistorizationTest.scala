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
package io.smartdatalake.util.historization

import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.historization.HistorizationTestUtils._
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.DataFrameFunctions
import io.smartdatalake.workflow.dataframe.spark.{SparkSimpleDataType, SparkSubFeed}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.TimestampType
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

/**
 * Unit tests for historization with incrementalCDCHistorize.
 * incrementalCDCHistorize is much different from incrementalHistorize because it doesn't need an existing DataFrame
 */
class IncrementalCDCHistorizationTest extends AnyFunSuite with BeforeAndAfter with SmartDataLakeLogger
  with io.smartdatalake.testutils.spark.dataset.TestToolDataset {

  private implicit val loggerImpl: Logger = logger
  private implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(SparkSubFeed.subFeedType)
  import functions._

  test("New/updated record creates updateClose and insertNew records for merge statement") {
    val dataNewFeed = List((123, "Egon", 23, "healthy", "new"))
    val dfNewFeed = toDataDf(dataNewFeed, colNames :+ "operation")

    val dfHistorized = Historization.incrementalCDCHistorize(dfNewFeed, col("operation") === lit("deleted"), referenceTimestampNewTs, defaultTimeAxisUnit)
      .drop("operation")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val dataExpected = Seq(
      (123, "Egon", 23, "healthy", true, HistorizationRecordOperations.updateClose, null, getReferenceTimestampOldTs()),
      (123, "Egon", 23, "healthy", false, HistorizationRecordOperations.insertNew, referenceTimestampNewTs, doomsdayTs),
    )
    val dfExpected = toDataDf(dataExpected, colNames ++ Seq("dl_dummy", Historization.historizeOperationColName, Environment.capturedColumnName, Environment.delimitedColumnName))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResultGeneric("New/updated record creates updateClose and insertNew records for merge statement")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("Deleted record creates updateClose record for merge statement") {
    val dataNewFeed = List((123, "Egon", 23, "healthy", "deleted"))
    val dfNewFeed = toDataDf(dataNewFeed, colNames :+ "operation")

    val dfHistorized = Historization.incrementalCDCHistorize(dfNewFeed, col("operation") === lit("deleted"), referenceTimestampNewTs, defaultTimeAxisUnit)
      .drop("operation")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val dataExpected = Seq(
      (123, "Egon", 23, "healthy", true, HistorizationRecordOperations.updateClose, null, getReferenceTimestampOldTs()),
    )
    val dfExpected = toDataDf(dataExpected, colNames ++ Seq("dl_dummy", Historization.historizeOperationColName, Environment.capturedColumnName, Environment.delimitedColumnName))
      .withColumn(Environment.capturedColumnName, col(Environment.capturedColumnName).cast(SparkSimpleDataType(TimestampType)))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) printFailedTestResultGeneric("Deleted record creates updateClose record for merge statement")(dfHistorized)(dfExpected)
    assert(result)
  }
}
