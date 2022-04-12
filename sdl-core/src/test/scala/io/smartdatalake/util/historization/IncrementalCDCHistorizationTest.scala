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
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Unit tests for historization with incrementalCDCHistorize.
 * incrementalCDCHistorize is much different from incrementalHistorize because it doesn't need an existing DataFrame
 */
class IncrementalCDCHistorizationTest extends FunSuite with BeforeAndAfter with SmartDataLakeLogger {

  private implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  test("New/updated record creates updateClose and insertNew records for merge statement") {
    val dataNewFeed = List((123, "Egon", 23, "healthy", "new"))
    val dfNewFeed = toDataDf(dataNewFeed, colNames :+ "operation")

    val dfHistorized = Historization.incrementalCDCHistorize(dfNewFeed, $"operation" === "deleted", referenceTimestampNew)
      .drop("operation")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val dataExpected = Seq(
      (123, "Egon", 23, "healthy", true, HistorizationRecordOperations.updateClose, null, referenceTimestampOldTs),
      (123, "Egon", 23, "healthy", false, HistorizationRecordOperations.insertNew, referenceTimestampNewTs, doomsdayTs),
    )
    val dfExpected = toDataDf(dataExpected, colNames ++ Seq("dl_dummy", Historization.historizeOperationColName, TechnicalTableColumn.captured, TechnicalTableColumn.delimited))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("New/updated record creates updateClose and insertNew records for merge statement")(dfHistorized)(dfExpected)
    assert(result)
  }

  test("Deleted record creates updateClose record for merge statement") {
    val dataNewFeed = List((123, "Egon", 23, "healthy", "deleted"))
    val dfNewFeed = toDataDf(dataNewFeed, colNames :+ "operation")

    val dfHistorized = Historization.incrementalCDCHistorize(dfNewFeed, $"operation" === "deleted", referenceTimestampNew)
      .drop("operation")
    if (logger.isDebugEnabled) logger.debug(s"Historization result:\n${dfHistorized.showString()}")

    val dataExpected = Seq(
      (123, "Egon", 23, "healthy", true, HistorizationRecordOperations.updateClose, null, referenceTimestampOldTs),
    )
    val dfExpected = toDataDf(dataExpected, colNames ++ Seq("dl_dummy", Historization.historizeOperationColName, TechnicalTableColumn.captured, TechnicalTableColumn.delimited))
      .withColumn(TechnicalTableColumn.captured, col(TechnicalTableColumn.captured).cast("timestamp"))

    val result = dfExpected.isEqual(dfHistorized)
    if (!result) TestUtil.printFailedTestResult("Deleted record creates updateClose record for merge statement")(dfHistorized)(dfExpected)
    assert(result)
  }
}
