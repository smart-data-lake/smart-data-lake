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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions.{CdcChangeType, Environment}
import io.smartdatalake.util.LogUtils.debugLog
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame}
import org.slf4j.Logger

import java.sql.Timestamp
import java.time.Duration

/**
 * Functions for historization
 */
object Historization {

  private[smartdatalake] val historizeHashColName = "dl_hash" // incrementalHistorize adds hash col to target schema for comparing changes
  private[smartdatalake] val historizeOperationColName =
    "dl_operation" // incrementalHistorize needs operation col for merge statement. It is temporary and is not added to target schema.
  private[smartdatalake] val historizeDummyColName =
    "dl_dummy" // incrementalCDCHistorize needs a dummy col for avoiding deduplication in merge statements join condition.
  // temporary columns holding the validity timestamps of a record if they are derived from a source timestamp column.
  // They are removed by the final select of incrementalHistorize.
  private[smartdatalake] val timestampNewColName = "_dl_ts_new"
  private[smartdatalake] val timestampOldColName = "_dl_ts_old"

  private[smartdatalake] def getCompareColumns(
      colsToUse: Seq[String],
      historizeWhitelist: Option[Seq[String]],
      historizeBlacklist: Option[Seq[String]],
      caseSensitive: Boolean = false
  ): Seq[String] = {
    val colsToCompare = (historizeWhitelist, historizeBlacklist) match {
      case (Some(w), None) => if (caseSensitive) colsToUse.intersect(w)
        else colsToUse.map(_.toLowerCase).intersect(
          w.map(_.toLowerCase)
        ) // merged columns from whitelist und dfLastHist without technical columns
      case (None, Some(b))    => if (caseSensitive) colsToUse.diff(b) else colsToUse.map(_.toLowerCase).diff(b.map(_.toLowerCase))
      case (None, None)       => if (caseSensitive) colsToUse else colsToUse.map(_.toLowerCase)
      case (Some(_), Some(_)) =>
        throw new ConfigurationException("historize-whitelist and historize-blacklist must not be used at the same time.")
    }
    colsToCompare.sorted
  }

  private[smartdatalake] def addHashCol(
      df: GenericDataFrame,
      historizeWhitelist: Option[Seq[String]],
      historizeBlacklist: Option[Seq[String]],
      useHash: Boolean,
      colsToIgnore: Seq[String] = Seq()
  ): GenericDataFrame = {
    assert(!df.columns.contains(historizeHashColName),
      s"DataFrame must not contain column with name $historizeHashColName if addHashCol is called")
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    val colsToCompare = getCompareColumns(df.columns.diff(colsToIgnore), historizeWhitelist, historizeBlacklist)
    df.withColumn(historizeHashColName, functions.colscomparisonExpr(colsToCompare.map(functions.col), useHash))
  }

  private[smartdatalake] def getPreviousTimeAxisEntry(ts: Timestamp, unit: Duration) = Timestamp.from(ts.toInstant.minus(unit))
}

object HistorizationRecordOperations {
  val updateExisting = "updateExisting"
  val updateClose = "updateClose"
  val insertNew = "insertNew"
}
