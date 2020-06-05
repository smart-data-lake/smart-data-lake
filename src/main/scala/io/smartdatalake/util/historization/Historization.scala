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

import java.time.LocalDateTime

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions
import io.smartdatalake.definitions.{HiveConventions, TechnicalTableColumn}
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.misc.{DataFrameUtil, SmartDataLakeLogger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.util.hashing.MurmurHash3

/**
 * Functions for historization
 */
private[smartdatalake] object Historization extends SmartDataLakeLogger {

  private val ts1: java.time.LocalDateTime => Column = t => lit(t.toString).cast(TimestampType)

  private val fooSeed = MurmurHash3.stringHash("mySpecificSeed")
  private val hashRow = (colsJson: String) => MurmurHash3.stringHash(colsJson, fooSeed)
  private val hashFunc = udf(hashRow)

  def historizedForEmptyLoad(history: DataFrame, referenceTimestamp: LocalDateTime, doPersist: Boolean,
                             filterClause: Option[String])(implicit session: SparkSession): DataFrame = {
    session.sparkContext.setJobDescription(s"${getClass.getSimpleName}.historizedForEmptyLoad")
    val expiryDateCol = TechnicalTableColumn.delimited.toString
    val offsetNs = 1000000L
    val doomsday = definitions.HiveConventions.getHistorizationSurrogateTimestamp
    val timestampNew = referenceTimestamp
    val timestampOld = timestampNew.minusNanos(offsetNs)

    val (historyFiltered, historyFilteredRemaining): (Option[DataFrame], Option[DataFrame]) =
      filterClause match {
      case Some(clause) =>
        val negatedClause = s"NOT ($clause)"
        (Some(history.where(clause)), Some(history.where(negatedClause)))
      case None => (None, None)
      }

    val historyDf = if (doPersist) {
      historyFiltered match {
        case Some(_) => DataFrameUtil.defaultPersistDf(historyFiltered.get)
        case None => DataFrameUtil.defaultPersistDf(history)
      }
    } else {
      historyFiltered match {
        case Some(_) => historyFiltered.get
        case None => history
      }
    }

    val lastHist = historyDf.where(s"$expiryDateCol = '$doomsday'")
    val lastHistDf = if (doPersist) {
      DataFrameUtil.defaultPersistDf(lastHist)
    } else {
      lastHist
    }

    val restHist = historyDf.where(s"$expiryDateCol <> '$doomsday'")

    // (technical) deletes, that is records are not present in current data load anymore
    val notInFeedAnymore = lastHistDf.withColumn(s"$expiryDateCol", ts1(timestampOld))

    val all = historyFilteredRemaining match {
      case Some(_) => notInFeedAnymore.union(restHist).union(historyFilteredRemaining.get)
      case None => notInFeedAnymore.union(restHist)
    }

    all
  }

  /**
   * Historizes data by merging the current load with the existing history
   *
   * @param historyDf exsisting history of data
   * @param newFeedDf current load of feed
   * @param primaryKeyColumns Primary keys to join history with current load
   * @param historizeBlacklist optional list of columns to ignore when comparing two records. Can not be used together with historizeWhitelist.
   * @param historizeWhitelist optional final list of columns to use when comparing two records. Can not be used together with historizeBlacklist.
   * @return current feed merged with history
  */
  def getHistorized(historyDf: DataFrame, newFeedDf: DataFrame, primaryKeyColumns: Seq[String],
                    referenceTimestamp: LocalDateTime,
                    historizeWhitelist: Option[Seq[String]],
                    historizeBlacklist: Option[Seq[String]]
                   )(implicit session: SparkSession): DataFrame = {

    // Name for Hive column "last updated on ..."
    val lastUpdateCol = TechnicalTableColumn.captured.toString

    // Name for Hive column "Replaced on ..."
    val expiryDateCol = TechnicalTableColumn.delimited.toString

    // "Tick" offset used to delimit timestamps of old and new values
    val offsetNs = 1000000L

    // High value - symbolic value of timestamp with meaning of "without expiry date"
    //val doomsday = new java.sql.Timestamp(new java.util.Date("9999/12/31").getTime)
    val doomsday = definitions.HiveConventions.getHistorizationSurrogateTimestamp

    // Current timestamp (used for insert and update operations, for "new" value)
    val timestampNew = referenceTimestamp

    // Shortly before the current timestamp ("Tick") used for existing, old records
    val timestampOld = timestampNew.minusNanos(offsetNs)

    // Records in history that still existed during the last execution
    //val lastHist = historyPersisted.where(s"$expiryDateCol = '$doomsday'")
    val lastHistDf = historyDf.where(col(expiryDateCol) === s"$doomsday")

    // Records in history that already didn't exist during last execution
    //val restHist = historyDf.except(lastHistDf)
    val restHist = historyDf.where(col(expiryDateCol) =!= s"$doomsday")


    val historizeHashCols = (historizeWhitelist, historizeBlacklist) match {
      case (Some(w), None) => w.sorted
      case (None, Some(b)) => newFeedDf.columns.diff(b).sorted.toSeq
      case (None, None) => newFeedDf.columns.sorted.toSeq
      case (Some(_), Some(_)) => throw new ConfigurationException("historizeWhitelist and historizeBlacklist mustn't be used at the same time.")
    }

    // Generic column expression to generate a JSON string of the hash fields
    def colsToJson(cols: Seq[String]): Column = hashFunc(to_json(struct(cols.map(col): _*)))

    val newFeedHashed = newFeedDf.withColumn("hash", colsToJson(historizeHashCols))

    // columns used to build hash according to newFeedDf (lastHistDf contains dl_captured/delimited)
    val colsToUseLastHistDf = lastHistDf.columns.diff(Seq(TechnicalTableColumn.captured.toString, TechnicalTableColumn.delimited.toString, "dl_dt")).sorted
    val lastHistHashed = (historizeWhitelist, historizeBlacklist) match {
      case (Some(w), None) =>
        val w_diff = w.diff(Seq(TechnicalTableColumn.captured.toString, TechnicalTableColumn.delimited.toString, "dl_dt"))
        val colsToUse = colsToUseLastHistDf.intersect(w_diff).sorted // merged columns from whitelist und lastHistDf without technical columns
        lastHistDf.withColumn("hash", colsToJson(colsToUse))
      case (None, Some(b)) =>
        val colsToUse = colsToUseLastHistDf.diff(b).sorted
        lastHistDf.withColumn("hash", colsToJson(colsToUse))
      case (None, None) =>
        val colsToUse = colsToUseLastHistDf.sorted
        lastHistDf.withColumn("hash", colsToJson(colsToUse))
      case (Some(_), Some(_)) => throw new ConfigurationException("historize-whitelist and historize-blacklist mustn't be used at the same time.")
    }

    val joined = newFeedHashed.as("newFeed").join(lastHistHashed.as("lastHist"),
      joinCols(newFeedHashed, lastHistHashed, primaryKeyColumns), "full")

    val newRows = joined.where(col(expiryDateCol).isNull)
      .select(newFeedDf("*"))
      .withColumn(lastUpdateCol, ts1(timestampNew))
      .withColumn(expiryDateCol, ts1(doomsday))

    val notInFeedAnymore = joined.where(nullTableCols("newFeed", primaryKeyColumns))
      .select(lastHistDf("*"))
      .withColumn(expiryDateCol, ts1(timestampOld))

    val noUpdates = joined
      .where(col("newFeed.hash") === col("lastHist.hash"))
      .select(lastHistDf("*"))

    val updated = joined
      .where(nonNullTableCols("newFeed", primaryKeyColumns))
      .where(col("newFeed.hash") =!= col("lastHist.hash"))

    val updatedNew = updated.select(newFeedDf("*"))
      .withColumn(lastUpdateCol, ts1(timestampNew))
      .withColumn(expiryDateCol, ts1(doomsday))

    val updatedOld = updated.select(lastHistDf("*"))
      .withColumn(expiryDateCol, ts1(timestampOld))

    // column order is used here!
    val tenantNewHist = SchemaEvolution.sortColumns(notInFeedAnymore, historyDf.columns)
      .union(SchemaEvolution.sortColumns(newRows, historyDf.columns))
      .union(SchemaEvolution.sortColumns(updatedNew, historyDf.columns))
      .union(SchemaEvolution.sortColumns(updatedOld, historyDf.columns))
      .union(SchemaEvolution.sortColumns(noUpdates, historyDf.columns))
      .union(SchemaEvolution.sortColumns(restHist, historyDf.columns))

    if (logger.isDebugEnabled) {
      logger.debug(s"Count previous history: ${historyDf.count}")
      logger.debug(s"Count current load of feed: ${newFeedDf.count}")
      logger.debug(s"Count rows not in current feed anymore: ${notInFeedAnymore.count}")
      logger.debug(s"Count new rows: ${newRows.count}")
      logger.debug(s"Count updated rows new: ${updatedNew.count}")
      logger.debug(s"Count updated rows old: ${updatedOld.count}")
      logger.debug(s"Count no updates old: ${noUpdates.count}")
      logger.debug(s"Count rows from remaining history: ${restHist.count}")
      logger.debug(s"Summary count rows new history: ${tenantNewHist.count}")
    }

    tenantNewHist
  }


  /**
   * Creates initial history of feed
   *
   * @param newFeed current run of feed
   * @param referenceTimestamp timestamp to use
   * @return initial history, identical with data from current run
   */
  def getInitialHistory(newFeed: DataFrame, referenceTimestamp: LocalDateTime)(implicit session: SparkSession): DataFrame = {
    session.sparkContext.setJobDescription("Historization.getInitialHistory")
    val expiryDate = HiveConventions.getHistorizationSurrogateTimestamp
    val lastUpdateCol = TechnicalTableColumn.captured.toString
    val expiryDateCol = TechnicalTableColumn.delimited.toString
    val newRows = newFeed
      .select("*")
      .withColumn(s"$lastUpdateCol", ts1(referenceTimestamp))
      .withColumn(s"$expiryDateCol", ts1(expiryDate))
    logger.debug(s"Initial history used for $lastUpdateCol: $referenceTimestamp")

    newRows
  }

  private def joinCols(left: DataFrame, right: DataFrame, cols: Seq[String]) = {
    cols.map(c => left(c) === right(c)).reduce(_ && _)
  }

  private def nullTableCols(table: String, cols: Seq[String]) = {
    cols.map(c => s"$table.$c is null").reduce((a, b) => s"$a and $b")
  }

  private def nonNullTableCols(table: String, cols: Seq[String]) = {
    cols.map(c => s"$table.$c is not null").reduce((a, b) => s"$a and $b")
  }
}
