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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame}

import java.sql.Timestamp
import java.time.Duration

/**
 * Functions for historization
 */
object Historization extends SmartDataLakeLogger {

  private[smartdatalake] val historizeHashColName = "dl_hash" // incrementalHistorize adds hash col to target schema for comparing changes
  private[smartdatalake] val historizeOperationColName = "dl_operation" // incrementalHistorize needs operation col for merge statement. It is temporary and is not added to target schema.
  private[smartdatalake] val historizeDummyColName = "dl_dummy" // incrementalCDCHistorize needs a dummy col for avoiding deduplication in merge statements join condition.

  /**
   * Historizes data by merging the current load with the existing history
   *
   * Expects dfHistory and dfNew having the same schema. Use [[SchemaEvolution.process]] for preparation.
   *
   * @param dfHistory exsisting history of data
   * @param dfNew current load of feed
   * @param primaryKeyColumns Primary keys to join history with current load
   * @param referenceTimestamp The valid from timestamp for new records
   * @param timeAxisUnit Time between ticks on the timestamp. Used to create valid to timestamp for existing/old records.
   * Set to empta to create a history with half-open intervals (e.g. valid to timestamp is exclusive)
   * @param historizeBlacklist optional list of columns to ignore when comparing two records. Can not be used together with historizeWhitelist.
   * @param historizeWhitelist optional final list of columns to use when comparing two records. Can not be used together with historizeBlacklist.
   * @return current feed merged with history
  */
  def fullHistorize(dfHistory: GenericDataFrame, dfNew: GenericDataFrame, primaryKeyColumns: Seq[String],
                    referenceTimestamp: Timestamp, timeAxisUnit: Option[Duration],
                    historizeWhitelist: Option[Seq[String]],
                    historizeBlacklist: Option[Seq[String]]
                   ): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(dfHistory.subFeedType)
    import functions._

    val doomsday = lit(Environment.historizationUpperHorizonTimestamp)

    // Name for Hive column "last updated on ..."
    val lastUpdateCol = Environment.capturedColumnName

    // Name for Hive column "Replaced on ..."
    val expiryDateCol = Environment.delimitedColumnName

    // Current timestamp (used for insert and update operations, for "new" value)
    val timestampNew = lit(referenceTimestamp)

    // Previous entry on time axis before the reference timestamp ("Tick"). This is used to delimit existing old records.
    val timestampOld = lit(timeAxisUnit.map(getPreviousTimeAxisEntry(referenceTimestamp, _)).getOrElse(referenceTimestamp))

    // make sure history schema is equal to new feed schema
    val colsToIgnore = Seq(lastUpdateCol, expiryDateCol, "dl_dt")
    val schemaHistoryRelevant = dfHistory.schema.filter(n => !colsToIgnore.contains(n.name.toLowerCase))
    assert(SchemaEvolution.hasSameColNamesAndTypes(schemaHistoryRelevant, dfNew.schema, Environment.caseSensitive),
      s"historical and new schema are not equal.\nHistory: ${schemaHistoryRelevant.treeString()}\nNew: ${dfNew.schema.treeString()}"
    )

    // Records in history that still existed during the last execution
    val dfLastHist = dfHistory.where(col(expiryDateCol) === doomsday)

    // Records in history that already didn't exist during last execution
    val restHist = dfHistory.where(col(expiryDateCol) =!= doomsday)

    // add hash-column to easily compare changed records
    val colsToCompare = getCompareColumns(dfNew.columns, historizeWhitelist, historizeBlacklist, Environment.caseSensitive)
    val dfNewHashed = dfNew.withColumn(historizeHashColName, colsComparisionExpr(colsToCompare.map(col)))
    val dfLastHistHashed = dfLastHist.withColumn(historizeHashColName, colsComparisionExpr(colsToCompare.map(col)))
    val hashColEqualsExpr = col(s"newFeed.$historizeHashColName") === col(s"lastHist.$historizeHashColName")

    val joined = dfNewHashed.as("newFeed")
      .join(dfLastHistHashed.as("lastHist"), joinCols(dfNewHashed, dfLastHistHashed, primaryKeyColumns), "full")

    val newRows = joined.where(col(expiryDateCol).isNull)
      .select(dfNew("*"))
      .withColumn(lastUpdateCol, timestampNew)
      .withColumn(expiryDateCol, doomsday)

    val notInFeedAnymore = joined.where(nullTableCols("newFeed", primaryKeyColumns))
      .select(dfLastHist("*"))
      .withColumn(expiryDateCol, timestampOld)

    val noUpdates = joined
      .where(hashColEqualsExpr)
      .select(dfLastHist("*"))

    val updated = joined
      .where(nonNullTableCols("newFeed", primaryKeyColumns))
      .where(not(hashColEqualsExpr))

    val updatedNew = updated.select(dfNew("*"))
      .withColumn(lastUpdateCol, timestampNew)
      .withColumn(expiryDateCol, doomsday)

    val updatedOld = updated.select(dfLastHist("*"))
      .withColumn(expiryDateCol, timestampOld)

    // column order is used here!
    val dfNewHist = notInFeedAnymore
      .unionByName(newRows)
      .unionByName(updatedNew)
      .unionByName(updatedOld)
      .unionByName(noUpdates)
      .unionByName(restHist)

    if (logger.isDebugEnabled) {
      logger.debug(s"Count previous history: ${dfHistory.count}")
      logger.debug(s"Count current load of feed: ${dfNew.count}")
      logger.debug(s"Count rows not in current feed anymore: ${notInFeedAnymore.count}")
      logger.debug(s"Count new rows: ${newRows.count}")
      logger.debug(s"Count updated rows new: ${updatedNew.count}")
      logger.debug(s"Count updated rows old: ${updatedOld.count}")
      logger.debug(s"Count no updates old: ${noUpdates.count}")
      logger.debug(s"Count rows from remaining history: ${restHist.count}")
      logger.debug(s"Summary count rows new history: ${dfNewHist.count}")
    }

    dfNewHist
  }

  /**
   * Historizes data by merging the current load with the existing history, generating records to update and insert for a SQL Upsert Statement.
   *
   * SQL Upsert statement has great performance potential, but also its limitation:
   * - matched records can be updated or deleted
   * - unmatched records can be inserted
   *
   * Implementing historization with one SQL statement is not possible
   *  - update matched records (close version if column changed) -> supported
   *  - insert matched records (new version if columns changed) -> '''insert on match is not supported'''
   *  - insert unmatched records (new record) -> supported
   *  - update unmatched records in source (deleted record) -> '''not supported in SQL standard''' (MS SQL would have some extension with its MATCHED BY SOURCE/TARGET clause)
   *
   * This functions joins new data with existing current data and generates update and insert records for an SQL Upsert statement.
   * A full outer join between new and existing current data is made and the following records generated:
   *  1. primary key matched and attributes have changed -> update record to close existing version, insert record to create new version
   *  1. primary key unmatched, record only in new data -> insert record
   *  1. primary key unmatched, record only in existing data -> update record to close existing version
   *
   * Existing and new DataFrame are not required to have the same schema, as schema evolution is handled by output DataObject.
   *
   * Compared with fullHistorized the following performance optimizations are implemented:
   *  - only current existing data needs to be read (delimited=doomsday)
   *  - only changed data needs to be written
   *  - a Column with hash-value calculated from all attributes is added to the target table, allowing to use only primary key and hashColumn for joining new data with existing data and detecting changes
   *
   *  Note that the use of hashColumn to detect changed records will create new version for every record on schema evolution.
   *  This behaviour is different from fullHistorize.
   *
   * @param referenceTimestamp The valid from timestamp for new records
   * @param timeAxisUnit       Time between ticks on the timestamp. Used to create valid to timestamp for existing/old records.
   *                           Set to empty to create a history with half-open intervals (e.g. valid to timestamp is exclusive)
   */
  def incrementalHistorize(dfExisting: GenericDataFrame,
                           dfNew: GenericDataFrame,
                           primaryKey: Seq[String],
                           referenceTimestamp: Timestamp, timeAxisUnit: Option[Duration],
                           historizeWhitelist: Option[Seq[String]],
                           historizeBlacklist: Option[Seq[String]],
                           addExistingDfHashColumn: Boolean): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(dfExisting.subFeedType)
    import functions._

    // Current timestamp (used for insert and update operations, for "new" value)
    val timestampNew = lit(referenceTimestamp)
    // Previous entry on time axis before the reference timestamp ("Tick"). This is used to delimit existing old records.
    val timestampOld = lit(timeAxisUnit.map(getPreviousTimeAxisEntry(referenceTimestamp, _)).getOrElse(referenceTimestamp))
    // prepare columns
    val existingCapturedCol = col(s"existing.${Environment.capturedColumnName}")
    val existingDelimitedCol = col(s"existing.${Environment.delimitedColumnName}")
    val existingHashCol = col(s"existing.$historizeHashColName")
    val newHashCol = col(s"new.$historizeHashColName")
    val hashColEqualsExpr = existingHashCol === newHashCol
    // add hash column
    val dfNewHashed = addHashCol(dfNew, historizeWhitelist, historizeBlacklist, useHash = true)
    val dfExistingHashed = if (addExistingDfHashColumn) {
      dfExisting
    } else {
      addHashCol(dfExisting, historizeWhitelist, historizeBlacklist, useHash = true, colsToIgnore = Seq(Environment.capturedColumnName, Environment.delimitedColumnName))
    }
    // join existing with new and determine operations needed
    val dfOperations = dfExistingHashed.as("existing")
      .where(existingDelimitedCol === lit(Environment.historizationUpperHorizonTimestamp)) // only current records needed
      .select((primaryKey :+ Environment.capturedColumnName :+ Environment.delimitedColumnName :+ historizeHashColName).map(col))
      .as("existing")
      .join(dfNewHashed.as("new"), primaryKey, "full")
      .withColumn("_operations",
        // 1. primary key matched and attributes have changed -> update record to close existing version, insert record to create new version
        when(existingHashCol.isNotNull and newHashCol.isNotNull and not(hashColEqualsExpr),
          array(lit(HistorizationRecordOperations.updateClose), lit(HistorizationRecordOperations.insertNew)))
          // 2. record only in new data -> insert new record
        .when(existingHashCol.isNull and newHashCol.isNotNull,
            array(lit(HistorizationRecordOperations.insertNew)))
          // 3. record only in existing data -> update record to close existing version
        .when(existingHashCol.isNotNull and newHashCol.isNull,
            array(lit(HistorizationRecordOperations.updateClose)))
        // 4. primary key matched, no attribute changes, but <historizeHashColName> column has been added -> update existing record
        .when((existingHashCol.isNotNull and newHashCol.isNotNull and hashColEqualsExpr) and lit(!dfExisting.columns.contains(historizeHashColName)),
          array(lit(HistorizationRecordOperations.updateExisting)))
      )
    // add versioning data
    val dfOperationVersioned = dfOperations
      .withColumn(historizeOperationColName, explode(col("_operations"))) // note: this filters records with no action
      .drop("_operations")
      .drop(col(s"existing.$historizeHashColName"))
      .withColumn(Environment.capturedColumnName,
        when(col(historizeOperationColName) === lit(HistorizationRecordOperations.insertNew), timestampNew)
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateClose), existingCapturedCol) // is needed vor merge join condition
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateExisting), existingCapturedCol)
      )
      .withColumn(Environment.delimitedColumnName,
        when(col(historizeOperationColName) === lit(HistorizationRecordOperations.insertNew), lit(Environment.historizationUpperHorizonTimestamp))
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateClose), timestampOld)
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateExisting), existingDelimitedCol)
      )
      .drop(col(s"existing.${Environment.capturedColumnName}"))
      .drop(col(s"existing.${Environment.delimitedColumnName}"))
    // return
    dfOperationVersioned
  }

  /**
   * Historizes data by merging the current load with the existing history, generating records to update and insert for SQL Upsert statements.
   * This algorithm uses information about the delete operation from the source system to optimize historization.
   * If deleted records can be identified, historization can omit the expensive join with existing data and use only SQL Upsert statements.
   * Normally input data from change-data-capture (CDC) data sources has this information.
   *
   * For further description of incremental historization see documentation for [[incrementalHistorize]]
   *
   * The operations produced by incrementalCDCHistorize are
   * 1. updated or inserted record -> update record to close existing version if existing, insert record to create new version
   * 2. deleted record -> update record to close existing version if existing
   *
   * Compared with incrementalHistorize the following performance optimizations are implemented:
   *  - current existing data is not read
   *  - no hash column is needed as we know from the CDC event that something has changed
   *
   * @param referenceTimestamp The valid from timestamp for new records
   * @param timeAxisUnit       Time between ticks on the timestamp. Used to create valid to timestamp for existing/old records.
   *                           Set to empty to create a history with half-open intervals (e.g. valid to timestamp is exclusive)
   */
  def incrementalCDCHistorize(dfNew: GenericDataFrame,
                              deletedRecordsCondition: GenericColumn,
                              referenceTimestamp: Timestamp, timeAxisUnit: Option[Duration],
                             ): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(dfNew.subFeedType)
    import functions._

    // Current timestamp (used for insert and update operations, for "new" value)
    val timestampNew = lit(referenceTimestamp)
    // Previous entry on time axis before the reference timestamp ("Tick"). This is used to delimit existing old records.
    val timestampOld = lit(timeAxisUnit.map(getPreviousTimeAxisEntry(referenceTimestamp, _)).getOrElse(referenceTimestamp))
    // join existing with new and determine operations needed
    val dfOperations = dfNew
      .withColumn("_operations",
        // 1. updated or inserted record -> update record to close existing version if existing, insert record to create new version - dl_hash has to be checked in merge statement
        when(not(deletedRecordsCondition), array(lit(HistorizationRecordOperations.updateClose), lit(HistorizationRecordOperations.insertNew)))
        // 2. deleted record -> update record to close existing version if existing
        .otherwise(array(lit(HistorizationRecordOperations.updateClose)))
      )
    // add versioning data
    val dfOperationVersioned = dfOperations
      .withColumn(historizeOperationColName, explode(col("_operations"))) // note: this filters records with no action
      .drop("_operations")
      .withColumn(historizeDummyColName, // dummy column is needed in merge join condition to avoid deduplication in merge statement
        when(col(historizeOperationColName) === lit(HistorizationRecordOperations.insertNew), lit(false)) // insert should not match with existing records in merge join condition
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateClose), lit(true)) // should match with existing records in merge join condition
      )
      .withColumn(Environment.capturedColumnName,
        when(col(historizeOperationColName) === lit(HistorizationRecordOperations.insertNew), timestampNew)
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateClose), lit(null)) // not needed for incremental CDC merge
      )
      .withColumn(Environment.delimitedColumnName,
        when(col(historizeOperationColName) === lit(HistorizationRecordOperations.insertNew), lit(Environment.historizationUpperHorizonTimestamp))
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateClose), timestampOld)
      )
    // return
    dfOperationVersioned
  }

  /**
   * Creates initial history of feed
   *
   * @param df current run of feed
   * @param referenceTimestamp timestamp to use
   * @return initial history, identical with data from current run
   */
  def getInitialHistory(df: GenericDataFrame, referenceTimestamp: Timestamp): GenericDataFrame = {
    logger.debug(s"Initial history used for ${Environment.capturedColumnName}: $referenceTimestamp")
    addVersionCols(df, referenceTimestamp, Environment.historizationUpperHorizonTimestamp)
  }

  /**
   * Creates initial history of feed for incrementalCDCHistorization
   *
   * @param df current run of feed
   * @param referenceTimestamp timestamp to use
   * @return initial history, identical with data from current run
   */
  def getInitialHistoryWithDummyCol(df: GenericDataFrame, referenceTimestamp: Timestamp): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    logger.debug(s"Initial history used for ${Environment.capturedColumnName}: $referenceTimestamp")
    val df1 = df.withColumn(historizeDummyColName, lit(true))
    addVersionCols(df1, referenceTimestamp, Environment.historizationUpperHorizonTimestamp)
  }

  /**
   * Creates initial history of feed for incrementalHistorization
   *
   * @param df current run of feed
   * @param referenceTimestamp timestamp to use
   * @return initial history, identical with data from current run
   */
  def getInitialHistoryWithHashCol(df: GenericDataFrame, referenceTimestamp: Timestamp, historizeWhitelist: Option[Seq[String]], historizeBlacklist: Option[Seq[String]]): GenericDataFrame = {
    val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    logger.debug(s"Initial history used for ${Environment.capturedColumnName}: $referenceTimestamp")
    val df1 = addHashCol(df, historizeWhitelist, historizeBlacklist, useHash = true)
    addVersionCols(df1, referenceTimestamp, Environment.historizationUpperHorizonTimestamp)
      .withColumn(historizeOperationColName, lit(HistorizationRecordOperations.insertNew))
  }

  private[smartdatalake] def addVersionCols(df: GenericDataFrame, captured: Timestamp, delimited: Timestamp): GenericDataFrame = {
    val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    df.withColumn(Environment.capturedColumnName, lit(captured))
      .withColumn(Environment.delimitedColumnName, lit(delimited))
  }

  private def joinCols(left: GenericDataFrame, right: GenericDataFrame, cols: Seq[String]): GenericColumn = {
    cols.map(c => left(c) === right(c)).reduce(_ and _)
  }

  private def nullTableCols(table: String, cols: Seq[String])(implicit functions: DataFrameFunctions): GenericColumn = {
    cols.map(c => functions.col(s"$table.$c").isNull).reduce(_ and _)
  }

  private def nonNullTableCols(table: String, cols: Seq[String])(implicit functions: DataFrameFunctions): GenericColumn = {
    cols.map(c => functions.col(s"$table.$c").isNotNull).reduce(_ and _)
  }

  private[smartdatalake] def getCompareColumns(colsToUse: Seq[String], historizeWhitelist: Option[Seq[String]], historizeBlacklist: Option[Seq[String]], caseSensitive: Boolean = false): Seq[String] = {
    val colsToCompare = (historizeWhitelist, historizeBlacklist) match {
      case (Some(w), None) => if (caseSensitive) colsToUse.intersect(w) else colsToUse.map(_.toLowerCase).intersect(w.map(_.toLowerCase)) // merged columns from whitelist und dfLastHist without technical columns
      case (None, Some(b)) => if (caseSensitive) colsToUse.diff(b) else colsToUse.map(_.toLowerCase).diff(b.map(_.toLowerCase))
      case (None, None) => if (caseSensitive) colsToUse else colsToUse.map(_.toLowerCase)
      case (Some(_), Some(_)) => throw new ConfigurationException("historize-whitelist and historize-blacklist must not be used at the same time.")
    }
    colsToCompare.toSeq.sorted
  }

  private[smartdatalake] def addHashCol(df: GenericDataFrame, historizeWhitelist: Option[Seq[String]], historizeBlacklist: Option[Seq[String]], useHash: Boolean, colsToIgnore: Seq[String] = Seq()): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    val colsToCompare = getCompareColumns(df.columns.diff(colsToIgnore), historizeWhitelist, historizeBlacklist)
    df.withColumn(historizeHashColName, functions.colsComparisionExpr(colsToCompare.map(functions.col), useHash))
  }

  private def getPreviousTimeAxisEntry(ts: Timestamp, unit: Duration) = Timestamp.from(ts.toInstant.minus(unit))
}

object HistorizationRecordOperations {
  val updateExisting = "updateExisting"
  val updateClose = "updateClose"
  val insertNew = "insertNew"
}
