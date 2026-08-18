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

import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.definitions.{Environment, SaveModeMergeOptions, SaveModeOptions}
import io.smartdatalake.util.LogUtils.debugLog
import io.smartdatalake.util.historization.Historization.{addHashCol, getPreviousTimeAxisEntry, historizeDummyColName, historizeHashColName, historizeOperationColName, timestampNewColName, timestampOldColName}
import io.smartdatalake.util.historization.IncrementalHistorizeMode.{getInitialHistoryWithHashCol, incrementalHistorize}
import io.smartdatalake.util.misc.GenericSchemaUtil
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import io.smartdatalake.workflow.action.{ActionHelper, HistorizeAction}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame, GenericSchema}
import org.slf4j.Logger

import java.sql.Timestamp
import java.time.Duration

/**
 * Default historization strategy. New data is joined with the existing history to detect changed records.
 */
case class IncrementalHistorizeMode(override val sourceTimestampColName: Option[String], action: HistorizeAction)
  extends HistorizeMode {

  override val transformerName: String = "incrementalHistorize"

  override def id: SdlConfigObject.ActionId = action.id

  override def excludedColNames: Seq[String] = sourceTimestampColName.toSeq

  private var existingDfNeedsHashColumn: Option[Boolean] = None

  override def saveModeOptions(schema: Option[GenericSchema])(implicit context: ActionPipelineContext): SaveModeOptions = {
    // the hash column is added to the target table if it is still missing, e.g. when migrating a legacy full history
    val updateCols =
      if (schema.exists(_.columnExists(Historization.historizeHashColName)))
        Seq(Environment.delimitedColumnName)
      else Seq(Environment.delimitedColumnName, Historization.historizeHashColName)
    SaveModeMergeOptions(
      updateCondition = Some(operationCondition(HistorizationRecordOperations.updateClose)),
      updateColumns = updateCols,
      updateExistingCondition = Some(operationCondition(HistorizationRecordOperations.updateExisting)),
      insertCondition = Some(operationCondition(HistorizationRecordOperations.insertNew)),
      insertColumnsToIgnore = Historization.historizeOperationColName +: excludedColNames,
      additionalMergePredicate =
        additionalMergePredicate(s"new.${Environment.capturedColumnName} = existing.${Environment.capturedColumnName}", action.mergeModeAdditionalJoinPredicate.toSeq)
    )
  }

  override def historize(existingDf: Option[GenericDataFrame], newDf: GenericDataFrame, pks: Seq[String], refTimestamp: Timestamp)
                        (implicit context: ActionPipelineContext): GenericDataFrame = {

    // Check input uniqueness if requested, otherwise just drop duplicates according to primary key.
    // Note that drop duplicate might be non-deterministic and cause attributes switching in history with every run.
    if (action.checkInputUnique && context.isExecPhase) {
      action.validateInputUniqueness(newDf, pks)
    }
    val newFeedDf = if (!action.checkInputUnique) newDf.dropDuplicates(pks) else newDf

    // if context is init, check if column needs to be added -> save in needsHashColumn
    if (!context.isExecPhase) existingDfNeedsHashColumn = existingDf match {
      case Some(df) => Some(!GenericSchemaUtil.columnExists(df.schema, Historization.historizeHashColName))
      case _ => Some(false)
    }

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      // if the validity of new versions starts at the timestamp of the source system, existing data may be newer
      // than the reference timestamp of this run, so the check is skipped
      if (context.isExecPhase && sourceTimestampColName.isEmpty) {
        ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get, Environment.capturedColumnName)
      }
      // historize
      val addExistingDfHashColumn =
        existingDfNeedsHashColumn.getOrElse(throw new IllegalStateException("HistorizeAction not correctly initialized"))
      // note that schema evolution is done by output DataObject
      incrementalHistorize(existingDf.get, newFeedDf, pks, refTimestamp, action.timeAxisUnitOpt, action.historizeWhitelist,
        action.historizeBlacklist, addExistingDfHashColumn, sourceTimestampColName)
    } else {
      getInitialHistoryWithHashCol(newFeedDf, refTimestamp, action.historizeWhitelist, action.historizeBlacklist,
        sourceTimestampColName)(logger)
    }
  }
}

object IncrementalHistorizeMode {

  /**
   * Historizes data by merging the current load with the existing history, generating records to
   * update and insert for a SQL Upsert Statement.
   *
   * SQL Upsert statement has great performance potential, but also its limitation:
   *   - matched records can be updated or deleted
   *   - unmatched records can be inserted
   *
   * Implementing historization with one SQL statement is not possible
   *   - update matched records (close version if column changed) -> supported
   *   - insert matched records (new version if columns changed) -> '''insert on match is not
   *     supported'''
   *   - insert unmatched records (new record) -> supported
   *   - update unmatched records in source (deleted record) -> '''not supported in SQL standard'''
   *     (MS SQL would have some extension with its MATCHED BY SOURCE/TARGET clause)
   *
   * This functions joins new data with existing current data and generates update and insert
   * records for an SQL Upsert statement. A full outer join between new and existing current data is
   * made and the following records generated:
   *   1. primary key matched and attributes have changed -> update record to close existing
   *      version, insert record to create new version
   *   1. primary key unmatched, record only in new data -> insert record
   *   1. primary key unmatched, record only in existing data -> update record to close existing
   *      version
   *
   * Existing and new DataFrame are not required to have the same schema, as schema evolution is
   * handled by output DataObject.
   *
   * Compared with a legacy full historize the following performance optimizations are implemented:
   *   - only current existing data needs to be read (delimited=doomsday)
   *   - only changed data needs to be written
   *   - a Column with hash-value calculated from all attributes is added to the target table,
   *     allowing to use only primary key and hashColumn for joining new data with existing data and
   *     detecting changes
   *
   * @param referenceTimestamp
   *   The valid from timestamp for new records, used if sourceTimestampColName is empty
   * @param timeAxisUnit
   *   Time between ticks on the timestamp. Used to create valid to timestamp for existing/old
   *   records. Set to empty to create a history with half-open intervals (e.g. valid to timestamp
   *   is exclusive)
   * @param sourceTimestampColName
   *   Optional name of a column holding the timestamp when the record was changed in the source system. If given,
   *   the validity of a new version starts at this timestamp instead of the reference timestamp, e.g. the history
   *   reflects the time axis of the source system. The column is excluded from change detection and not part of
   *   the historized data.
   */
  def incrementalHistorize(
                            dfExisting: GenericDataFrame,
                            dfNew: GenericDataFrame,
                            primaryKey: Seq[String],
                            referenceTimestamp: Timestamp,
                            timeAxisUnit: Option[Duration],
                            historizeWhitelist: Option[Seq[String]],
                            historizeBlacklist: Option[Seq[String]],
                            addExistingDfHashColumn: Boolean,
                            sourceTimestampColName: Option[String] = None
                          ): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(dfExisting.subFeedType)
    import functions._
    import io.smartdatalake.util.misc.SeqUtil.SeqStringExtension

    // prepare columns
    val existingCapturedCol = col(s"existing.${Environment.capturedColumnName}")
    val existingDelimitedCol = col(s"existing.${Environment.delimitedColumnName}")
    val existingHashCol = col(s"existing.$historizeHashColName")
    val newHashCol = col(s"new.$historizeHashColName")
    val hashColEqualsExpr = existingHashCol === newHashCol
    // add hash column. Note that the source timestamp column is not part of the historized data and therefore not
    // used for change detection, otherwise a new version would be created for every change of the source timestamp.
    val dfNewHashed = addHashCol(dfNew, historizeWhitelist, historizeBlacklist, useHash = true,
      colsToIgnore = sourceTimestampColName.toSeq)
    val dfExistingHashed = if (addExistingDfHashColumn) {
      addHashCol(dfExisting, historizeWhitelist, historizeBlacklist, useHash = true,
        colsToIgnore = Seq(Environment.capturedColumnName, Environment.delimitedColumnName) ++ sourceTimestampColName)
    } else dfExisting
    // join existing with new and determine operations needed
    val dfOperations = dfExistingHashed.as("existing")
      .where(existingDelimitedCol === lit(Environment.historizationUpperHorizonTimestamp)) // only current records needed
      .select((primaryKey :+ Environment.capturedColumnName :+ Environment.delimitedColumnName :+ historizeHashColName).map(col))
      .as("existing")
      .join(dfNewHashed.as("new"), primaryKey, "full")
      .withColumn(
        "_operations",
        // 1. primary key matched and attributes have changed -> update record to close existing version, insert record to create new version
        when(
          existingHashCol.isNotNull and newHashCol.isNotNull and not(hashColEqualsExpr),
          array(lit(HistorizationRecordOperations.updateClose), lit(HistorizationRecordOperations.insertNew))
        )
          // 2. record only in new data -> insert new record
          .when(existingHashCol.isNull and newHashCol.isNotNull,
            array(lit(HistorizationRecordOperations.insertNew)))
          // 3. record only in existing data -> update record to close existing version
          .when(existingHashCol.isNotNull and newHashCol.isNull,
            array(lit(HistorizationRecordOperations.updateClose)))
          // 4. primary key matched, no attribute changes, but <historizeHashColName> column has been added -> update existing record
          .when(
            (existingHashCol.isNotNull and newHashCol.isNotNull and hashColEqualsExpr) and
              lit(!dfExisting.columns.contains(historizeHashColName)),
            array(lit(HistorizationRecordOperations.updateExisting))
          )
      )
    // Timestamp when a new version starts to be valid ("timestampNew"), and previous entry on the time axis
    // ("Tick") used to delimit the existing version ("timestampOld").
    // They are the same for all records if the reference timestamp of the run is used, but differ per record if a
    // source timestamp column is given. In the latter case they are prepared as temporary columns, as they are
    // needed twice below. Note that they must be created before capturedColumnName is overwritten, as they are
    // derived from the captured timestamp of the existing version.
    val (dfOperationsWithTimestamps, timestampNew, timestampOld) = sourceTimestampColName.map { sourceTimestampCol =>
      val sourceTimestamp = col(s"new.$sourceTimestampCol")
      // A new version must not start before the version it replaces, otherwise the history would contain intervals
      // with delimited < captured. Records arriving late are therefore delayed to the next tick after the existing
      // version was captured.
      val minTimestampNew = timeAxisUnit.map(unit => timestampAdd(existingCapturedCol, unit)).getOrElse(existingCapturedCol)
      val timestampNewExpr = when(sourceTimestamp.isNull, lit(referenceTimestamp)) // record deleted in source system
        .when(existingCapturedCol.isNull, sourceTimestamp) // record not yet existing in history
        .when(sourceTimestamp < minTimestampNew, minTimestampNew) // record arriving late
        .otherwise(sourceTimestamp)
      val timestampOldExpr = timeAxisUnit.map(unit => timestampSubtract(col(timestampNewColName), unit))
        .getOrElse(col(timestampNewColName))
      val df = dfOperations
        .withColumn(timestampNewColName, timestampNewExpr)
        .withColumn(timestampOldColName, timestampOldExpr)
      (df, col(timestampNewColName), col(timestampOldColName))
    }.getOrElse((
      dfOperations,
      lit(referenceTimestamp),
      lit(timeAxisUnit.map(getPreviousTimeAxisEntry(referenceTimestamp, _)).getOrElse(referenceTimestamp))
    ))
    // add versioning data
    val dfOperationVersioned = dfOperationsWithTimestamps
      .withColumn(historizeOperationColName, explode(col("_operations"))) // note: this filters records with no action
      .drop("_operations")
      .drop(col(s"existing.$historizeHashColName"))
      .withColumn(
        Environment.capturedColumnName,
        when(col(historizeOperationColName) === lit(HistorizationRecordOperations.insertNew), timestampNew)
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateClose),
            existingCapturedCol) // is needed vor merge join condition
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateExisting), existingCapturedCol)
      )
      .withColumn(
        Environment.delimitedColumnName,
        when(col(historizeOperationColName) === lit(HistorizationRecordOperations.insertNew),
          lit(Environment.historizationUpperHorizonTimestamp))
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateClose), timestampOld)
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateExisting), existingDelimitedCol)
      )
      .drop(col(s"existing.${Environment.capturedColumnName}"))
      .drop(col(s"existing.${Environment.delimitedColumnName}"))
    // return
    val techCols = Seq(historizeOperationColName, Environment.capturedColumnName, Environment.delimitedColumnName, historizeHashColName)
    val resultColOrder = dfExisting.columns.caseSensitiveDiff(techCols :+ historizeDummyColName) ++
      dfNew.columns.caseSensitiveDiff(dfExistingHashed.columns) ++
      techCols
    val dfResult = dfOperationVersioned
      .select(resultColOrder.map(col))
    dfResult
  }


  /**
   * Creates initial history of feed for incrementalHistorization
   *
   * @param df
   *   current run of feed
   * @param referenceTimestamp
   *   timestamp to use if sourceTimestampColName is empty
   * @param sourceTimestampColName
   *   optional name of a column holding the timestamp when the record was changed in the source system, see
   *   [[incrementalHistorize]]
   * @return
   *   initial history, identical with data from current run
   */
  def getInitialHistoryWithHashCol(
                                    df: GenericDataFrame,
                                    referenceTimestamp: Timestamp,
                                    historizeWhitelist: Option[Seq[String]],
                                    historizeBlacklist: Option[Seq[String]],
                                    sourceTimestampColName: Option[String] = None
                                  )(implicit logger: Logger): GenericDataFrame = {
    val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    debugLog(s"Initial history used for ${Environment.capturedColumnName}: ${sourceTimestampColName.getOrElse(referenceTimestamp)}")
    val df1 = addHashCol(df, historizeWhitelist, historizeBlacklist, useHash = true,
      colsToIgnore = sourceTimestampColName.toSeq)
    df1.withColumn(Environment.capturedColumnName, sourceTimestampColName.map(col).getOrElse(lit(referenceTimestamp)))
      .withColumn(Environment.delimitedColumnName, lit(Environment.historizationUpperHorizonTimestamp))
      .withColumn(historizeOperationColName, lit(HistorizationRecordOperations.insertNew))
  }

}