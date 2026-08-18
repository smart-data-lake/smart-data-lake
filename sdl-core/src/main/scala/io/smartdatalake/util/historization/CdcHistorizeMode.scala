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
import io.smartdatalake.definitions.{CdcChangeType, Environment, SaveModeMergeOptions, SaveModeOptions}
import io.smartdatalake.util.LogUtils.debugLog
import io.smartdatalake.util.historization.CdcHistorizeMode.{getInitialHistoryWithDummyCol, incrementalCDCHistorize}
import io.smartdatalake.util.historization.Historization.{getPreviousTimeAxisEntry, historizeDummyColName, historizeOperationColName}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import io.smartdatalake.workflow.action.{ActionHelper, HistorizeAction}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame, GenericSchema}
import org.slf4j.Logger

import java.sql.Timestamp
import java.time.Duration

/**
 * Historization strategy replaying change-data-capture (CDC) events, see mergeModeCDCColumn and
 * mergeModeCDCAutoDetect. It needs no join with the existing history, as the change events tell what happened.
 *
 * @param changeTypeColName name of the column holding the change type/operation
 * @param deletedValue value of changeTypeColName marking a record as deleted
 * @param isStandardCdc true if the input uses SDLBs standard CDC columns, see [[CdcChangeType]].
 *                      Only then change events are prepared by [[CdcHistorizeMode.prepareCdcInput]].
 * @param orderColName optional column defining the order of change events of the same primary key
 * @param sourceTimestampColName optional column holding the timestamp of the change in the source system, used as
 *                               start of validity of the new version. Note that this is `_commit_timestamp` for
 *                               standard CDC columns, see HistorizeAction.mergeModeCDCTimestampAutoDetect.
 * @param cdcMetadataColNames CDC metadata columns of the input. Note that this includes CDC columns which are not
 *                            used, e.g. the commit timestamp if mergeModeCDCTimestampAutoDetect=false.
 */
case class CdcHistorizeMode(
                                     changeTypeColName: String,
                                     deletedValue: String,
                                     isStandardCdc: Boolean,
                                     orderColName: Option[String],
                                     override val sourceTimestampColName: Option[String],
                                     cdcMetadataColNames: Seq[String],
                                     action: HistorizeAction
                                   ) extends HistorizeMode with SmartDataLakeLogger {

  override val transformerName: String = "cdcHistorize"

  override def id: SdlConfigObject.ActionId = action.id

  override def excludedColNames: Seq[String] = (cdcMetadataColNames ++ sourceTimestampColName).distinct

  def deletedRecordsCondition(implicit f: DataFrameFunctions): GenericColumn =
    f.col(changeTypeColName) === f.lit(deletedValue)

  override def logInfo(): Unit = {
    if (isStandardCdc) logger.info(s"(${action.id}) using CDC historization with columns ${cdcMetadataColNames.mkString(", ")}")
    super.logInfo()
  }

  override def saveModeOptions(schema: Option[GenericSchema] = None)(implicit context: ActionPipelineContext): SaveModeOptions = {
    val sqlReferenceTimestamp = Timestamp.valueOf(action.getReferenceTimestamp)
    // different condition for closed and half-closed intervals
    val mergeTimePredicate = if (sourceTimestampColName.isDefined) {
      // the validity of a new version starts at the timestamp of the source system, which is different for every
      // record. It is not available as column of the merge statement, but the delimited timestamp of the record to
      // close is derived from it, see [[CdcHistorizeMode.incrementalCDCHistorize]].
      if (action.timeAxisUnitOpt.isDefined)
        s"new.${Environment.delimitedColumnName} between existing.${Environment.capturedColumnName}" +
          s" AND existing.${Environment.delimitedColumnName}"
      else
        s"existing.${Environment.capturedColumnName} <= new.${Environment.delimitedColumnName}" +
          s" AND new.${Environment.delimitedColumnName} < existing.${Environment.delimitedColumnName}"
    } else if (action.timeAxisUnitOpt.isDefined)
      s"timestamp'$sqlReferenceTimestamp' between existing.${Environment.capturedColumnName}" +
        s" AND existing.${Environment.delimitedColumnName}"
    else
      s"existing.${Environment.capturedColumnName} <= timestamp'$sqlReferenceTimestamp'" +
        s" AND timestamp'$sqlReferenceTimestamp' < existing.${Environment.delimitedColumnName}"
    SaveModeMergeOptions(
      updateCondition = Some(operationCondition(HistorizationRecordOperations.updateClose)),
      updateColumns = Seq(Environment.delimitedColumnName),
      insertCondition = Some(operationCondition(HistorizationRecordOperations.insertNew)),
      insertColumnsToIgnore = Historization.historizeOperationColName +: excludedColNames,
      insertValuesOverride = Map(Historization.historizeDummyColName -> "true"),
      additionalMergePredicate = additionalMergePredicate(
        s"existing.${Historization.historizeDummyColName} = new.${Historization.historizeDummyColName} AND $mergeTimePredicate", action.mergeModeAdditionalJoinPredicate.toSeq
      )
    )
  }

  override def historize(existingDf: Option[GenericDataFrame], newDf: GenericDataFrame, pks: Seq[String], refTimestamp: Timestamp)
                        (implicit context: ActionPipelineContext): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(action.subFeedType)
    import functions._

    // reduce the change events to the last event per primary key if SDLBs standard CDC columns are used
    val cdcDf = if (isStandardCdc) CdcHistorizeMode.prepareCdcInput(newDf, pks, changeTypeColName, orderColName) else newDf

    // Check input uniqueness if requested (excluding deleted records)
    if (action.checkInputUnique && context.isExecPhase) {
      action.validateInputUniqueness(cdcDf.where(not(deletedRecordsCondition)), pks)
    }

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      // if the validity of new versions starts at the timestamp of the source system, existing data may be newer
      // than the reference timestamp of this run, so the check is skipped
      if (context.isExecPhase && sourceTimestampColName.isEmpty) {
        ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get, Environment.capturedColumnName)
      }
      // historize
      // note that schema evolution is done by output DataObject
      incrementalCDCHistorize(cdcDf, deletedRecordsCondition, refTimestamp, action.timeAxisUnitOpt, sourceTimestampColName)
    } else {
      getInitialHistoryWithDummyCol(cdcDf, refTimestamp, Some(deletedRecordsCondition), sourceTimestampColName)(logger)
    }
  }
}

object CdcHistorizeMode {

  /**
   * Historizes data by merging the current load with the existing history, generating records to
   * update and insert for SQL Upsert statements. This algorithm uses information about the delete
   * operation from the source system to optimize historization. If deleted records can be
   * identified, historization can omit the expensive join with existing data and use only SQL
   * Upsert statements. Normally input data from change-data-capture (CDC) data sources has this
   * information.
   *
   * For further description of incremental historization see documentation for [[IncrementalHistorizeMode]].
   *
   * The operations produced by incrementalCDCHistorize are
   *   1. updated or inserted record -> update record to close existing version if existing, insert
   *      record to create new version
   *   2. deleted record -> update record to close existing version if existing
   *
   * Compared with incrementalHistorize the following performance optimizations are implemented:
   *   - current existing data is not read
   *   - no hash column is needed as we know from the CDC event that something has changed
   *
   * @param referenceTimestamp
   *   The valid from timestamp for new records, used if eventTimestampColName is empty
   * @param timeAxisUnit
   *   Time between ticks on the timestamp. Used to create valid to timestamp for existing/old
   *   records. Set to empty to create a history with half-open intervals (e.g. valid to timestamp
   *   is exclusive)
   * @param eventTimestampColName
   *   Optional name of a column holding the timestamp when the change happened in the source system, normally
   *   [[Environment.cdcCommitTimestampColumnName]]. If given, the validity of the new version starts at this
   *   timestamp instead of the reference timestamp, e.g. the history reflects the time axis of the source system.
   */
  def incrementalCDCHistorize(
                               dfNew: GenericDataFrame,
                               deletedRecordsCondition: GenericColumn,
                               referenceTimestamp: Timestamp,
                               timeAxisUnit: Option[Duration],
                               eventTimestampColName: Option[String] = None
                             ): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(dfNew.subFeedType)
    import functions._

    // Timestamp when the new version starts to be valid (used for insert and update operations, for "new" value)
    val timestampNew = eventTimestampColName.map(col).getOrElse(lit(referenceTimestamp))
    // Previous entry on time axis before timestampNew ("Tick"). This is used to delimit existing old records.
    val timestampOld = eventTimestampColName.map { c =>
      timeAxisUnit.map(unit => timestampSubtract(col(c), unit)).getOrElse(col(c))
    }.getOrElse(lit(timeAxisUnit.map(getPreviousTimeAxisEntry(referenceTimestamp, _)).getOrElse(referenceTimestamp)))
    // join existing with new and determine operations needed
    val dfOperations = dfNew
      .withColumn(
        "_operations",
        // 1. updated or inserted record -> update record to close existing version if existing, insert record to create new version - dl_hash has to be checked in merge statement
        when(not(deletedRecordsCondition),
          array(lit(HistorizationRecordOperations.updateClose), lit(HistorizationRecordOperations.insertNew)))
          // 2. deleted record -> update record to close existing version if existing
          .otherwise(array(lit(HistorizationRecordOperations.updateClose)))
      )
    // add versioning data
    val dfOperationVersioned = dfOperations
      .withColumn(historizeOperationColName, explode(col("_operations"))) // note: this filters records with no action
      .drop("_operations")
      .withColumn(
        historizeDummyColName, // dummy column is needed in merge join condition to avoid deduplication in merge statement
        when(col(historizeOperationColName) === lit(HistorizationRecordOperations.insertNew),
          lit(false)) // insert should not match with existing records in merge join condition
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateClose),
            lit(true)) // should match with existing records in merge join condition
      )
      .withColumn(
        Environment.capturedColumnName,
        when(col(historizeOperationColName) === lit(HistorizationRecordOperations.insertNew), timestampNew)
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateClose),
            lit(null)) // not needed for incremental CDC merge
      )
      .withColumn(
        Environment.delimitedColumnName,
        when(col(historizeOperationColName) === lit(HistorizationRecordOperations.insertNew),
          lit(Environment.historizationUpperHorizonTimestamp))
          .when(col(historizeOperationColName) === lit(HistorizationRecordOperations.updateClose), timestampOld)
      )
    // return
    dfOperationVersioned
  }

  /**
   * Creates initial history of feed for incrementalCDCHistorization
   *
   * @param df
   *   current run of feed
   * @param referenceTimestamp
   *   timestamp to use if eventTimestampColName is empty
   * @param deletedRecordsCondition
   *   condition marking a record as deleted in the source system. Deleted records are not part of the initial
   *   history, as there is no existing version they could close.
   * @param eventTimestampColName
   *   optional name of a column holding the timestamp when the change happened in the source system, see
   *   [[incrementalCDCHistorize]]
   * @return
   *   initial history, identical with data from current run
   */
  def getInitialHistoryWithDummyCol(
                                     df: GenericDataFrame,
                                     referenceTimestamp: Timestamp,
                                     deletedRecordsCondition: Option[GenericColumn] = None,
                                     eventTimestampColName: Option[String] = None
                                   )(implicit logger: Logger): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    debugLog(s"Initial history used for ${Environment.capturedColumnName}: ${eventTimestampColName.getOrElse(referenceTimestamp)}")
    val dfWithoutDeleted = deletedRecordsCondition.map(condition => df.where(not(condition))).getOrElse(df)
    val df1 = dfWithoutDeleted.withColumn(historizeDummyColName, lit(true))
    df1.withColumn(Environment.capturedColumnName, eventTimestampColName.map(col).getOrElse(lit(referenceTimestamp)))
      .withColumn(Environment.delimitedColumnName, lit(Environment.historizationUpperHorizonTimestamp))
  }


  /**
   * Prepares change-data-capture (CDC) events for [[incrementalCDCHistorize]].
   *
   * A batch of CDC events can contain several events for the same primary key, and events which describe the value
   * of a record before it was changed. As one SQL Upsert statement can create at most one new version per primary
   * key, the events are reduced to the last event per primary key:
   *   1. events of type `update_preimage` are removed, as they describe the value of a record before the update,
   *      which is already stored in the existing history
   *   1. of the remaining events only the last event per primary key is kept, e.g. intermediate states of a record
   *      within the same batch are not historized
   *
   * @param dfNew
   *   change events of the current run
   * @param primaryKey
   *   primary key columns of the output DataObject
   * @param changeTypeColName
   *   name of the column holding the change type, see [[io.smartdatalake.definitions.CdcChangeType]]
   * @param orderColName
   *   optional name of the column defining the order of the events, normally
   *   [[Environment.cdcChangeOrdinalColumnName]] or [[Environment.cdcCommitTimestampColumnName]].
   *   If empty, events are not reduced to the last event per primary key.
   *   Note that events with a null value in this column are dropped, as their order can not be determined.
   */
  def prepareCdcInput(
                                              dfNew: GenericDataFrame,
                                              primaryKey: Seq[String],
                                              changeTypeColName: String,
                                              orderColName: Option[String]
                                            ): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(dfNew.subFeedType)
    import functions._

    // remove preimages, e.g. the value of a record before it was updated. Note that <=> is null-safe, so events
    // without change type are kept.
    val dfEvents = dfNew.where(not(col(changeTypeColName) <=> lit(CdcChangeType.updatePreimage)))

    // keep the last event per primary key
    orderColName.map { orderCol =>
      val dfLastEvents = dfEvents
        .groupBy(primaryKey.map(col))
        .agg(Seq(max(col(orderCol)).as(orderCol)))
      dfEvents.join(dfLastEvents, primaryKey :+ orderCol)
        .select(dfEvents.columns.map(col)) // join by column names reorders columns, restore the original order
        .dropDuplicates(primaryKey) // safety net if several events of the same primary key share the same order value
    }.getOrElse(dfEvents)
  }

}
