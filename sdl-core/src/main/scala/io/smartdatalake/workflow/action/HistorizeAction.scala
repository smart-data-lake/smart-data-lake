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
package io.smartdatalake.workflow.action

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions._
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.historization.{Historization, HistorizationRecordOperations}
import io.smartdatalake.util.misc.GenericSchemaUtil
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, GenericDfTransformerDef}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanMergeDataFrame, TransactionalTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataObjectState, SubFeed}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import java.time.{Duration, LocalDateTime}
import scala.reflect.runtime.universe.Type

/**
 * This [[Action]] historizes data between an input and output DataObject using DataFrames.
 * Historization creates a technical history of data by creating valid-from/to columns. The
 * DataFrame might be transformed using SQL or DataFrame transformations. These transformations are
 * applied before the deduplication.
 *
 * By default, a history with closed intervals is created, e.g. valid-from and valid-to is
 * inclusive. The time axis unit can be set by configuration attribute `timeAxisUnit`. It is used as
 * the offset between valid-to of the previous record and valid-from of the current record. A
 * history with half-open intervals can be created by setting timeAxisUnit=0. In a half-open
 * interval valid-from is inclusive and valid-to is exclusive.
 *
 * HistorizeAction needs a transactional table (e.g. implementation of
 * [[TransactionalTableDataObject]]) as output with defined primary keys.
 *
 * Since SDLB version 3.0 incremental historization is used per default, and full historization is removed.
 * Incremental historization does not rewrite all data in output table.
 * It still needs to join new data with all existing data, but uses hash values to minimize data
 * transfer. If you have change-data-capture (CDC) information available to identify deleted
 * records, you can set mergeModeCDCColumn and mergeModeCDCDeletedValue to even avoid the join
 * between new and existing data. This is optimal from a performance perspective.
 *
 * If the input delivers change events using SDLBs standard CDC columns `_change_type`, `_commit_timestamp` and
 * `_change_ordinal` (as DebeziumCdcDataObject does), CDC historization is used without any further configuration,
 * see mergeModeCDCAutoDetect.
 *
 * If you still have a legacy full historization table, migration to incremental historization should happen
 * automatically. The missing hash column is detected and added to existing data.
 *
 * Example:
 * {{{
 * actions = {
 *   historize-airports {
 *     type = HistorizeAction
 *     inputId = stg-airports
 *     outputId = int-airports
 *     timeAxisUnit = 1d
 *     historizeBlacklist = [last_updated]
 *   }
 * }
 * }}}
 *
 * @param inputId
 *   inputs DataObject
 * @param outputId
 *   output DataObject
 * @param historizeBlacklist
 *   optional list of columns to ignore when comparing two records in historization. Can not be used
 *   together with [[historizeWhitelist]].
 * @param historizeWhitelist
 *   optional final list of columns to use when comparing two records in historization. Can not be
 *   used together with [[historizeBlacklist]].
 * @param ignoreOldDeletedColumns
 *   if true, remove no longer existing columns in Schema Evolution
 * @param ignoreOldDeletedNestedColumns
 *   if true, remove no longer existing columns from nested data types in Schema Evolution. Keeping
 *   deleted columns in complex data types has performance impact as all new data in the future has
 *   to be converted by a complex function.
 * @param transformers
 *   optional list of transformations to apply before historization. The transformations are applied
 *   according to the lists ordering.
 * @param mergeModeAdditionalJoinPredicate
 *   To optimize performance it might be interesting to limit the records read from the existing
 *   table data, e.g. it might be sufficient to use only the last 7 days. Specify a condition to
 *   select existing data to be used in transformation as Spark SQL expression. Use table alias
 *   'existing' to reference columns of the existing table data.
 * @param mergeModeCDCColumn
 *   Optional column holding the CDC operation to replay to enable mergeModeCDC. If CDC information
 *   is available from the source incremental historization can be further optimized, as the join
 *   with existing data can be omitted. Note that this should be enabled only, if input data
 *   contains just inserted, updated and deleted records. HistorizeAction in mergeModeCDC will make
 *   *no* change detection on its own, and create a new version for every inserted/updated record it
 *   receives! You will also need to specify parameter mergeModeCDCDeletedValue to use this.
 *   Increment CDC historization will add a column "dl_dummy" to the target table, which is used to
 *   work around limitations of SQL merge statement, but "dl_hash" column from mergeMode is no
 *   longer needed.
 * @param mergeModeCDCDeletedValue
 *   Optional value of mergeModeCDCColumn that marks a record as deleted. It defaults to `delete` if SDLBs standard
 *   CDC columns are used, see mergeModeCDCAutoDetect.
 * @param mergeModeCDCAutoDetect
 *   If true (default), mergeModeCDC is enabled automatically if the input has a column `_change_type`, e.g. it
 *   delivers change events using SDLBs standard CDC columns (see
 *   [[io.smartdatalake.definitions.CdcChangeType]]), as DebeziumCdcDataObject does. No further configuration is
 *   needed in that case: records marked as `delete` close the existing version, records of type `update_preimage`
 *   are ignored, and if several change events exist for the same primary key only the last one is historized. The
 *   order of the change events is defined by column `_change_ordinal`, or `_commit_timestamp` if it is missing.
 *   The CDC columns themselves are not written to the output DataObject.
 * @param mergeModeCDCTimestampColumn
 *   Optional column holding the timestamp of the change in the source system, used as start of validity of the new
 *   version instead of the runs reference timestamp. If empty (default), column `_commit_timestamp` is used if the
 *   input has one, otherwise the reference timestamp. If set, the column must exist and be of type timestamp.
 *   Only used with SDLBs standard CDC columns, see mergeModeCDCAutoDetect.
 * @param mergeModeCDCUseSourceTimestamp
 *   Set to false to always use the runs reference timestamp as start of validity of new versions, e.g. to ignore
 *   mergeModeCDCTimestampColumn and column `_commit_timestamp`. Default is true.
 * @param checkInputUnique
 *   If true, validates that input records have unique primary keys according to output DataObject
 *   primary key before historization. This is a fail-fast mechanism to detect data quality issues
 *   early and prevent incorrect historization. If duplicate keys are found, the job will fail with
 *   details about the duplicate records. Default is false to maintain backward compatibility. In
 *   that case dropDuplicate(primary key columns) is applied on input DataFrame.
 * @param timeAxisUnit
 *   Time between ticks on the time axis. Used to create valid to timestamp for existing/old
 *   records. Set to 0 to create a history with half-open intervals (e.g. valid to timestamp is
 *   exclusive). Format is `x(ns|us|ms|s|m|h|d)`, e.g. 1d. Default is 1ms.
 */
case class HistorizeAction(
                            override val id: ActionId,
                            inputId: DataObjectId,
                            outputId: DataObjectId,
                            transformers: Seq[GenericDfTransformer] = Seq(),
                            historizeBlacklist: Option[Seq[String]] = None,
                            historizeWhitelist: Option[Seq[String]] = None,
                            ignoreOldDeletedColumns: Boolean = false,
                            ignoreOldDeletedNestedColumns: Boolean = true,
                            mergeModeAdditionalJoinPredicate: Option[String] = None,
                            mergeModeCDCColumn: Option[String] = None,
                            mergeModeCDCDeletedValue: Option[String] = None,
                            mergeModeCDCAutoDetect: Boolean = true,
                            mergeModeCDCTimestampColumn: Option[String] = None,
                            mergeModeCDCUseSourceTimestamp: Boolean = true,
                            checkInputUnique: Boolean = false,
                            timeAxisUnit: Duration = Duration.ofMillis(1),
                            override val cacheOutput: Boolean = false,
                            override val cacheInput: Boolean = false,
                            override val executionMode: Option[ExecutionMode] = None,
                            override val executionCondition: Option[Condition] = None,
                            override val metricsFailCondition: Option[String] = None,
                            override val metadata: Option[ActionMetadata] = None,
                            override val engineConnectionId: Option[ConnectionId] = None
)(implicit val instanceRegistry: InstanceRegistry, logger: Logger) extends DataFrameOneToOneActionImpl {

  override val input: DataObject with CanCreateDataFrame = getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  override val output: TransactionalTableDataObject = getOutputDataObject[TransactionalTableDataObject](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[TransactionalTableDataObject] = Seq(output)

  /**
   * Resolved configuration of mergeModeCDC, see [[resolveCdcMode]].
   *
   * @param changeTypeColName name of the column holding the change type/operation
   * @param deletedValue value of changeTypeColName marking a record as deleted
   * @param isStandardCdc true if the input uses SDLBs standard CDC columns, see [[CdcChangeType]].
   *                      Only then change events are prepared by [[Historization.prepareCdcInput]].
   * @param orderColName optional column defining the order of change events of the same primary key
   * @param eventTimestampColName optional column holding the timestamp of the change in the source system,
   *                              used as start of validity of the new version
   * @param metadataColNames CDC metadata columns of the input which must not be written to the output DataObject.
   *                         Note that this includes CDC columns which are not used, e.g. the commit timestamp if
   *                         mergeModeCDCUseSourceTimestamp=false.
   */
  private case class CdcMode(
      changeTypeColName: String,
      deletedValue: String,
      isStandardCdc: Boolean,
      orderColName: Option[String] = None,
      eventTimestampColName: Option[String] = None,
      metadataColNames: Seq[String] = Seq()
  ) {
    def deletedRecordsCondition(implicit f: DataFrameFunctions): GenericColumn =
      f.col(changeTypeColName) === f.lit(deletedValue)
  }

  /**
   * Determine if and how mergeModeCDC is used.
   * This is done on configuration only in preInit phase, and refined as soon as the input schema is known, as
   * SDLBs standard CDC columns are detected by looking at the input schema, see mergeModeCDCAutoDetect.
   */
  private def resolveCdcMode(inputSchema: Option[GenericSchema]): Option[CdcMode] = {
    def colExists(colName: String) = inputSchema.exists(GenericSchemaUtil.columnExists(_, colName))
    def isTimestampCol(colName: String) = inputSchema.forall(
      _.fields.filter(f => if (Environment.caseSensitive) f.name == colName else f.name.equalsIgnoreCase(colName))
        .forall(_.dataType.typeName.toLowerCase.startsWith("timestamp"))
    )
    // note that CDC historization can not compare records, so it is not auto-enabled if a historizeWhitelist is set
    val autoDetect = mergeModeCDCAutoDetect && historizeWhitelist.isEmpty && colExists(Environment.cdcChangeTypeColumnName)
    val changeTypeColNameOpt = mergeModeCDCColumn
      .orElse(if (autoDetect) Some(Environment.cdcChangeTypeColumnName) else None)
    changeTypeColNameOpt.map { changeTypeColName =>
      val isStandardCdc = changeTypeColName == Environment.cdcChangeTypeColumnName
      assert(mergeModeCDCDeletedValue.isDefined || isStandardCdc,
        s"($id) mergeModeCDCDeletedValue must be set when mergeModeCDCColumn is defined")
      assert(historizeWhitelist.isEmpty, s"($id) historizeWhitelist cannot be set when using mergeModeCDC")
      // the ordinal is more precise than the commit timestamp, prefer it if available
      val orderColName = if (isStandardCdc) {
        Seq(Environment.cdcChangeOrdinalColumnName, Environment.cdcCommitTimestampColumnName).find(colExists)
      } else None
      val eventTimestampColName = if (isStandardCdc && mergeModeCDCUseSourceTimestamp) {
        mergeModeCDCTimestampColumn match {
          case Some(colName) => // an explicitly configured column must exist and hold timestamps
            if (inputSchema.isDefined) { // the input schema is not yet known in preInit phase
              assert(colExists(colName), s"($id) mergeModeCDCTimestampColumn '$colName' not found in input schema")
              assert(isTimestampCol(colName), s"($id) mergeModeCDCTimestampColumn '$colName' must be of type timestamp")
            }
            Some(colName)
          case None => Some(Environment.cdcCommitTimestampColumnName)
            .filter(colName => colExists(colName) && isTimestampCol(colName))
        }
      } else None
      // all CDC metadata columns of the input are removed on write, also if they are not used for historization
      val metadataColNames = (changeTypeColName +:
        (if (isStandardCdc) {
          Seq(Environment.cdcCommitTimestampColumnName, Environment.cdcChangeOrdinalColumnName).filter(colExists) ++
            eventTimestampColName
        } else Seq())).distinct
      CdcMode(changeTypeColName, mergeModeCDCDeletedValue.getOrElse(CdcChangeType.delete), isStandardCdc, orderColName,
        eventTimestampColName, metadataColNames)
    }
  }

  // resolved CDC configuration, initialized in preInit and refined in transform when the input schema is known
  private var _cdcMode: Option[CdcMode] = None

  // saveMode options need ActionPipelineContext to initialize
  private var _saveModeOptions: Option[SaveModeOptions] = None
  override def saveModeOptions: Option[SaveModeOptions] = {
    assert(_saveModeOptions.isDefined, s"($id) SaveModeOptions not initialized")
    _saveModeOptions
  }
  private def initSaveModeOptions(implicit context: ActionPipelineContext): Unit =
    _saveModeOptions = if (_cdcMode.isDefined) {
      val cdcMode = _cdcMode.get
      // customize update/insert condition
      val updateCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.updateClose}'")
      val updateCols = Seq(Environment.delimitedColumnName)
      val insertCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.insertNew}'")
      val insertColsToIgnore = Historization.historizeOperationColName +: cdcMode.metadataColNames
      val insertValuesOverride = Map(Historization.historizeDummyColName -> "true")
      val sqlReferenceTimestamp = Timestamp.valueOf(getReferenceTimestamp)
      // different condition for closed and half-closed intervals
      val mergeTimePredicate = if (cdcMode.eventTimestampColName.isDefined) {
        // the validity of a new version starts at the timestamp of the change event, which is different for every
        // record. It is not available as column of the merge statement, but the delimited timestamp of the record to
        // close is derived from it, see Historization.incrementalCDCHistorize.
        if (timeAxisUnitOpt.isDefined)
          s"new.${Environment.delimitedColumnName} between existing.${Environment.capturedColumnName}" +
            s" AND existing.${Environment.delimitedColumnName}"
        else
          s"existing.${Environment.capturedColumnName} <= new.${Environment.delimitedColumnName}" +
            s" AND new.${Environment.delimitedColumnName} < existing.${Environment.delimitedColumnName}"
      } else if (timeAxisUnitOpt.isDefined)
        s"timestamp'$sqlReferenceTimestamp' between existing.${Environment.capturedColumnName}" +
          s" AND existing.${Environment.delimitedColumnName}"
      else
        s"existing.${Environment.capturedColumnName} <= timestamp'$sqlReferenceTimestamp'" +
          s" AND timestamp'$sqlReferenceTimestamp' < existing.${Environment.delimitedColumnName}"
      val additionalMergePredicate =
        Some((s"existing.${Historization.historizeDummyColName} = new.${Historization.historizeDummyColName} AND $mergeTimePredicate" +:
            mergeModeAdditionalJoinPredicate.toSeq).reduce(_ + " and " + _))
      Some(SaveModeMergeOptions(
          updateCondition = updateCondition,
          updateColumns = updateCols,
          insertCondition = insertCondition,
          insertColumnsToIgnore = insertColsToIgnore,
          insertValuesOverride = insertValuesOverride,
          additionalMergePredicate = additionalMergePredicate
        ))

    } else {
      // customize update condition
      val updateCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.updateClose}'")
      val updateCols =
        if (output.isTableExisting && output.getDataFrame(Seq(), subFeedType).schema.columnExists(Historization.historizeHashColName))
          Seq(Environment.delimitedColumnName)
        else Seq(Environment.delimitedColumnName, Historization.historizeHashColName)
      val updateExistingCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.updateExisting}'")
      val insertCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.insertNew}'")
      val insertColsToIgnore = Seq(Historization.historizeOperationColName)
      val additionalMergePredicate = Some((s"new.${Environment.capturedColumnName} = existing.${Environment.capturedColumnName}" +:
          mergeModeAdditionalJoinPredicate.toSeq).reduce(_ + " and " + _))
      Some(SaveModeMergeOptions(
          updateCondition = updateCondition,
          updateColumns = updateCols,
          updateExistingCondition = updateExistingCondition,
          insertCondition = insertCondition,
          insertColumnsToIgnore = insertColsToIgnore,
          additionalMergePredicate = additionalMergePredicate
        ))
    }

  // Output is used as recursive input in DeduplicateAction to get existing data.
  // This override is needed to force tick-tock write operation.
  override val recursiveInputs: Seq[TransactionalTableDataObject] = Seq(output)

  private[smartdatalake] override val handleRecursiveInputsAsSubFeeds: Boolean = false

  // DataFrame created by HistorizeAction should not be passed on to the next Action,
  // but must be recreated from the DataObject.

  // historize black/white list
  require(historizeWhitelist.isEmpty || historizeBlacklist.isEmpty,
    s"($id) HistorizeWhitelist and historizeBlacklist mustn't be used at the same time")
  // primary key
  require(output.table.primaryKey.isDefined, s"($id) Primary key must be defined for output DataObject")

  private val transformerDefs: Seq[GenericDfTransformerDef] = transformers

  override val transformerSubFeedSupportedTypes: Seq[Type] =
    transformerDefs.map(_.getSubFeedSupportedType) // historize transformer can be ignored as it is generic

  private val timeAxisUnitOpt = {
    assert(!timeAxisUnit.isNegative, s"($id) timeAxisUnit must be 0 or a positive duration, but is $timeAxisUnit")
    Some(timeAxisUnit).filter(!_.isZero)
  }

  validateConfig()

  override def validateConfig(): Unit = {
    super.validateConfig()
    assert(
      output.isInstanceOf[CanMergeDataFrame],
      s"($id) output DataObject must support SaveMode.Merge (implement CanMergeDataFrame)"
    )
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    transformerDefs.foreach(_.prepare(id))
  }

  override def preInit(subFeeds: Seq[SubFeed], dataObjectsState: Seq[DataObjectState])(implicit context: ActionPipelineContext): Unit = {
    super.preInit(subFeeds, dataObjectsState)
    // initialize with the CDC configuration known without input schema. It is refined in transform, see initCdcMode.
    _cdcMode = resolveCdcMode(None)
    initSaveModeOptions
  }

  /**
   * Detect SDLBs standard CDC columns in the input schema and adapt saveModeOptions accordingly.
   * This can not be done in preInit, as the input schema is not yet known there. It must be done before the
   * transformers are applied, as saveModeOptions are used to initialize the output DataObject and to convert the
   * DataFrame to the target schema, which both happens after the transformation.
   */
  private def initCdcMode(inputSubFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): Unit = {
    val cdcMode = resolveCdcMode(inputSubFeed.schemaOpt)
    if (cdcMode != _cdcMode) {
      cdcMode.filter(_.isStandardCdc).foreach(m =>
        logger.info(s"($id) using CDC historization with columns ${m.metadataColNames.mkString(", ")}" +
          m.eventTimestampColName.map(c => s", validity of new versions starts at $c").getOrElse(""))
      )
      _cdcMode = cdcMode
      initSaveModeOptions
    }
  }

  private[smartdatalake] override def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfTransformerDef] = {
    val capturedTs = Timestamp.valueOf(getReferenceTimestamp)
    val pks = output.table.primaryKey.get // existence is validated earlier

    // get existing data
    // Note that HistorizeAction with mergeModeEnabled=false needs to read/write all existing data for tick-tock operation,
    // even if only specific partitions have changed
    val existingDf = if (output.isTableExisting) Some(output.getDataFrame(Seq(), subFeedType)) else None

    // historize
    val historizeTransformer = if (_cdcMode.isDefined) {
      val cdcMode = _cdcMode.get
      new GenericDfTransformerDef {
        override val name = "incrementalCDCHistorize"

        override def transform(
            actionId: ActionId,
            partitionValues: Seq[PartitionValues],
            df: GenericDataFrame,
            dataObjectId: DataObjectId,
            previousTransformerName: Option[String],
            executionModeResultOptions: Map[String, String]
        )(implicit context: ActionPipelineContext): GenericDataFrame =
          incrementalCDCHistorizeDataFrame(existingDf, pks, cdcMode, capturedTs, df)
      }
    } else {
      new GenericDfTransformerDef {
        override val name = "incrementalHistorize"

        override def transform(
            actionId: ActionId,
            partitionValues: Seq[PartitionValues],
            df: GenericDataFrame,
            dataObjectId: DataObjectId,
            previousTransformerName: Option[String],
            executionModeResultOptions: Map[String, String]
        )(implicit context: ActionPipelineContext): GenericDataFrame =
          incrementalHistorizeDataFrame(existingDf, pks, capturedTs, df)
      }
    }
    transformerDefs :+ historizeTransformer
  }

  override def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)(implicit
      context: ActionPipelineContext
  ): DataFrameSubFeed = {
    initCdcMode(inputSubFeed)
    applyTransformers(getTransformers, inputSubFeed, outputSubFeed)
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String, String])(implicit
      context: ActionPipelineContext
  ): Map[PartitionValues, PartitionValues] =
    applyTransformers(getTransformers, partitionValues, executionModeResultOptions)

  private def incrementalHistorizeDataFrame(
      existingDf: Option[GenericDataFrame],
      pks: Seq[String],
      refTimestamp: Timestamp,
      newDf: GenericDataFrame
  )(implicit context: ActionPipelineContext): GenericDataFrame = {

    // Check input uniqueness if requested, otherwise just drop duplicates according to primary key.
    // Note that drop duplicate might be non-deterministic and cause attributes switching in history with every run.
    if (checkInputUnique && context.isExecPhase) {
      validateInputUniqueness(newDf, pks)
    }
    val newFeedDf = if (!checkInputUnique) newDf.dropDuplicates(pks) else newDf

    // if context is init, check if column needs to be added -> save in needsHashColumn
    if (!context.isExecPhase) existingDfNeedsHashColumn = existingDf match {
      case Some(df) => Some(!GenericSchemaUtil.columnExists(df.schema, Historization.historizeHashColName))
      case _        => Some(false)
    }

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      if (context.isExecPhase) ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get, Environment.capturedColumnName)
      // historize

      val addExistingDfHashColumn =
        existingDfNeedsHashColumn.getOrElse(throw new IllegalStateException("HistorizeAction not correctly initialized"))
      // note that schema evolution is done by output DataObject
      Historization.incrementalHistorize(existingDf.get, newFeedDf, pks, refTimestamp, timeAxisUnitOpt, historizeWhitelist,
        historizeBlacklist,
        addExistingDfHashColumn)
    } else Historization.getInitialHistoryWithHashCol(newFeedDf, refTimestamp, historizeWhitelist, historizeBlacklist)
  }

  private var existingDfNeedsHashColumn: Option[Boolean] = None

  private def incrementalCDCHistorizeDataFrame(
      existingDf: Option[GenericDataFrame],
      pks: Seq[String],
      cdcMode: CdcMode,
      refTimestamp: Timestamp,
      newDf: GenericDataFrame
  )(implicit context: ActionPipelineContext): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(subFeedType)
    import functions._
    val deletedRecordsCondition = cdcMode.deletedRecordsCondition

    // reduce the change events to the last event per primary key if SDLBs standard CDC columns are used
    val cdcDf = if (cdcMode.isStandardCdc) {
      Historization.prepareCdcInput(newDf, pks, cdcMode.changeTypeColName, cdcMode.orderColName)
    } else newDf

    // Check input uniqueness if requested (excluding deleted records)
    if (checkInputUnique && context.isExecPhase) {
      // For CDC mode, only validate non-deleted records
      val nonDeletedDf = cdcDf.where(not(deletedRecordsCondition))
      validateInputUniqueness(nonDeletedDf, pks)
    }

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      // if the validity of new versions starts at the timestamp of the source system, existing data may be newer
      // than the reference timestamp of this run, so the check is skipped
      if (context.isExecPhase && cdcMode.eventTimestampColName.isEmpty) {
        ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get, Environment.capturedColumnName)
      }
      // historize
      // note that schema evolution is done by output DataObject
      Historization.incrementalCDCHistorize(cdcDf, deletedRecordsCondition, refTimestamp, timeAxisUnitOpt,
        cdcMode.eventTimestampColName)
    } else {
      Historization.getInitialHistoryWithDummyCol(cdcDf, refTimestamp, Some(deletedRecordsCondition),
        cdcMode.eventTimestampColName)
    }
  }

  /**
   * Validates that the input DataFrame has unique primary keys. Throws an exception with details
   * about duplicate records if uniqueness is violated.
   */
  private def validateInputUniqueness(df: GenericDataFrame, pks: Seq[String]): Unit = {
    // Get duplicate records
    val duplicates = df.getNonuniqueRows(pks)
    val duplicateCount = duplicates.count

    if (duplicateCount > 0) {
      // Collect a sample of duplicate records for error message
      val sampleSize = math.min(10, duplicateCount.toInt)
      val pkColsStr = pks.mkString(", ")
      val duplicateSample = duplicates.limit(sampleSize).showString()

      throw DuplicateInputDataException(
        s"($id) Input data uniqueness validation failed: Found $duplicateCount duplicate records based on primary key [$pkColsStr]. " +
          s"Set checkInputUnique=false to disable this check or fix the source data to ensure uniqueness.\n" +
          s"Sample of duplicate records:\n$duplicateSample"
      )
    }
  }

  private def getReferenceTimestamp(implicit context: ActionPipelineContext): LocalDateTime =
    context.referenceTimestamp.getOrElse(LocalDateTime.now)

  override private[smartdatalake] def reset(implicit context: ActionPipelineContext): Unit = {
    super.reset
    existingDfNeedsHashColumn = None
    _cdcMode = None
  }
}

object HistorizeAction extends FromConfigFactory[Action] {
  private implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): HistorizeAction =
    extract[HistorizeAction](config)
}

/**
 * Exception to signal that input data contains duplicate records based on primary key constraints.
 * This is a data quality issue rather than a configuration problem.
 */
case class DuplicateInputDataException(message: String) extends RuntimeException(message)
