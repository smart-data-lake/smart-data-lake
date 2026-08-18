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
import io.smartdatalake.util.historization.{CdcHistorizeMode, Historization, HistorizationRecordOperations, HistorizeMode, IncrementalHistorizeMode}
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
 * By default the validity of a new version starts at the reference timestamp of the run. If the input contains the
 * timestamp of the last change in the source system, it can be used instead by setting sourceTimestampColumn, so
 * that the history reflects the time axis of the source system.
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
 * @param mergeModeCDCTimestampAutoDetect
 *   If true (default), column `_commit_timestamp` is used as sourceTimestampColumn, if the input delivers change
 *   events using SDLBs standard CDC columns (see mergeModeCDCAutoDetect) and sourceTimestampColumn is not set. Set
 *   to false to start the validity of new versions at the runs reference timestamp instead. Note that there is no
 *   auto detection of a source timestamp column outside of CDC historization.
 * @param sourceTimestampColumn
 *   Optional column holding the timestamp of the last change of the record in the source system. If set, the
 *   validity of a new version starts at this timestamp instead of the runs reference timestamp, e.g. the history
 *   reflects the time axis of the source system. The column must exist in the input and be of type timestamp.
 *   Note that the column itself is not historized: it is excluded from change detection and not written to the
 *   output DataObject. Copy it to a column with another name in a transformer if you want to keep it.
 *   Records arriving late, e.g. having a source timestamp older than the version they replace, are delayed to the
 *   next tick on the time axis after that version was captured, in order to avoid negative validity intervals.
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
                            mergeModeCDCTimestampAutoDetect: Boolean = true,
                            sourceTimestampColumn: Option[String] = None,
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
   * Determine the historization strategy to use.
   * This is done on configuration only in preInit phase, and refined as soon as the input schema is known, as
   * SDLBs standard CDC columns are detected by looking at the input schema, see mergeModeCDCAutoDetect.
   */
  private def resolveMode(inputSchema: Option[GenericSchema]): HistorizeMode = {
    def colExists(colName: String) = inputSchema.exists(GenericSchemaUtil.columnExists(_, colName))
    def isTimestampCol(colName: String) = inputSchema.forall(GenericSchemaUtil.columnIsTimestamp(_, colName))
    // note that CDC historization can not compare records, so it is not auto-enabled if a historizeWhitelist is set
    val autoDetectCdc = mergeModeCDCAutoDetect && historizeWhitelist.isEmpty && colExists(Environment.cdcChangeTypeColumnName)
    val changeTypeColNameOpt = mergeModeCDCColumn
      .orElse(if (autoDetectCdc) Some(Environment.cdcChangeTypeColumnName) else None)
    val isStandardCdc = changeTypeColNameOpt.contains(Environment.cdcChangeTypeColumnName)
    // the source timestamp column is never auto-detected, except for the commit timestamp of standard CDC columns
    val sourceTimestampColName = sourceTimestampColumn.map { colName =>
      if (inputSchema.isDefined) { // the input schema is not yet known in preInit phase
        assert(colExists(colName), s"($id) sourceTimestampColumn '$colName' not found in input schema")
        assert(isTimestampCol(colName), s"($id) sourceTimestampColumn '$colName' must be of type timestamp")
      }
      colName
    }.orElse(
      Some(Environment.cdcCommitTimestampColumnName)
        .filter(_ => isStandardCdc && mergeModeCDCTimestampAutoDetect)
        .filter(colName => colExists(colName) && isTimestampCol(colName))
    )
    changeTypeColNameOpt.map { changeTypeColName =>
      assert(mergeModeCDCDeletedValue.isDefined || isStandardCdc,
        s"($id) mergeModeCDCDeletedValue must be set when mergeModeCDCColumn is defined")
      assert(historizeWhitelist.isEmpty, s"($id) historizeWhitelist cannot be set when using mergeModeCDC")
      // the ordinal is more precise than the commit timestamp, prefer it if available
      val orderColName = if (isStandardCdc) {
        Seq(Environment.cdcChangeOrdinalColumnName, Environment.cdcCommitTimestampColumnName).find(colExists)
      } else None
      // all CDC metadata columns of the input are removed on write, also if they are not used for historization
      val cdcMetadataColNames = (changeTypeColName +:
        (if (isStandardCdc) {
          Seq(Environment.cdcCommitTimestampColumnName, Environment.cdcChangeOrdinalColumnName).filter(colExists)
        } else Seq())).distinct
      CdcHistorizeMode(changeTypeColName, mergeModeCDCDeletedValue.getOrElse(CdcChangeType.delete), isStandardCdc,
        orderColName, sourceTimestampColName, cdcMetadataColNames, this)
    }.getOrElse(IncrementalHistorizeMode(sourceTimestampColName, this))
  }

  // historization strategy, initialized in preInit and refined in transform when the input schema is known
  private var _mode: Option[HistorizeMode] = None
  private def mode: HistorizeMode = _mode.getOrElse {
    val resolvedMode = resolveMode(None)
    _mode = Some(resolvedMode)
    resolvedMode
  }

  // saveMode options need ActionPipelineContext to initialize
  private var _saveModeOptions: Option[SaveModeOptions] = None
  override def saveModeOptions: Option[SaveModeOptions] = {
    assert(_saveModeOptions.isDefined, s"($id) SaveModeOptions not initialized")
    _saveModeOptions
  }
  private def initSaveModeOptions(implicit context: ActionPipelineContext): Unit = {
    val schema = if (output.isTableExisting) Some(output.getDataFrame(Seq(), subFeedType).schema) else None
    _saveModeOptions = Some(mode.saveModeOptions(schema))
  }

  // Output is used as recursive input in DeduplicateAction to get existing data.
  // This override is needed to force tick-tock write operation.
  override val recursiveInputs: Seq[TransactionalTableDataObject] = Seq(output)

  override val handleRecursiveInputsAsSubFeeds: Boolean = false

  // DataFrame created by HistorizeAction should not be passed on to the next Action,
  // but must be recreated from the DataObject.

  // historize black/white list
  require(historizeWhitelist.isEmpty || historizeBlacklist.isEmpty,
    s"($id) HistorizeWhitelist and historizeBlacklist mustn't be used at the same time")
  // the source timestamp column is not historized, so it can not be part of the columns to compare
  require(
    sourceTimestampColumn.isEmpty || !historizeWhitelist.exists(_.exists(c => columnNameEquals(c, sourceTimestampColumn.get))),
    s"($id) sourceTimestampColumn mustn't be part of historizeWhitelist, as it is not historized"
  )
  // primary key
  require(output.table.primaryKey.isDefined, s"($id) Primary key must be defined for output DataObject")

  private val transformerDefs: Seq[GenericDfTransformerDef] = transformers

  override val transformerSubFeedSupportedTypes: Seq[Type] =
    transformerDefs.map(_.getSubFeedSupportedType) // historize transformer can be ignored as it is generic

  private[smartdatalake] val timeAxisUnitOpt = {
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
    // initialize with the strategy known without input schema. It is refined in transform, see initMode.
    _mode = Some(resolveMode(None))
    initSaveModeOptions
  }

  /**
   * Detect SDLBs standard CDC columns in the input schema and adapt the historization strategy accordingly.
   * This can not be done in preInit, as the input schema is not yet known there. It must be done before the
   * transformers are applied, as saveModeOptions are used to initialize the output DataObject and to convert the
   * DataFrame to the target schema, which both happens after the transformation.
   */
  private def initMode(inputSubFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): Unit = {
    val resolvedMode = resolveMode(inputSubFeed.schemaOpt)
    if (!_mode.contains(resolvedMode)) {
      resolvedMode.logInfo()
      _mode = Some(resolvedMode)
      initSaveModeOptions
    }
  }

  override def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfTransformerDef] = {
    val capturedTs = Timestamp.valueOf(getReferenceTimestamp)
    val pks = output.table.primaryKey.get // existence is validated earlier
    val historizeMode = mode

    // get existing data
    // Note that HistorizeAction with mergeModeEnabled=false needs to read/write all existing data for tick-tock operation,
    // even if only specific partitions have changed
    val existingDf = if (output.isTableExisting) Some(output.getDataFrame(Seq(), subFeedType)) else None

    // historize
    val historizeTransformer = new GenericDfTransformerDef {
      override val name: String = historizeMode.transformerName

      override def transform(
          actionId: ActionId,
          partitionValues: Seq[PartitionValues],
          df: GenericDataFrame,
          dataObjectId: DataObjectId,
          previousTransformerName: Option[String],
          executionModeResultOptions: Map[String, String]
      )(implicit context: ActionPipelineContext): GenericDataFrame =
        historizeMode.historize(existingDf, df, pks, capturedTs)
    }
    transformerDefs :+ historizeTransformer
  }

  override def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)(implicit
      context: ActionPipelineContext
  ): DataFrameSubFeed = {
    initMode(inputSubFeed)
    applyTransformers(getTransformers, inputSubFeed, outputSubFeed)
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String, String])(implicit
      context: ActionPipelineContext
  ): Map[PartitionValues, PartitionValues] =
    applyTransformers(getTransformers, partitionValues, executionModeResultOptions)

  /**
   * Validates that the input DataFrame has unique primary keys. Throws an exception with details
   * about duplicate records if uniqueness is violated.
   */
  private[smartdatalake] def validateInputUniqueness(df: GenericDataFrame, pks: Seq[String]): Unit = {
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

  private[smartdatalake] def getReferenceTimestamp(implicit context: ActionPipelineContext): LocalDateTime =
    context.referenceTimestamp.getOrElse(LocalDateTime.now)

  private def columnNameEquals(colName1: String, colName2: String): Boolean =
    if (Environment.caseSensitive) colName1 == colName2 else colName1.equalsIgnoreCase(colName2)

  override private[smartdatalake] def reset(implicit context: ActionPipelineContext): Unit = {
    super.reset
    _mode = None
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
