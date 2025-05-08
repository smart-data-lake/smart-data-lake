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
package io.smartdatalake.workflow.action

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions._
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.historization.{Historization, HistorizationRecordOperations}
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, GenericDfTransformerDef}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame}
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanMergeDataFrame, DataObject, TransactionalTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataObjectState, SubFeed}

import java.sql.Timestamp
import java.time.{Duration, LocalDateTime}
import scala.reflect.runtime.universe.Type
import scala.util.{Failure, Success, Try}

/**
 * This [[Action]] historizes data between an input and output DataObject using DataFrames.
 * Historization creates a technical history of data by creating valid-from/to columns.
 * The DataFrame might be transformed using SQL or DataFrame transformations. These transformations are applied before the deduplication.
 *
 * By default, a history with closed intervals is created, e.g. valid-from and valid-to is inclusive.
 * The time axis unit can be set by configuration attribute `timeAxisUnit`. It is used as the offset between valid-to of the previous record and valid-from of the current record.
 * A history with half-open intervals can be created by setting timeAxisUnit=0. In a half-open interval valid-from is inclusive and valid-to is exclusive.
 *
 * HistorizeAction needs a transactional table (e.g. implementation of [[TransactionalTableDataObject]]) as output with defined primary keys.
 *
 * Normal historization join new with all existing data, and rewrites all data in output table. This is not optimal from
 * a performance perspective.
 * It can be optimized if output object supports [[CanMergeDataFrame]]. In that case you can
 * set mergeModeEnable=true to use incremental historization, which does not rewrite all data in output table. It still needs to
 * join new data with all existing data, but uses hash values to minimize data transfer.
 * If you have change-data-capture (CDC) information available to identify deleted records, you can set
 * mergeModeCDCColumn and mergeModeCDCDeletedValue to even avoid the join between new and existing data. This is optimal from
 * a performance perspective.
 *
 * @param inputId inputs DataObject
 * @param outputId output DataObject
 * @param filterClause Filter of data to be processed by historization. It can be used to exclude historical data not needed to create new history, for performance reasons.
 *                     Note that filterClause is only applied if mergeModeEnable=false. Use mergeModeAdditionalJoinPredicate if mergeModeEnable=true to achieve a similar performance tuning.
 * @param historizeBlacklist optional list of columns to ignore when comparing two records in historization. Can not be used together with [[historizeWhitelist]].
 * @param historizeWhitelist optional final list of columns to use when comparing two records in historization. Can not be used together with [[historizeBlacklist]].
 * @param ignoreOldDeletedColumns if true, remove no longer existing columns in Schema Evolution
 * @param ignoreOldDeletedNestedColumns if true, remove no longer existing columns from nested data types in Schema Evolution.
 *                                      Keeping deleted columns in complex data types has performance impact as all new data
 *                                      in the future has to be converted by a complex function.
 * @param transformer optional custom transformation to apply
 * @param transformers optional list of transformations to apply before historization. The transformations are applied according to the lists ordering.
 * @param mergeModeEnable Set to true to use saveMode.Merge for much better performance by using incremental historization.
 *                        Output DataObject must implement [[CanMergeDataFrame]] if enabled (default = false).
 *                        Incremental historization will add an additional "dl_hash" column which is used for change detection between
 *                        existing and new data.
 *                        Note that enabling mergeMode on an existing HistorizeAction will create a new version for every
 *                        new record in the output table, as "dl_hash" column is initially null.
 * @param mergeModeAdditionalJoinPredicate To optimize performance it might be interesting to limit the records read from the existing table data, e.g. it might be sufficient to use only the last 7 days.
 *                                         Specify a condition to select existing data to be used in transformation as Spark SQL expression.
 *                                         Use table alias 'existing' to reference columns of the existing table data.
 * @param mergeModeCDCColumn Optional colum holding the CDC operation to replay to enable mergeModeCDC. If CDC information is available from the source
 *                           incremental historization can be further optimized, as the join with existing data can be omitted.
 *                           Note that this should be enabled only, if input data contains just inserted, updated and deleted records.
 *                           HistorizeAction in mergeModeCDC will make *no* change detection on its own, and create a new version for every inserted/updated record it receives!
 *                           You will also need to specify parameter mergeModeCDCDeletedValue to use this and mergeModeEnable=true.
 *                           Increment CDC historization will add an additional column "dl_dummy" to the target table,
 *                           which is used to work around limitations of SQL merge statement, but "dl_hash" column from mergeMode is no longer needed.
 * @param mergeModeCDCDeletedValue Optional value of mergeModeCDCColumn that marks a record as deleted.
 * @param timeAxisUnit             Time between ticks on the time axis. Used to create valid to timestamp for existing/old records.
 *                                 Set to 0 to create a history with half-open intervals (e.g. valid to timestamp is exclusive).
 *                                 Format is `x(ns|us|ms|s|m|h|d)`, e.g. 1d.
 *                                 Default is 1ms.
 */
case class HistorizeAction(
                            override val id: ActionId,
                            inputId: DataObjectId,
                            outputId: DataObjectId,
                            @Deprecated @deprecated("Use transformers instead.", "2.0.5")
                            transformer: Option[CustomDfTransformerConfig] = None,
                            transformers: Seq[GenericDfTransformer] = Seq(),
                            filterClause: Option[String] = None,
                            historizeBlacklist: Option[Seq[String]] = None,
                            historizeWhitelist: Option[Seq[String]] = None,
                            ignoreOldDeletedColumns: Boolean = false,
                            ignoreOldDeletedNestedColumns: Boolean = true,
                            @Deprecated @deprecated("mergeModeEnable will be always true in future - make sure to use a DataObject with Implementation of CanMergeDataFrame.", "2.0.8")
                            mergeModeEnable: Boolean = false,
                            mergeModeAdditionalJoinPredicate: Option[String] = None,
                            mergeModeCDCColumn: Option[String] = None,
                            mergeModeCDCDeletedValue: Option[String] = None,
                            timeAxisUnit: Duration = Duration.ofMillis(1),
                            override val breakDataFrameLineage: Boolean = false,
                            override val persist: Boolean = false,
                            override val executionMode: Option[ExecutionMode] = None,
                            override val executionCondition: Option[Condition] = None,
                            override val metricsFailCondition: Option[String] = None,
                            override val metadata: Option[ActionMetadata] = None
                          )(implicit instanceRegistry: InstanceRegistry) extends DataFrameOneToOneActionImpl {

  override val input: DataObject with CanCreateDataFrame = getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  override val output: TransactionalTableDataObject = getOutputDataObject[TransactionalTableDataObject](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[TransactionalTableDataObject] = Seq(output)

  private lazy val mergeModeAdditionalJoinPredicateExpr: Option[GenericColumn] = try {
    implicit val f: DataFrameFunctions = DataFrameSubFeed.getFunctions(subFeedType)
    mergeModeAdditionalJoinPredicate.map(f.expr)
  } catch {
    case ex: Exception => throw new ConfigurationException(s"($id) Cannot parse mergeModeAdditionalJoinPredicate as Spark expression: ${ex.getClass.getSimpleName} ${ex.getMessage}", Some(s"{$id.id}.mergeModeAdditionalJoinPredicate"), ex)
  }
  private lazy val mergeModeDeletedRecordsConditionExpr: Option[GenericColumn] = {
    implicit val f: DataFrameFunctions = DataFrameSubFeed.getFunctions(subFeedType)
    mergeModeCDCColumn.map{ x =>
      assert(mergeModeCDCDeletedValue.isDefined, s"($id) mergeModeCDCDeletedValue must be set when mergeModeCDCColumn is defined")
      assert(historizeWhitelist.isEmpty, s"($id) historizeWhitelist cannot be set when mergeModeCDCColumn is defined")
      f.col(x) === f.lit(mergeModeCDCDeletedValue.get)
    }
  }

  // saveMode options need ActionPipelineContext to initialize
  private var _saveModeOptions: Option[SaveModeOptions] = None
  override def saveModeOptions: Option[SaveModeOptions] = {
    assert(_saveModeOptions.isDefined, s"($id) SaveModeOptions not initialized")
    _saveModeOptions
  }
  def initSaveModeOptions(implicit context: ActionPipelineContext): Unit = {
    _saveModeOptions = if (mergeModeEnable && mergeModeDeletedRecordsConditionExpr.isDefined) {
      // customize update/insert condition
      val updateCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.updateClose}'")
      val updateCols = Seq(Environment.delimitedColumnName)
      val insertCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.insertNew}'")
      val insertColsToIgnore = Seq(Historization.historizeOperationColName, mergeModeCDCColumn.get)
      val insertValuesOverride = Map(Historization.historizeDummyColName -> "true")
      val sqlReferenceTimestamp = Timestamp.valueOf(getReferenceTimestamp)
      // different condition for closed and half-closed intervals
      val mergeTimePredicate = if (timeAxisUnitOpt.isDefined) s"timestamp'$sqlReferenceTimestamp' between existing.${Environment.capturedColumnName} AND existing.${Environment.delimitedColumnName}"
      else s"existing.${Environment.capturedColumnName} <= timestamp'$sqlReferenceTimestamp' AND timestamp'$sqlReferenceTimestamp' < existing.${Environment.delimitedColumnName}"
      val additionalMergePredicate = Some((s"existing.${Historization.historizeDummyColName} = new.${Historization.historizeDummyColName} AND $mergeTimePredicate" +: mergeModeAdditionalJoinPredicate.toSeq).reduce(_ + " and " + _))
      Some(SaveModeMergeOptions(updateCondition = updateCondition, updateColumns = updateCols, insertCondition = insertCondition, insertColumnsToIgnore = insertColsToIgnore, insertValuesOverride = insertValuesOverride, additionalMergePredicate = additionalMergePredicate))

    } else if (mergeModeEnable) {
      // customize update condition
      val updateCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.updateClose}'")
      val updateCols = if (output.isTableExisting && output.getDataFrame(Seq(), subFeedType).schema.columnExists(Historization.historizeHashColName)) Seq(Environment.delimitedColumnName)
      else Seq(Environment.delimitedColumnName, Historization.historizeHashColName)
      val updateExistingCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.updateExisting}'")
      val insertCondition =  Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.insertNew}'")
      val insertColsToIgnore = Seq(Historization.historizeOperationColName)
      val additionalMergePredicate = Some((s"new.${Environment.capturedColumnName} = existing.${Environment.capturedColumnName}" +: mergeModeAdditionalJoinPredicate.toSeq).reduce(_ + " and " + _))
      Some(SaveModeMergeOptions(updateCondition = updateCondition, updateColumns = updateCols, updateExistingCondition = updateExistingCondition, insertCondition = insertCondition, insertColumnsToIgnore = insertColsToIgnore, additionalMergePredicate = additionalMergePredicate))
    } else {
      // force SDLSaveMode.Overwrite otherwise
      Some(SaveModeGenericOptions(SDLSaveMode.Overwrite))
    }
  }

  // Output is used as recursive input in DeduplicateAction to get existing data. This override is needed to force tick-tock write operation.
  override val recursiveInputs: Seq[TransactionalTableDataObject] = Seq(output)

  private[smartdatalake] override val handleRecursiveInputsAsSubFeeds: Boolean = false

  // DataFrame created by HistorizeAction should not be passed on to the next Action, but must be recreated from the DataObject.
  override val breakDataFrameOutputLineage: Boolean = true

  // historize black/white list
  require(historizeWhitelist.isEmpty || historizeBlacklist.isEmpty, s"($id) HistorizeWhitelist and historizeBlacklist mustn't be used at the same time")
  // primary key
  require(output.table.primaryKey.isDefined, s"($id) Primary key must be defined for output DataObject")

  private val transformerDefs: Seq[GenericDfTransformerDef] = transformer.map(t => t.impl).toSeq ++ transformers

  override val transformerSubFeedSupportedTypes: Seq[Type] = transformerDefs.map(_.getSubFeedSupportedType) // historize transformer can be ignored as it is generic

  private val timeAxisUnitOpt = {
    assert(!timeAxisUnit.isNegative, s"($id) timeAxisUnit must be 0 or a positive duration, but is $timeAxisUnit")
    Some(timeAxisUnit).filter(!_.isZero)
  }

  validateConfig()

  override def validateConfig(): Unit = {
    super.validateConfig()
    if (!mergeModeEnable) logger.warn(s"($id) mergeModeEnable = false will not be supported in future anymore, please change to a DataObject with Implementation of CanMergeDataFrame otherwise your code will fail at some point.")
    if (mergeModeEnable) assert(output.isInstanceOf[CanMergeDataFrame], s"($id) output DataObject must support SaveMode.Merge (implement CanMergeDataFrame) if mergeModeEnable = true")
    if (!mergeModeEnable && mergeModeAdditionalJoinPredicateExpr.nonEmpty) logger.warn(s"($id) Configuration of mergeModeAdditionalJoinPredicate has no effect if mergeModeEnable = false")
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    transformerDefs.foreach(_.prepare(id))
  }

  override def preInit(subFeeds: Seq[SubFeed], dataObjectsState: Seq[DataObjectState])(implicit context: ActionPipelineContext): Unit = {
    super.preInit(subFeeds, dataObjectsState)
    initSaveModeOptions
  }

  private[smartdatalake] override def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfTransformerDef] = {
    val capturedTs = Timestamp.valueOf(getReferenceTimestamp)
    val pks = output.table.primaryKey.get // existance is validated earlier

    // get existing data
    // Note that HistorizeAction with mergeModeEnabled=false needs to read/write all existing data for tick-tock operation, even if only specific partitions have changed
    val existingDf = if (output.isTableExisting) Some(output.getDataFrame(Seq(), subFeedType)) else None

    // historize
    val historizeTransformer = if (mergeModeEnable && mergeModeDeletedRecordsConditionExpr.isDefined) {
      new GenericDfTransformerDef {
        override val name = "incrementalCDCHistorize"

        override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {
          incrementalCDCHistorizeDataFrame(existingDf, pks, mergeModeDeletedRecordsConditionExpr.get, capturedTs, df)
        }
      }
    } else if (mergeModeEnable) {
      new GenericDfTransformerDef {
        override val name = "incrementalHistorize"

        override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {
          incrementalHistorizeDataFrame(existingDf, pks, capturedTs, df)
        }
      }
    } else {
      new GenericDfTransformerDef {
        override val name = "fullHistorize"

        override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {
          fullHistorizeDataFrame(existingDf, pks, capturedTs, df)
        }
      }
    }
    transformerDefs :+ historizeTransformer
  }

  override def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    applyTransformers(getTransformers, inputSubFeed, outputSubFeed)
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    applyTransformers(getTransformers, partitionValues)
  }

  protected def fullHistorizeDataFrame(existingDf: Option[GenericDataFrame], pks: Seq[String], refTimestamp: Timestamp, newDf: GenericDataFrame)(implicit context: ActionPipelineContext): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(subFeedType)
    import functions._

    // parse filter clause
    val filterClauseExpr = Try(filterClause.map(expr)) match {
      case Success(result) => result
      case Failure(e) => throw new ConfigurationException(s"($id) Error parsing filterClause parameter as expression: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

    val newFeedDf = newDf.dropDuplicates(pks)

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      if (context.isExecPhase) ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get.where(filterClauseExpr.getOrElse(lit(true))), Environment.capturedColumnName)
      // apply schema evolution
      val (modifiedExistingDf, modifiedNewFeedDf) = SchemaEvolution.process(existingDf.get, newFeedDf,
        ignoreOldDeletedColumns = ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns = ignoreOldDeletedNestedColumns,
        colsToIgnore = Seq(Environment.capturedColumnName, Environment.delimitedColumnName)
      )
      // filter existing data to be excluded from historize operation
      val (filteredExistingDf, filteredExistingRemainingDf) =
        filterClauseExpr match {
          case Some(expr) => (modifiedExistingDf.where(expr), Some(modifiedExistingDf.where(not(expr))))
          case None => (modifiedExistingDf, None)
        }
      // historize
      val historizedDf = Historization.fullHistorize(filteredExistingDf, modifiedNewFeedDf, pks, refTimestamp, timeAxisUnitOpt, historizeWhitelist, historizeBlacklist)
      // union with filter remaining df and return
      if (filteredExistingRemainingDf.isDefined) historizedDf.unionByName(filteredExistingRemainingDf.get)
      else historizedDf
    } else Historization.getInitialHistory(newFeedDf, refTimestamp)
  }

  protected def incrementalHistorizeDataFrame(existingDf: Option[GenericDataFrame], pks: Seq[String], refTimestamp: Timestamp, newDf: GenericDataFrame)(implicit context: ActionPipelineContext): GenericDataFrame = {

    val newFeedDf = newDf.dropDuplicates(pks)

    // if context is init check if column needs to be added -> save in needsHashColumn
    if (!context.isExecPhase) existingDfNeedsHashColumn = existingDf match {
      case Some(df) => Some(df.columns.contains(Historization.historizeHashColName))
      case _ => Some(false)
    }

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      if (context.isExecPhase) ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get, Environment.capturedColumnName)
      // historize

      val addExistingDfHashColumn = existingDfNeedsHashColumn.getOrElse(throw new IllegalStateException("HistorizeAction not correctly initialized"))
      // note that schema evolution is done by output DataObject
      Historization.incrementalHistorize(existingDf.get, newDf, pks, refTimestamp, timeAxisUnitOpt, historizeWhitelist, historizeBlacklist, addExistingDfHashColumn)
    } else Historization.getInitialHistoryWithHashCol(newFeedDf, refTimestamp, historizeWhitelist, historizeBlacklist)
  }

  private var existingDfNeedsHashColumn: Option[Boolean] = None

  protected def incrementalCDCHistorizeDataFrame(existingDf: Option[GenericDataFrame], pks: Seq[String], mergeModeDeletedRecordsConditionExpr: GenericColumn, refTimestamp: Timestamp, newDf: GenericDataFrame)(implicit context: ActionPipelineContext): GenericDataFrame = {

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      if (context.isExecPhase) ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get, Environment.capturedColumnName)
      // historize
      // note that schema evolution is done by output DataObject
      Historization.incrementalCDCHistorize(newDf, mergeModeDeletedRecordsConditionExpr, refTimestamp, timeAxisUnitOpt)
    } else Historization.getInitialHistoryWithDummyCol(newDf, refTimestamp)
  }

  private def getReferenceTimestamp(implicit context: ActionPipelineContext): LocalDateTime = {
    context.referenceTimestamp.getOrElse(LocalDateTime.now)
  }

  override private[smartdatalake] def reset(implicit context: ActionPipelineContext): Unit = {
    super.reset
    existingDfNeedsHashColumn = None
  }

  override def factory: FromConfigFactory[Action] = HistorizeAction
}

object HistorizeAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): HistorizeAction = {
    extract[HistorizeAction](config)
  }
}