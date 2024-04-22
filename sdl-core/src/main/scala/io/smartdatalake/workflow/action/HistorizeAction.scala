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
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, GenericDfTransformerDef, SparkDfTransformerFunctionWrapper}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanMergeDataFrame, DataObject, TransactionalTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.reflect.runtime.universe.Type
import scala.util.{Failure, Success, Try}

/**
 * This [[Action]] historizes data between an input and output DataObject using DataFrames.
 * Historization creates a technical history of data by creating valid-from/to columns.
 * The DataFrame might be transformed using SQL or DataFrame transformations. These transformations are applied before the deduplication.
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
 * @param transformers optional list of transformations to apply before historization. See [[sparktransformer]] for a list of included Transformers.
 *                     The transformations are applied according to the lists ordering.
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
 * @param executionMode optional execution mode for this Action
 * @param executionCondition optional spark sql expression evaluated against [[SubFeedsExpressionData]]. If true Action is executed, otherwise skipped. Details see [[Condition]].
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
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
                            mergeModeEnable: Boolean = false,
                            mergeModeAdditionalJoinPredicate: Option[String] = None,
                            mergeModeCDCColumn: Option[String] = None,
                            mergeModeCDCDeletedValue: Option[String] = None,
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

  private val mergeModeAdditionalJoinPredicateExpr: Option[Column] = try {
    mergeModeAdditionalJoinPredicate.map(expr)
  } catch {
    case ex: Exception => throw new ConfigurationException(s"($id) Cannot parse mergeModeAdditionalJoinPredicate as Spark expression: ${ex.getClass.getSimpleName} ${ex.getMessage}", Some(s"{$id.id}.mergeModeAdditionalJoinPredicate"), ex)
  }
  private val mergeModeDeletedRecordsConditionExpr: Option[Column] = {
    mergeModeCDCColumn.map{ x =>
      assert(mergeModeCDCDeletedValue.isDefined, s"($id) mergeModeCDCDeletedValue must be set when mergeModeCDCColumn is defined")
      assert(historizeWhitelist.isEmpty, s"($id) historizeWhitelist cannot be set when mergeModeCDCColumn is defined")
      col(x) === lit(mergeModeCDCDeletedValue.get)
    }
  }
  if (mergeModeEnable) assert(output.isInstanceOf[CanMergeDataFrame], s"($id) output DataObject must support SaveMode.Merge (implement CanMergeDataFrame) if mergeModeEnable = true")
  if (!mergeModeEnable && mergeModeAdditionalJoinPredicateExpr.nonEmpty) logger.warn(s"($id) Configuration of mergeModeAdditionalJoinPredicate has no effect if mergeModeEnable = false")

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
      val updateCols = Seq(TechnicalTableColumn.delimited)
      val insertCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.insertNew}'")
      val insertColsToIgnore = Seq(Historization.historizeOperationColName, mergeModeCDCColumn.get)
      val insertValuesOverride = Map(Historization.historizeDummyColName -> "true")
      val sqlReferenceTimestamp = Timestamp.valueOf(getReferenceTimestamp)
      val additionalMergePredicate = Some((s"existing.${Historization.historizeDummyColName} = new.${Historization.historizeDummyColName} AND timestamp'$sqlReferenceTimestamp' between existing.${TechnicalTableColumn.captured} AND existing.${TechnicalTableColumn.delimited}" +: mergeModeAdditionalJoinPredicate.toSeq).reduce(_ + " and " + _))
      //val additionalMergePredicate = Some((s"timestamp'$sqlReferenceTimestamp' between existing.${TechnicalTableColumn.captured} AND existing.${TechnicalTableColumn.delimited}" +: mergeModeAdditionalJoinPredicate.toSeq).reduce(_ + " AND " + _))
      Some(SaveModeMergeOptions(updateCondition = updateCondition, updateColumns = updateCols, insertCondition = insertCondition, insertColumnsToIgnore = insertColsToIgnore, insertValuesOverride = insertValuesOverride, additionalMergePredicate = additionalMergePredicate))

    } else if (mergeModeEnable) {
      // customize update condition
      val updateCondition = Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.updateClose}'")
      val updateCols = if (input.getDataFrame(Seq(), subFeedType).schema.columnExists(Historization.historizeHashColName)) Seq(TechnicalTableColumn.delimited) // This
        else Seq(TechnicalTableColumn.delimited, Historization.historizeHashColName)

      val insertCondition =  Some(s"${Historization.historizeOperationColName} = '${HistorizationRecordOperations.insertNew}'")
      val insertColsToIgnore = Seq(Historization.historizeOperationColName)
      val additionalMergePredicate = Some((s"new.${TechnicalTableColumn.captured} = existing.${TechnicalTableColumn.captured}" +: mergeModeAdditionalJoinPredicate.toSeq).reduce(_ + " and " + _))
      Some(SaveModeMergeOptions(updateCondition = updateCondition, updateColumns = updateCols, insertCondition = insertCondition, insertColumnsToIgnore = insertColsToIgnore, additionalMergePredicate = additionalMergePredicate))
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
  require(historizeWhitelist.isEmpty || historizeBlacklist.isEmpty, s"(${id}) HistorizeWhitelist and historizeBlacklist mustn't be used at the same time")
  // primary key
  require(output.table.primaryKey.isDefined, s"(${id}) Primary key must be defined for output DataObject")

  private val transformerDefs: Seq[GenericDfTransformerDef] = transformer.map(t => t.impl).toSeq ++ transformers

  override val transformerSubFeedSupportedTypes: Seq[Type] = transformerDefs.map(_.getSubFeedSupportedType) // historize transformer can be ignored as it is generic

  validateConfig()

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    initSaveModeOptions
    transformerDefs.foreach(_.prepare(id))
  }

  private def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfTransformerDef] = {
    val capturedTs = getReferenceTimestamp
    val pks = output.table.primaryKey.get // existance is validated earlier

    // get existing data
    // Note that HistorizeAction with mergeModeEnabled=false needs to read/write all existing data for tick-tock operation, even if only specific partitions have changed
    val existingDf = if (output.isTableExisting) Some(output.getDataFrame(Seq(), subFeedType)) else None

    // historize
    val historizeTransformer = if (mergeModeEnable && mergeModeDeletedRecordsConditionExpr.isDefined) {
      // TODO: make generic
      val historizeFunction = incrementalCDCHistorizeDataFrame(existingDf.map(_.asInstanceOf[SparkDataFrame].inner), pks, mergeModeDeletedRecordsConditionExpr.get, capturedTs) _
      SparkDfTransformerFunctionWrapper("incrementalCDCHistorize", historizeFunction)
    } else if (mergeModeEnable) {
      // TODO: make generic
      val historizeFunction = incrementalHistorizeDataFrame(existingDf.map(_.asInstanceOf[SparkDataFrame].inner), pks, capturedTs) _
      SparkDfTransformerFunctionWrapper("incrementalHistorize", historizeFunction)
    } else {
      // TODO: make generic
      val historizeFunction = fullHistorizeDataFrame(existingDf.map(_.asInstanceOf[SparkDataFrame].inner), pks, capturedTs) _
      SparkDfTransformerFunctionWrapper("fullHistorize", historizeFunction)
    }
    transformerDefs :+ historizeTransformer
  }

  override def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    applyTransformers(getTransformers, inputSubFeed, outputSubFeed)
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    applyTransformers(getTransformers, partitionValues)
  }

  // TODO: make generic
  protected def fullHistorizeDataFrame(existingDf: Option[DataFrame], pks: Seq[String], refTimestamp: LocalDateTime)(newDf: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession

    // parse filter clause
    val filterClauseExpr = Try(filterClause.map(expr)) match {
      case Success(result) => result
      case Failure(e) => throw new ConfigurationException(s"(${id}) Error parsing filterClause parameter as expression: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

    val newFeedDf = newDf.dropDuplicates(pks)

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      if (context.isExecPhase) ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get.where(filterClauseExpr.getOrElse(lit(true))), TechnicalTableColumn.captured)
      // apply schema evolution
      val (modifiedExistingDf, modifiedNewFeedDf) = SchemaEvolution.process(existingDf.get, newFeedDf, ignoreOldDeletedColumns = ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns = ignoreOldDeletedNestedColumns
        , colsToIgnore = Seq(TechnicalTableColumn.captured, TechnicalTableColumn.delimited))
      // filter existing data to be excluded from historize operation
      val (filteredExistingDf, filteredExistingRemainingDf) =
        filterClauseExpr match {
          case Some(expr) => (modifiedExistingDf.where(expr), Some(modifiedExistingDf.where(not(expr))))
          case None => (modifiedExistingDf, None)
        }
      // historize
      val historizedDf = Historization.fullHistorize(filteredExistingDf, modifiedNewFeedDf, pks, refTimestamp, historizeWhitelist, historizeBlacklist)
      // union with filter remaining df and return
      if (filteredExistingRemainingDf.isDefined) historizedDf.union(filteredExistingRemainingDf.get)
      else historizedDf
    } else Historization.getInitialHistory(newFeedDf, refTimestamp)
  }

  // TODO: make generic
  protected def incrementalHistorizeDataFrame(existingDf: Option[DataFrame], pks: Seq[String], refTimestamp: LocalDateTime)(newDf: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession

    val newFeedDf = newDf.dropDuplicates(pks)

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      if (context.isExecPhase) ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get, TechnicalTableColumn.captured)
      // historize
      // note that schema evolution is done by output DataObject
      Historization.incrementalHistorize(existingDf.get, newDf, pks, refTimestamp, historizeWhitelist, historizeBlacklist)
    } else Historization.getInitialHistoryWithHashCol(newFeedDf, refTimestamp, historizeWhitelist, historizeBlacklist)
  }

  // TODO: make generic
  protected def incrementalCDCHistorizeDataFrame(existingDf: Option[DataFrame], pks: Seq[String], mergeModeDeletedRecordsConditionExpr: Column, refTimestamp: LocalDateTime)(newDf: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession

    // if output exists we have to do historization, otherwise we just transform the new data into historized form
    if (existingDf.isDefined) {
      if (context.isExecPhase) ActionHelper.checkDataFrameNotNewerThan(refTimestamp, existingDf.get, TechnicalTableColumn.captured)
      // historize
      // note that schema evolution is done by output DataObject
      Historization.incrementalCDCHistorize(newDf, mergeModeDeletedRecordsConditionExpr, refTimestamp)
    } else Historization.getInitialHistoryWithDummyCol(newDf, refTimestamp)
  }

  private def getReferenceTimestamp(implicit context: ActionPipelineContext): LocalDateTime = {
    context.referenceTimestamp.getOrElse(LocalDateTime.now)
  }

  override def factory: FromConfigFactory[Action] = HistorizeAction
}

object HistorizeAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): HistorizeAction = {
    extract[HistorizeAction](config)
  }
}