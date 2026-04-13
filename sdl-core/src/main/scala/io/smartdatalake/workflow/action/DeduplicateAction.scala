/*
 * sdl-core - Build your data lake the smart way.
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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions._
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, GenericDfTransformerDef}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanMergeDataFrame, TransactionalTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import java.sql.Timestamp
import java.time.LocalDateTime
import scala.reflect.runtime.universe.Type

/**
 * This [[Action]] copies and deduplicates data between an input and output DataObject using DataFrames.
 * Deduplication keeps the last record for every key, also after it has been deleted in the source.
 * The DataFrame might be transformed using SQL or DataFrame transformations. These transformations are applied before the deduplication.
 *
 * DeduplicateAction adds an additional Column [[Environment.capturedColumnName]]. It contains the timestamp of the last occurrence of the record in the source.
 * This creates lots of updates. Especially when using saveMode.Merge it is better to set [[Environment.capturedColumnName]] to the last change of the record in the source. Use updateCapturedColumnOnlyWhenChanged = true to enable this optimization.
 *
 * DeduplicateAction needs a transactional table (e.g. [[TransactionalTableDataObject]]) as output with defined primary keys.
 * If output implements [[CanMergeDataFrame]], saveMode.Merge can be enabled by setting mergeModeEnable = true. This allows for much better performance.
 *
 * DeduplicateAction's input data must be unique across the primary key, otherwise the merge statement creates errors like `DeltaUnsupportedOperationException: [DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same`.
 * This can be achieved through adding a DeduplicateTransformer to transformers. Note that this is not included by default in DeduplicateAction, as it is a performance intensive operation.
 *
 * @param inputId                             inputs DataObject
 * @param outputId                            output DataObject
 * @param transformers                        optional list of transformations to apply before deduplication. See [[sparktransformer]] for a list of included Transformers.
 *                                            The transformations are applied according to the lists ordering.
 * @param ignoreOldDeletedColumns             if true, remove no longer existing columns in Schema Evolution
 * @param ignoreOldDeletedNestedColumns       if true, remove no longer existing columns from nested data types in Schema Evolution.
 *                                            Keeping deleted columns in complex data types has performance impact as all new data
 *                                            in the future has to be converted by a complex function.
 * @param updateCapturedColumnOnlyWhenChanged Set to true to enable update Column [[Environment.capturedColumnName]] only if Record has changed in the source, instead of updating it with every execution (default=false).
 *                                            This results in much less records updated with saveMode.Merge.
 * @param mergeModeEnable                     Set to true to use saveMode.Merge for much better performance. Output DataObject must implement [[CanMergeDataFrame]] if enabled (default = false).
 * @param mergeModeAdditionalJoinPredicate    To optimize performance it might be interesting to limit the records read from the existing table data, e.g. it might be sufficient to use only the last 7 days.
 *                                            Specify a condition to select existing data to be used in transformation as Spark SQL expression.
 *                                            Use table alias 'existing' to reference columns of the existing table data.
 */
case class DeduplicateAction(override val id: ActionId,
                             inputId: DataObjectId,
                             outputId: DataObjectId,
                             transformers: Seq[GenericDfTransformer] = Seq(),
                             ignoreOldDeletedColumns: Boolean = false,
                             ignoreOldDeletedNestedColumns: Boolean = true,
                             updateCapturedColumnOnlyWhenChanged: Boolean = false,
                             @Deprecated @deprecated("mergeModeEnable will be always true in future - make sure to use a DataObject with Implementation of CanMergeDataFrame.", "2.0.8")
                             mergeModeEnable: Boolean = false,
                             mergeModeAdditionalJoinPredicate: Option[String] = None,
                             override val breakDataFrameLineage: Boolean = false,
                             override val persist: Boolean = false,
                             override val executionMode: Option[ExecutionMode] = None,
                             override val executionCondition: Option[Condition] = None,
                             override val metricsFailCondition: Option[String] = None,
                             override val metadata: Option[ActionMetadata] = None,
                             override val engineConnectionId: Option[ConnectionId] = None
)(implicit val instanceRegistry: InstanceRegistry) extends DataFrameOneToOneActionImpl {

  override val input: DataObject with CanCreateDataFrame = getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  override val output: TransactionalTableDataObject = getOutputDataObject[TransactionalTableDataObject](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[TransactionalTableDataObject] = Seq(output)

  if (!mergeModeEnable) logger.warn(s"($id) mergeModeEnable = false will not be supported in future anymore, please change to a DataObject with Implementation of CanMergeDataFrame otherwise your code will fail at some point.")
  if (!mergeModeEnable && mergeModeAdditionalJoinPredicate.nonEmpty) logger.warn(s"($id) Configuration of mergeModeAdditionalJoinPredicate has no effect if mergeModeEnable = false")

  override def saveModeOptions: Option[SaveModeOptions] = if (mergeModeEnable) {
    // force SDLSaveMode.Merge if mergeModeEnable = true
    assert(output.isInstanceOf[CanMergeDataFrame], s"($id) output DataObject must support SaveMode.Merge (implement CanMergeDataFrame) if mergeModeEnable = true")
    // customize update condition
    val updateCondition = if (updateCapturedColumnOnlyWhenChanged) {
      val (colsToUpdate, colsNew) = checkRecordChangedColumns.partition(outputCols.contains)
      val colsToUpdateConditions = colsToUpdate.map(c => s"not(existing.$c <=> new.$c)") // comparing equality including null is complicated with standard sql
      val colsNewCondition = colsNew.map(c => s"new.$c is not null") // null is the default value of the new column, we need to update if the value in new data is not null
      Some((colsToUpdateConditions ++ colsNewCondition).mkString(" or "))
    }
    else None
    Some(SaveModeMergeOptions(updateCondition = updateCondition, additionalMergePredicate = mergeModeAdditionalJoinPredicate))
  } else {
    // force SDLSaveMode.Overwrite otherwise
    Some(SaveModeGenericOptions(SDLSaveMode.Overwrite))
  }
  // DataFrame columns are needed in order to generate update condition for SaveModeMergeOptions. Unfortunately they are not available here. A variable is needed which gets updated in transform(...).
  private var checkRecordChangedColumns: Seq[String] = Seq()
  // Output columns are needed in order to generate update condition for SaveModeMergeOptions. Unfortunately they are not available here. A variable is needed which gets updated in transform(...).
  private var outputCols: Set[String] = Set()

  // If mergeModeEnabled=false, output is used as recursive input in DeduplicateAction to get existing data. This override is needed to force tick-tock write operation.
  override val recursiveInputs: Seq[TransactionalTableDataObject] = if (!mergeModeEnable) Seq(output) else Seq()

  private[smartdatalake] override val handleRecursiveInputsAsSubFeeds: Boolean = false

  // DataFrame created by DeduplicateAction should not be passed on to the next Action, but must be recreated from the DataObject.
  override val breakDataFrameOutputLineage: Boolean = true

  // check preconditions
  require(output.table.primaryKey.isDefined, s"($id) Primary key must be defined for output DataObject")
  require(mergeModeEnable || !updateCapturedColumnOnlyWhenChanged, s"($id) updateCapturedColumnOnlyWhenChanged = true is not implemented for mergeModeEnable = false")

  override val transformerSubFeedSupportedTypes: Seq[Type] = transformers.map(_.getSubFeedSupportedType) // deduplicate transformer can be ignored as it is generic

  validateConfig()

  override def validateConfig(): Unit = {
    super.validateConfig()
    // validate parsing mergeModeAdditionalJoinPredicate
    try {
      val functions = DataFrameSubFeed.getFunctions(subFeedType)
      mergeModeAdditionalJoinPredicate.map(functions.expr)
    } catch {
      case ex: Exception => throw new ConfigurationException(s"($id) Cannot parse mergeModeAdditionalJoinPredicate as Spark expression: ${ex.getClass.getSimpleName} ${ex.getMessage}", Some(s"{$id.id}.mergeModeAdditionalJoinPredicate"), ex)
    }
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    transformers.foreach(_.prepare(id))
  }

  private[smartdatalake] override def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfTransformerDef] = {
    val timestamp = Timestamp.valueOf(context.referenceTimestamp.getOrElse(LocalDateTime.now))

    val deduplicateTransformer = if (mergeModeEnable) {
      // deduplication & schema evolution is done by merge stmt, only captured column needs to be added before
      new GenericDfTransformerDef {
        override def name: String = "enhanceForMergeDeduplicate"

        override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {
          DeduplicateAction.enhanceDataFrame(df, timestamp)
        }
      }
    } else {
      // get existing data
      // Note that DeduplicateAction needs to read/write all existing data for tick-tock operation, even if only specific partitions have changed
      val existingDf = if (output.isTableExisting) Some(output.getDataFrame(Seq(), subFeedType))
      else None
      val pks = output.table.primaryKey.get // existance is validated earlier
      new GenericDfTransformerDef {
        override def name: String = "deduplicate"

        override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {
          DeduplicateAction.deduplicateDataFrame(existingDf, pks, timestamp, ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns)(df)
        }
      }
    }

    transformers :+ deduplicateTransformer
  }

  override def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    checkRecordChangedColumns = inputSubFeed.dataFrame
      .map(_.columns.map(c => if (!Environment.caseSensitive) c.toLowerCase else c))
      .getOrElse(Seq())
    if (output.isTableExisting && mergeModeEnable && updateCapturedColumnOnlyWhenChanged) {
      outputCols = output.getDataFrame(Seq(), outputSubFeed.tpe).columns.map(c => if (!Environment.caseSensitive) c.toLowerCase else c).toSet
    }
    applyTransformers(getTransformers, inputSubFeed, outputSubFeed)
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    applyTransformers(getTransformers, partitionValues)
  }

  override def factory: FromConfigFactory[Action] = DeduplicateAction
}

object DeduplicateAction extends FromConfigFactory[Action] {

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeduplicateAction = {
    extract[DeduplicateAction](config)
  }

  /**
   * deduplicates a SubFeed.
   */
  def deduplicateDataFrame(existingDf: Option[GenericDataFrame], pks: Seq[String], refTimestamp: Timestamp, ignoreOldDeletedColumns: Boolean, ignoreOldDeletedNestedColumns: Boolean)(df: GenericDataFrame): GenericDataFrame = {
    assert(!df.columns.contains(rnkColName), s"Column $rnkColName not allowed in DataFrame for DeduplicateAction")

    // enhance
    val enhancedDf = enhanceDataFrame(df, refTimestamp)

    // deduplicate
    if (existingDf.isDefined) {
      // apply schema evolution
      val (baseDf, newDf) = SchemaEvolution.process(existingDf.get, enhancedDf,
        ignoreOldDeletedColumns = ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns = ignoreOldDeletedNestedColumns
      )
      deduplicate(baseDf, newDf, pks)
    } else enhancedDf
  }

  /**
   * deduplicate -> keep latest record per key
   *
   * @param baseDf existing data
   * @param newDf  new data
   * @return deduplicated data
   */
  def deduplicate(baseDf: GenericDataFrame, newDf: GenericDataFrame, keyColumns: Seq[String]): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(baseDf.subFeedType)
    baseDf.unionByName(newDf)
      .withColumn(rnkColName, functions.window(() => functions.row_number, partitionBy = keyColumns.map(functions.col), orderBy = functions.col(Environment.capturedColumnName).desc))
      .where(functions.col(rnkColName) === functions.lit(1))
      .drop(rnkColName)
  }

  /**
   * enhance DataFrame with captured column
   */
  def enhanceDataFrame(df: GenericDataFrame, refTimestamp: Timestamp): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(df.subFeedType)
    df.withColumn(Environment.capturedColumnName, functions.lit(refTimestamp))
  }

  private val rnkColName = "__rnk"
}