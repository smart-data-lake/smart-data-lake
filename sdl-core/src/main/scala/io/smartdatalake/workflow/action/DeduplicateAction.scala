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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions._
import io.smartdatalake.util.evolution.SchemaEvolution
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.GenericSchemaUtil
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, GenericDfTransformerDef}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanMergeDataFrame, TransactionalTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import java.sql.Timestamp
import scala.reflect.runtime.universe.Type

/**
 * This [[Action]] copies and deduplicates data between an input and output DataObject using
 * DataFrames. Deduplication keeps the last record for every key, also after it has been deleted in
 * the source. The DataFrame might be transformed using SQL or DataFrame transformations. These
 * transformations are applied before the deduplication.
 *
 * DeduplicateAction adds an additional Column [[Environment.capturedColumnName]]. It contains the
 * timestamp of the last occurrence of the record in the source. This creates lots of updates.
 * Especially when using saveMode.Merge it is better to set [[Environment.capturedColumnName]] to
 * the last change of the record in the source. Use updateCapturedColumnOnlyWhenChanged = true to
 * enable this optimization.
 *
 * If the input contains the timestamp of the last change of the record in the source system, it can be used as
 * [[Environment.capturedColumnName]] by setting sourceTimestampColumn, so that the output reflects the time axis of
 * the source system instead of the schedule of the pipeline. updateCapturedColumnOnlyWhenChanged is normally not
 * needed then, as the source timestamp is only moved forward if the source system changed the record anyway.
 *
 * DeduplicateAction needs a transactional table (e.g. [[TransactionalTableDataObject]]) as output
 * with defined primary keys. If output implements [[CanMergeDataFrame]], saveMode.
 *
 * DeduplicateAction's input data must be unique across the primary key, otherwise the merge
 * statement creates errors like
 * `DeltaUnsupportedOperationException: [DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same`.
 * This can be achieved through adding a DeduplicateTransformer to transformers. Note that this is
 * not included by default in DeduplicateAction, as it is a performance intensive operation.
 *
 * Example:
 * {{{
 * actions = {
 *   dedup-airports {
 *     type = DeduplicateAction
 *     inputId = stg-airports
 *     outputId = int-airports
 *     updateCapturedColumnOnlyWhenChanged = true
 *     mergeModeAdditionalJoinPredicate = "existing.dl_ts_captured > current_date - interval 7 days"
 *   }
 * }
 * }}}
 *
 * @param inputId
 *   inputs DataObject
 * @param outputId
 *   output DataObject
 * @param transformers
 *   optional list of transformations to apply before deduplication. See [[sparktransformer]] for a
 *   list of included Transformers. The transformations are applied according to the lists ordering.
 * @param ignoreOldDeletedColumns
 *   if true, remove no longer existing columns in Schema Evolution
 * @param ignoreOldDeletedNestedColumns
 *   if true, remove no longer existing columns from nested data types in Schema Evolution. Keeping
 *   deleted columns in complex data types has performance impact as all new data in the future has
 *   to be converted by a complex function.
 * @param updateCapturedColumnOnlyWhenChanged
 *   Set to true to enable update Column [[Environment.capturedColumnName]] only if Record has
 *   changed in the source, instead of updating it with every execution (default=false). This
 *   results in much less records updated with saveMode.Merge.
 *   Note that this is normally not needed if sourceTimestampColumn is set, see there.
 * @param sourceTimestampColumn
 *   Optional column holding the timestamp of the last change of the record in the source system. If set, it is used
 *   as value for Column [[Environment.capturedColumnName]] instead of the runs reference timestamp. The column must
 *   exist in the data to deduplicate and be of type timestamp. Records where it is null fall back to the runs
 *   reference timestamp.
 *   Note that the column itself is not written to the output DataObject, as its value is kept in
 *   [[Environment.capturedColumnName]]. Copy it to a column with another name in a transformer if you want to keep it.
 *   Records arriving late, e.g. having a source timestamp older than the record already stored, are not applied, so
 *   that [[Environment.capturedColumnName]] always holds the latest version according to the source system.
 *   Setting updateCapturedColumnOnlyWhenChanged = true is normally not needed together with sourceTimestampColumn:
 *   [[Environment.capturedColumnName]] is moved forward only if the source system changed the record anyway, no
 *   matter how it is set. What it changes is that existing records are updated only if the source timestamp
 *   increased, instead of comparing all columns. This avoids rewriting records which the source system delivers
 *   again with an unchanged timestamp, but a change which the source system did not timestamp is not applied.
 * @param mergeModeAdditionalJoinPredicate
 *   To optimize performance it might be interesting to limit the records read from the existing
 *   table data, e.g. it might be sufficient to use only the last 7 days. Specify a condition to
 *   select existing data to be used in transformation as Spark SQL expression. Use table alias
 *   'existing' to reference columns of the existing table data.
 */
case class DeduplicateAction(
                              override val id: ActionId,
                              inputId: DataObjectId,
                              outputId: DataObjectId,
                              transformers: Seq[GenericDfTransformer] = Seq(),
                              ignoreOldDeletedColumns: Boolean = false,
                              ignoreOldDeletedNestedColumns: Boolean = true,
                              updateCapturedColumnOnlyWhenChanged: Boolean = false,
                              sourceTimestampColumn: Option[String] = None,
                              mergeModeAdditionalJoinPredicate: Option[String] = None,
                              override val cacheOutput: Boolean = false,
                              override val cacheInput: Boolean = false,
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

  override def saveModeOptions: Option[SaveModeOptions] = { // force SDLSaveMode.Merge
    assert(
      output.isInstanceOf[CanMergeDataFrame],
      s"($id) output DataObject must support SaveMode.Merge (implement CanMergeDataFrame)"
    )
    // customize update condition
    val capturedCol = Environment.capturedColumnName
    val updateCondition = if (updateCapturedColumnOnlyWhenChanged) {
      if (sourceTimestampColumn.isDefined) {
        // the source timestamp tells us when a record has changed, there is no need to compare all columns.
        // Note that a change which the source system did not timestamp is not applied, see sourceTimestampColumn.
        Some(s"new.$capturedCol > existing.$capturedCol")
      } else {
        val (colsToUpdate, colsNew) = checkRecordChangedColumns.partition(outputCols.contains)
        val colsToUpdateConditions = colsToUpdate.map(c => s"not(existing.$c <=> new.$c)") // comparing equality including null is complicated with standard sql
        val colsNewCondition =
          colsNew.map(c => s"new.$c is not null") // null is the default value of the new column, we need to update if the value in new data is not null
        Some((colsToUpdateConditions ++ colsNewCondition).mkString(" or "))
      }
    } else {
      // records arriving late must not overwrite a newer version of the record, see sourceTimestampColumn
      sourceTimestampColumn.map(_ => s"new.$capturedCol >= existing.$capturedCol")
    }
    Some(SaveModeMergeOptions(updateCondition = updateCondition, additionalMergePredicate = mergeModeAdditionalJoinPredicate))
  }
  // DataFrame columns are needed in order to generate update condition for SaveModeMergeOptions. Unfortunately they are not available here. A variable is needed which gets updated in transform(...).
  private var checkRecordChangedColumns: Seq[String] = Seq()
  // Output columns are needed in order to generate update condition for SaveModeMergeOptions. Unfortunately they are not available here. A variable is needed which gets updated in transform(...).
  private var outputCols: Set[String] = Set()

  override val handleRecursiveInputsAsSubFeeds: Boolean = false

  // DataFrame created by DeduplicateAction should not be passed on to the next Action, but must be recreated from the DataObject.

  // check preconditions
  require(output.table.primaryKey.isDefined, s"($id) Primary key must be defined for output DataObject")
  // the value of the source timestamp column is kept in the captured column, so it can not be the captured column itself
  require(!sourceTimestampColumn.exists(_.equalsIgnoreCase(Environment.capturedColumnName)),
    s"($id) sourceTimestampColumn must not be ${Environment.capturedColumnName}")

  override val transformerSubFeedSupportedTypes: Seq[Type] =
    transformers.map(_.getSubFeedSupportedType) // deduplicate transformer can be ignored as it is generic

  validateConfig()

  override def validateConfig(): Unit = {
    super.validateConfig()
    // validate parsing mergeModeAdditionalJoinPredicate
    try {
      val functions = DataFrameSubFeed.getFunctions(subFeedType)
      mergeModeAdditionalJoinPredicate.map(functions.expr)
    } catch {
      case ex: Exception => throw new ConfigurationException(
          s"($id) Cannot parse mergeModeAdditionalJoinPredicate as Spark expression: ${ex.getClass.getSimpleName} ${ex.getMessage}",
          Some(s"{$id.id}.mergeModeAdditionalJoinPredicate"),
          ex
        )
    }
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    transformers.foreach(_.prepare(id))
  }

  override def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfTransformerDef] = {
    val timestamp = Timestamp.valueOf(context.referenceTimestamp)

    val deduplicateTransformer =
      // deduplication & schema evolution is done by merge stmt, only captured column needs to be added before
      new GenericDfTransformerDef {
        override def name: String = "enhanceForMergeDeduplicate"

        override def transform(
            actionId: ActionId,
            partitionValues: Seq[PartitionValues],
            df: GenericDataFrame,
            dataObjectId: DataObjectId,
            previousTransformerName: Option[String],
            executionModeResultOptions: Map[String, String]
        )(implicit context: ActionPipelineContext): GenericDataFrame = {
          sourceTimestampColumn.foreach(DeduplicateAction.validateSourceTimestampColumn(actionId, df, _))
          DeduplicateAction.enhanceDataFrame(df, timestamp, sourceTimestampColumn)
        }
      }

    transformers :+ deduplicateTransformer
  }

  override def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    checkRecordChangedColumns = inputSubFeed.dataFrame
      .map(_.columns.map(c => if (!Environment.caseSensitive) c.toLowerCase else c))
      .getOrElse(Seq())
    if (output.isTableExisting && updateCapturedColumnOnlyWhenChanged) {
      outputCols = output.getDataFrame(Seq(), outputSubFeed.tpe).columns.map(c => if (!Environment.caseSensitive) c.toLowerCase else c).toSet
    }
    applyTransformers(getTransformers, inputSubFeed, outputSubFeed)
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): Map[PartitionValues, PartitionValues] =
    applyTransformers(getTransformers, partitionValues, executionModeResultOptions)
}

object DeduplicateAction extends FromConfigFactory[Action] {

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeduplicateAction =
    extract[DeduplicateAction](config)

  /**
   * deduplicates a SubFeed.
   */
  def deduplicateDataFrame(
      existingDf: Option[GenericDataFrame],
      pks: Seq[String],
      refTimestamp: Timestamp,
      ignoreOldDeletedColumns: Boolean,
      ignoreOldDeletedNestedColumns: Boolean,
      sourceTimestampColumn: Option[String] = None
  )(df: GenericDataFrame): GenericDataFrame = {
    assert(!df.columns.contains(rnkColName), s"Column $rnkColName not allowed in DataFrame for DeduplicateAction")

    // enhance
    val enhancedDf = enhanceDataFrame(df, refTimestamp, sourceTimestampColumn)

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
   * @param baseDf
   *   existing data
   * @param newDf
   *   new data
   * @return
   *   deduplicated data
   */
  def deduplicate(baseDf: GenericDataFrame, newDf: GenericDataFrame, keyColumns: Seq[String]): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(baseDf.subFeedType)
    baseDf.unionByName(newDf)
      .withColumn(
        rnkColName,
        functions.window(() => functions.row_number, partitionBy = keyColumns.map(functions.col), orderBy = functions.col(Environment.capturedColumnName).desc)
      )
      .where(functions.col(rnkColName) === functions.lit(1))
      .drop(rnkColName)
  }

  /**
   * enhance DataFrame with captured column.
   * Its value is taken from sourceTimestampColumn if defined, otherwise from the runs reference timestamp.
   */
  def enhanceDataFrame(df: GenericDataFrame, refTimestamp: Timestamp, sourceTimestampColumn: Option[String] = None): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    sourceTimestampColumn.map(colName =>
      // the source timestamp column itself is not written to the output, its value is kept in the captured column
      df.withColumn(Environment.capturedColumnName, coalesce(col(colName), lit(refTimestamp))).drop(colName)
    ).getOrElse(df.withColumn(Environment.capturedColumnName, lit(refTimestamp)))
  }

  private[smartdatalake] def validateSourceTimestampColumn(actionId: ActionId, df: GenericDataFrame, colName: String): Unit = {
    assert(GenericSchemaUtil.columnExists(df.schema, colName),
      s"($actionId) sourceTimestampColumn '$colName' not found in columns to deduplicate (${df.columns.mkString(", ")})")
    assert(GenericSchemaUtil.columnIsTimestamp(df.schema, colName),
      s"($actionId) sourceTimestampColumn '$colName' must be of type timestamp")
  }

  private val rnkColName = "__rnk"
}
