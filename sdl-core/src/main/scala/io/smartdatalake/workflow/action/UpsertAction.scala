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
 * Implementation of the upsert logic shared by [[UpsertAction]] and the deprecated [[DeduplicateAction]].
 *
 * Note that all members derived from a constructor parameter must be `lazy`, as the constructor of this base class
 * runs before the parameters of the implementing case class are initialized. For the same reason the preconditions
 * are not checked here, but in [[checkPreconditions]] which the implementing case class calls in its body.
 */
abstract class UpsertActionImpl extends DataFrameOneToOneActionImpl {

  /**
   * inputs DataObject
   */
  def inputId: DataObjectId

  /**
   * output DataObject
   */
  def outputId: DataObjectId

  /**
   * optional list of transformations to apply before the upsert. See [[sparktransformer]] for a
   * list of included Transformers. The transformations are applied according to the lists ordering.
   */
  def transformers: Seq[GenericDfTransformer]

  /**
   * if true, remove no longer existing columns in Schema Evolution
   */
  def ignoreOldDeletedColumns: Boolean

  /**
   * if true, remove no longer existing columns from nested data types in Schema Evolution. Keeping
   * deleted columns in complex data types has performance impact as all new data in the future has
   * to be converted by a complex function.
   */
  def ignoreOldDeletedNestedColumns: Boolean

  /**
   * Set to true to enable update Column [[Environment.capturedColumnName]] only if Record has
   * changed in the source, instead of updating it with every execution (default=false). This
   * results in much less records updated with saveMode.Merge.
   * Note that this is normally not needed if sourceTimestampColumn is set, see there.
   */
  def updateCapturedColumnOnlyWhenChanged: Boolean

  /**
   * Optional column holding the timestamp of the last change of the record in the source system. If set, it is used
   * as value for Column [[Environment.capturedColumnName]] instead of the runs reference timestamp. The column must
   * exist in the data to upsert and be of type timestamp. Records where it is null fall back to the runs
   * reference timestamp.
   * Note that the column itself is not written to the output DataObject, as its value is kept in
   * [[Environment.capturedColumnName]]. Copy it to a column with another name in a transformer if you want to keep it.
   * Records arriving late, e.g. having a source timestamp older than the record already stored, are not applied, so
   * that [[Environment.capturedColumnName]] always holds the latest version according to the source system.
   * Setting updateCapturedColumnOnlyWhenChanged = true is normally not needed together with sourceTimestampColumn:
   * [[Environment.capturedColumnName]] is moved forward only if the source system changed the record anyway, no
   * matter how it is set. What it changes is that existing records are updated only if the source timestamp
   * increased, instead of comparing all columns. This avoids rewriting records which the source system delivers
   * again with an unchanged timestamp, but a change which the source system did not timestamp is not applied.
   */
  def sourceTimestampColumn: Option[String]

  /**
   * To optimize performance it might be interesting to limit the records read from the existing
   * table data, e.g. it might be sufficient to use only the last 7 days. Specify a condition to
   * select existing data to be used in transformation as Spark SQL expression. Use table alias
   * 'existing' to reference columns of the existing table data.
   */
  def mergeModeAdditionalJoinPredicate: Option[String]

  override lazy val input: DataObject with CanCreateDataFrame = {
    implicit val registry: InstanceRegistry = instanceRegistry
    getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  }
  override lazy val output: TransactionalTableDataObject = {
    implicit val registry: InstanceRegistry = instanceRegistry
    getOutputDataObject[TransactionalTableDataObject](outputId)
  }
  override lazy val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override lazy val outputs: Seq[TransactionalTableDataObject] = Seq(output)

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

  // DataFrame created by UpsertAction should not be passed on to the next Action, but must be recreated from the DataObject.

  /**
   * check preconditions.
   * This must be called from the body of the implementing case class, as its parameters are not yet initialized when
   * the constructor of this base class runs.
   */
  protected def checkPreconditions(): Unit = {
    // force initialization of the lazy input/output, so that a wrong inputId/outputId is detected when parsing the configuration
    val _ = (input, output)
    require(output.table.primaryKey.isDefined, s"($id) Primary key must be defined for output DataObject")
    // the value of the source timestamp column is kept in the captured column, so it can not be the captured column itself
    require(!sourceTimestampColumn.exists(_.equalsIgnoreCase(Environment.capturedColumnName)),
      s"($id) sourceTimestampColumn must not be ${Environment.capturedColumnName}")
  }

  override lazy val transformerSubFeedSupportedTypes: Seq[Type] =
    transformers.map(_.getSubFeedSupportedType) // deduplicate transformer can be ignored as it is generic

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

    val upsertTransformer =
      // deduplication & schema evolution is done by merge stmt, only captured column needs to be added before
      new GenericDfTransformerDef {
        override def name: String = "enhanceForUpsert"

        override def transform(
            actionId: ActionId,
            partitionValues: Seq[PartitionValues],
            df: GenericDataFrame,
            dataObjectId: DataObjectId,
            previousTransformerName: Option[String],
            executionModeResultOptions: Map[String, String]
        )(implicit context: ActionPipelineContext): GenericDataFrame = {
          sourceTimestampColumn.foreach(UpsertAction.validateSourceTimestampColumn(actionId, df, _))
          UpsertAction.enhanceDataFrame(df, timestamp, sourceTimestampColumn)
        }
      }

    transformers :+ upsertTransformer
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

/**
 * This [[Action]] keeps the **latest version of every record** in the output DataObject, identified by the primary
 * key of the output table. This is known as *Slowly Changing Dimension Type 1* (SCD1): when a record changes in the
 * source, the stored version is overwritten and no history is kept. Use [[HistorizeAction]] if you need the full
 * history of every change (SCD2).
 *
 * Records which are no longer delivered by the source are kept, so the output always contains the complete set of
 * records ever seen, each one in its most recent state. The DataFrame might be transformed using SQL or DataFrame
 * transformations. These transformations are applied before the upsert.
 *
 * UpsertAction adds an additional Column [[Environment.capturedColumnName]]. It contains the
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
 * UpsertAction needs a transactional table (e.g. [[TransactionalTableDataObject]]) as output
 * with defined primary keys. If output implements [[CanMergeDataFrame]], saveMode.
 *
 * UpsertAction's input data must be unique across the primary key, otherwise the merge
 * statement creates errors like
 * `DeltaUnsupportedOperationException: [DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same`.
 * This can be achieved through adding a DeduplicateTransformer to transformers. Note that this is
 * not included by default in UpsertAction, as it is a performance intensive operation.
 *
 * Example:
 * {{{
 * actions = {
 *   upsert-airports {
 *     type = UpsertAction
 *     inputId = stg-airports
 *     outputId = int-airports
 *     updateCapturedColumnOnlyWhenChanged = true
 *     mergeModeAdditionalJoinPredicate = "existing.dl_ts_captured > current_date - interval 7 days"
 *   }
 * }
 * }}}
 *
 * @note UpsertAction was called DeduplicateAction until version 3.0.0. The old name is deprecated but still works,
 *       see [[DeduplicateAction]]. It was misleading, as this Action does not deduplicate its input data.
 */
case class UpsertAction(
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
)(implicit val instanceRegistry: InstanceRegistry) extends UpsertActionImpl {

  checkPreconditions()
  validateConfig()
}

object UpsertAction extends FromConfigFactory[Action] {

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): UpsertAction =
    extract[UpsertAction](config)

  /**
   * deduplicates a SubFeed, e.g. keeps the latest record per key.
   */
  def deduplicateDataFrame(
      existingDf: Option[GenericDataFrame],
      pks: Seq[String],
      refTimestamp: Timestamp,
      ignoreOldDeletedColumns: Boolean,
      ignoreOldDeletedNestedColumns: Boolean,
      sourceTimestampColumn: Option[String] = None
  )(df: GenericDataFrame): GenericDataFrame = {
    assert(!df.columns.contains(rnkColName), s"Column $rnkColName not allowed in DataFrame for UpsertAction")

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
      s"($actionId) sourceTimestampColumn '$colName' not found in columns to upsert (${df.columns.mkString(", ")})")
    assert(GenericSchemaUtil.columnIsTimestamp(df.schema, colName),
      s"($actionId) sourceTimestampColumn '$colName' must be of type timestamp")
  }

  private val rnkColName = "__rnk"
}
