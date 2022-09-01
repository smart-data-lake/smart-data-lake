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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions._
import io.smartdatalake.metrics.{SparkStageMetricsListener, SparkStreamingQueryListener}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.ScalaUtil
import io.smartdatalake.util.spark.DummyStreamProvider
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformerDef, PartitionValueTransformer}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.util.concurrent.Semaphore
import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Implementation of logic needed for Spark Actions.
 * This is a generic implementation that supports many input and output SubFeeds.
 */
private[smartdatalake] abstract class DataFrameActionImpl extends ActionSubFeedsImpl[DataFrameSubFeed] {

  override def inputs: Seq[DataObject with CanCreateDataFrame]
  override def outputs: Seq[DataObject with CanWriteDataFrame]
  override def recursiveInputs: Seq[DataObject with CanCreateDataFrame] = Seq()

  /**
   * Stop propagating input DataFrame through action and instead get a new DataFrame from DataObject.
   * This can help to save memory and performance if the input DataFrame includes many transformations from previous Actions.
   * The new DataFrame will be initialized according to the SubFeed's partitionValues.
   */
  def breakDataFrameLineage: Boolean

  /**
   * Stop propagating output DataFrame through action. The next action should get a fresh DataFrame from the DataObject according to the partition values.
   * This is needed for Actions which create a specific DataFrame to implement the logic needed, e.g. Deduplicate- and HistorizeAction
   */
  def breakDataFrameOutputLineage: Boolean = false

  /**
   * Force persisting input DataFrame's on Disk.
   * This improves performance if dataFrame is used multiple times in the transformation and can serve as a recovery point
   * in case a task get's lost.
   * Note that DataFrames are persisted automatically by the previous Action if later Actions need the same data. To avoid this
   * behaviour set breakDataFrameLineage=false.
   */
  def persist: Boolean

  override def isAsynchronous: Boolean = executionMode.exists(_.isAsynchronous)

  override def isAsynchronousProcessStarted: Boolean = isAsynchronous && streamingQuery.nonEmpty

  /**
   * Override and parametrize saveMode in output DataObject configurations when writing to DataObjects.
   */
  def saveModeOptions: Option[SaveModeOptions] = None

  /**
   * Common DataFrameSubFeed type needed by transformers
   * If None there are no transformers or all of them can work with GenericDataFrames.
   */
  def transformerSubFeedType: Option[Type]

  // Determine DataFrameSubFeed type of this DataFrameAction
  // This has to be done at runtime as it depends on the types of input & output DataObjects.
  // It is a "lazy val" so it is executed after inputs & outputs are defined by subclass initialization.
  private[smartdatalake] lazy val subFeedType: Type = {
    val commonInputTypes = inputs.map(_.getSubFeedSupportedTypes).toSet.reduce(_ intersect _)
    val commonOutputTypes = outputs.map(_.writeSubFeedSupportedTypes).toSet.reduce(_ intersect _)
    // search common types in input & output
    val commonTypes = commonInputTypes.intersect(commonOutputTypes)
    if (commonTypes.isEmpty) throw ConfigurationException(s"($id) No common subfeed type found between inputs & outputs")
    val commonType = if (transformerSubFeedType.isDefined && !(transformerSubFeedType.get =:= typeOf[DataFrameSubFeed])) {
      // if transformerSubFeedType is defined and not generic, we have to take that one and assert it is in common types list
      assert(transformerSubFeedType.get =:= typeOf[DataFrameSubFeed] || commonTypes.contains(transformerSubFeedType.get), s"($id) subfeed type of transformers (${transformerSubFeedType.get}) doesnt exist in common subfeed types of inputs & outputs (${commonTypes.mkString(", ")})")
      transformerSubFeedType.get
    } else {
      // if transformerSubFeedType is None or generic, take the one that occurs first in the inputs list
      commonTypes.map(t => (t, inputs.map(_.getSubFeedSupportedTypes.indexOf(t)).max)).minBy(_._2)._1
    }
    logger.info(s"($id) selected subFeedType ${commonType.typeSymbol.name}")
    commonType
  }
  private[smartdatalake] implicit lazy val subFeedHelper: DataFrameSubFeedCompanion = {
    ScalaUtil.companionOf[DataFrameSubFeedCompanion](subFeedType)
  }
  private[smartdatalake] override def subFeedConverter(): SubFeedConverter[DataFrameSubFeed] = subFeedHelper

  override def getRuntimeDataImpl: RuntimeData = {
    // override runtime data implementation for SparkStreamingMode
    if (executionMode.exists(_.isInstanceOf[SparkStreamingMode])) AsynchronousRuntimeData(Environment.runtimeDataNumberOfExecutionsToKeep)
    else super.getRuntimeDataImpl
  }

  // stage metrics listener to collect metrics
  private var _stageMetricsListener: Option[SparkStageMetricsListener] = None
  private def registerStageMetricsListener(implicit context: ActionPipelineContext): Unit = {
    if (_stageMetricsListener.isEmpty) {
      _stageMetricsListener = Some(new SparkStageMetricsListener(this))
      context.sparkSession.sparkContext.addSparkListener(_stageMetricsListener.get)
    }
  }
  private def unregisterStageMetricsListener(implicit context: ActionPipelineContext): Unit = {
    if (_stageMetricsListener.isDefined) {
      context.sparkSession.sparkContext.removeSparkListener(_stageMetricsListener.get)
      _stageMetricsListener = None
    }
  }

  // remember streaming query
  // TODO: this is still spark specific!
  private var streamingQuery: Option[StreamingQuery] = None
  private[smartdatalake] def notifyStreamingQueryTerminated(implicit context: ActionPipelineContext): Unit = {
    streamingQuery = None
    unregisterStageMetricsListener
  }

  // reset streaming query
  override private[smartdatalake] def reset(implicit context: ActionPipelineContext): Unit = {
    super.reset
    streamingQuery = None
    unregisterStageMetricsListener
  }

  /**
   * Enriches SparkSubFeed with DataFrame if not existing
   *
   * @param input input data object.
   * @param subFeed input SubFeed.
   * @param phase current execution phase
   * @param isRecursive true if this input is a recursive input
   */
  def enrichSubFeedDataFrame(input: DataObject with CanCreateDataFrame, subFeed: DataFrameSubFeed, phase: ExecutionPhase, isRecursive: Boolean = false)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    assert(input.id == subFeed.dataObjectId, s"($id) DataObject.Id ${input.id} doesnt match SubFeed.DataObjectId ${subFeed.dataObjectId} ")
    assert(phase!=ExecutionPhase.Prepare, "Strangely enrichSubFeedDataFrame got called in phase prepare. It should only be called in Init and Exec.")
    executionMode match {
      case Some(m: SparkStreamingMode) if !context.simulation =>
        // this must be a SparkSubFeed
        val sparkSubFeed = subFeed.asInstanceOf[SparkSubFeed]
        implicit val sparkSession: SparkSession = context.sparkSession
        if (subFeed.dataFrame.isEmpty || phase==ExecutionPhase.Exec) { // in exec phase we always needs a fresh streaming DataFrame
          // recreate DataFrame from DataObject
          assert(input.isInstanceOf[CanCreateStreamingDataFrame], s"($id) DataObject ${input.id} doesn't implement CanCreateStreamingDataFrame. Can not create StreamingDataFrame for executionMode=SparkStreamingOnceMode")
          logger.info(s"getting streaming DataFrame for ${input.id}")
          val df = input.asInstanceOf[CanCreateStreamingDataFrame].getStreamingDataFrame(m.inputOptions, sparkSubFeed.dataFrame.map(_.schema.inner))
          sparkSubFeed.copy(dataFrame = Some(SparkDataFrame(df)), partitionValues = Seq()) // remove partition values for streaming mode
        } else if (sparkSubFeed.isStreaming.contains(false)) {
          // convert to dummy streaming DataFrame
          val emptyStreamingDataFrame = sparkSubFeed.dataFrame.map(df => DummyStreamProvider.getDummyDf(df.schema.inner))
          sparkSubFeed.copy(dataFrame = emptyStreamingDataFrame.map(SparkDataFrame), partitionValues = Seq()) // remove partition values for streaming mode
        } else sparkSubFeed
      case _ =>
        // count reuse of subFeed.dataFrame for caching/release in exec phase
        if (phase == ExecutionPhase.Init && subFeed.hasReusableDataFrame && Environment.enableAutomaticDataFrameCaching)
          context.rememberDataFrameReuse(subFeed.dataObjectId, subFeed.partitionValues, id)
        // process subfeed
        if (phase==ExecutionPhase.Exec) {
          // check if dataFrame must be created
          if (subFeed.dataFrame.isEmpty || subFeed.isDummy || subFeed.isStreaming.contains(true)) {
            // validate partition values existing for input
            input match {
              case partitionedInput: DataObject with CanHandlePartitions => validatePartitionValuesExisting(partitionedInput, subFeed)
              case _ => Unit
            }
            // check if data is existing, otherwise create empty dataframe for recursive input
            val isDataExisting = input match {
              case tableInput: TableDataObject => tableInput.isTableExisting
              case _ => true // default is that data is existing
            }
            // recreate DataFrame from DataObject if not skipped
            if (!subFeed.isSkipped && (!isRecursive || isDataExisting)) {
              logger.info(s"($id) getting DataFrame for ${input.id}" + (if (subFeed.partitionValues.nonEmpty) s" filtered by partition values ${subFeed.partitionValues.mkString(" ")}" else ""))
              input.getSubFeed(subFeed.partitionValues, subFeedType) // get SubFeed of specified type with fresh DataFrame
                .withFilter(subFeed.partitionValues, subFeed.filter)
            } else {
              // if skipped create empty DataFrame
              subFeed.withDataFrame(Some(createEmptyDataFrame(input)))
            }
          } else {
            // existing DataFrame can be used
            subFeed
          }
        } else {
          // phase != exec
          if (subFeed.dataFrame.isEmpty) {
            // create a dummy subFeed, as we are not in exec phase
            subFeed.withDataFrame(Some(createEmptyDataFrame(input)))
              .applyFilter // check that filter is working
              .asDummy()
          } else if (subFeed.isStreaming.contains(true)) {
            // convert to empty normal DataFrame
            subFeed.withDataFrame(subFeed.schema.map(x => x.getEmptyDataFrame(subFeed.dataObjectId)))
          } else subFeed
        }
    }
  }

  def createEmptyDataFrame(dataObject: DataObject with CanCreateDataFrame)(implicit context: ActionPipelineContext): GenericDataFrame = {
    implicit val session: SparkSession = context.sparkSession
    val schema = dataObject match {
      case input: SparkFileDataObject if input.getSchema.isDefined => input.getSchema
      case input: UserDefinedSchema if input.schema.isDefined => input.schema
      case input: SchemaValidation if input.schemaMin.isDefined => input.schemaMin
      case _ => None
    }
    val readSchema = schema.map(dataObject.createReadSchema)
    readSchema
      .map(s => subFeedHelper.getEmptyDataFrame(s, dataObject.id))
      .getOrElse(dataObject.getDataFrame(Seq(), subFeedType).filter(subFeedHelper.lit(false)))
  }

  override protected def preprocessInputSubFeedCustomized(subFeed: DataFrameSubFeed, ignoreFilters: Boolean, isRecursive: Boolean)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val inputMap = (inputs ++ recursiveInputs).map(i => i.id -> i).toMap
    val input = inputMap(subFeed.dataObjectId)
    // persist if requested
    var preparedSubFeed = if (persist) subFeed.persist else subFeed
    // create dummy DataFrame if read schema is different from write schema on this DataObject
    val writeSchema = preparedSubFeed.schema
    val readSchema = writeSchema.map(schema => input.createReadSchema(schema))
    val schemaChanges = writeSchema != readSchema
    require(!context.simulation || !schemaChanges, s"($id) write & read schema is not the same for ${input.id}. Need to create a dummy DataFrame, but this is not allowed in simulation!")
    preparedSubFeed = if (schemaChanges) {
      if (subFeed.isStreaming.getOrElse(false)) {
        subFeed.withDataFrame(readSchema.map(subFeedHelper.getEmptyStreamingDataFrame)).asDummy()
      } else {
        subFeed.withDataFrame(readSchema.map(s => subFeedHelper.getEmptyDataFrame(s, subFeed.dataObjectId))).asDummy()
      }
    } else preparedSubFeed
    // remove potential filter and partition values added by execution mode
    if (ignoreFilters) preparedSubFeed = preparedSubFeed.breakLineage.clearFilter().clearPartitionValues().clearSkipped()
    // break lineage if requested or if it's a streaming DataFrame or if a filter expression is set
    if (breakDataFrameLineage || preparedSubFeed.isStreaming.contains(true) || preparedSubFeed.filter.isDefined) preparedSubFeed = preparedSubFeed.breakLineage
    // enrich with fresh DataFrame if needed
    preparedSubFeed = enrichSubFeedDataFrame(input, preparedSubFeed, context.phase, isRecursive)
    // return
    preparedSubFeed
  }

  override def postprocessOutputSubFeedCustomized(subFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    assert(subFeed.dataFrame.isDefined)
    val output = outputs.find(_.id == subFeed.dataObjectId).get
    // initialize outputs
    if (context.phase == ExecutionPhase.Init) {
      output.init(subFeed.dataFrame.get, subFeed.partitionValues, saveModeOptions)
    }
    // apply expectation validation
    output match {
      case evDataObject: DataObject with ExpectationValidation =>
        val (dfExpectations, observation) = evDataObject.setupConstraintsAndJobExpectations(subFeed.dataFrame.get)
        subFeed
          .withDataFrame(Some(dfExpectations))
          .withObservation(Some(observation))
      case _ => subFeed
    }
  }

  override protected def writeSubFeed(subFeed: DataFrameSubFeed, isRecursive: Boolean)(implicit context: ActionPipelineContext): WriteSubFeedResult[DataFrameSubFeed] = {
    // write subfeed to output
    setSparkJobMetadata(Some(s"writing to ${subFeed.dataObjectId}"))
    val output = outputs.find(_.id == subFeed.dataObjectId).getOrElse(throw new IllegalStateException(s"($id) output for subFeed ${subFeed.dataObjectId} not found"))
    val noData = writeSubFeed(subFeed, output, isRecursive)
    setSparkJobMetadata(None)
    val outputSubFeed = if (breakDataFrameOutputLineage) subFeed.breakLineage else subFeed
    WriteSubFeedResult(outputSubFeed, noData)
    // get expectations metrics and check violations
    output match {
      case evDataObject: DataObject with ExpectationValidation =>
        val scopeJobExpectationMetrics = subFeed.observation.map(_.waitFor())
        val metrics = evDataObject.validateExpectations(subFeed.dataFrame.get, subFeed.partitionValues, scopeJobExpectationMetrics.getOrElse(Map()))
        WriteSubFeedResult(outputSubFeed, noData, Some(metrics))
      case _ =>
        WriteSubFeedResult(outputSubFeed, noData)
    }
  }

  /**
   * writes subfeed to output respecting given execution mode
   * @return true if no data was transferred, otherwise false. None if unknown.
   */
  def writeSubFeed(subFeed: DataFrameSubFeed, output: DataObject with CanWriteDataFrame, isRecursiveInput: Boolean = false)(implicit context: ActionPipelineContext): Option[Boolean] = {
    assert(!subFeed.isDummy, s"($id) Can not write dummy DataFrame to ${output.id}")
    // write
    executionMode match {
      case Some(m: SparkStreamingMode) if m.isAsynchronous && context.appConfig.streaming =>
        // Use spark streaming mode asynchronously - first microbatch is executed synchronously then it runs on asynchronously.
        assert(subFeed.isStreaming.getOrElse(false), s"($id) ExecutionMode ${m.getClass} needs streaming DataFrame in SubFeed")
        // check if streaming query already started. This is needed if dag is restarted for pseudo-streaming.
        if (streamingQuery.isEmpty) {
          // initialize listener to release lock after first progress
          // semaphore with size=1 used as Lock.
          val firstProgressWaitLock = new Semaphore(1)
          // add metrics listener for this action if not yet done
          val queryName = getStreamingQueryName(output.id)
          new SparkStreamingQueryListener(this, output.id, queryName, Some(firstProgressWaitLock)) // self-registering, listener will release firstProgressWaitLock after first progress event.
          // start streaming query
          val streamingQueryLocalVal = output.writeStreamingDataFrame(subFeed.dataFrame.get, m.trigger, m.outputOptions, m.checkpointLocation, queryName, m.outputMode, saveModeOptions)
          // wait for first progress
          firstProgressWaitLock.acquire() // empty semaphore
          firstProgressWaitLock.acquire() // wait for SparkStreamingQueryListener releasing a sempahore permit
          streamingQueryLocalVal.exception.foreach(throw _) // throw exception if failed
          val noData = streamingQueryLocalVal.lastProgress.numInputRows == 0
          if (noData) logger.info(s"($id) no data to process for ${output.id} in first micro-batch streaming mode")
          streamingQuery = Some(streamingQueryLocalVal) // remember streaming query
          // return
          Some(noData)
        } else {
          logger.debug(s"($id) streaming query already started")
          None // unknown
        }
      case Some(m: SparkStreamingMode) =>
        // Use spark streaming mode synchronously (Trigger.once & awaitTermination)
        assert(subFeed.isStreaming.getOrElse(false), s"($id) ExecutionMode ${m.getClass} needs streaming DataFrame in SubFeed")
        // add metrics listener for this action if not yet done
        val queryName = getStreamingQueryName(output.id)
        new SparkStreamingQueryListener(this, output.id, queryName)
        // start streaming query - use Trigger.Once for synchronous one-time execution
        val streamingQuery = output.writeStreamingDataFrame(subFeed.dataFrame.get, Trigger.Once, m.outputOptions, m.checkpointLocation, queryName, m.outputMode, saveModeOptions)
        // wait for termination
        streamingQuery.awaitTermination
        val noData = streamingQuery.lastProgress.numInputRows == 0
        if (noData) logger.info(s"($id) no data to process for ${output.id} in streaming mode")
        // return
        Some(noData)
      case None | Some(_: DataObjectStateIncrementalMode) | Some(_: PartitionDiffMode) | Some(_: DataFrameIncrementalMode) | Some(_: FailIfNoPartitionValuesMode) | Some(_: CustomPartitionMode) | Some(_: CustomMode) | Some(_: ProcessAllMode) | Some(_: FileIncrementalMoveMode) =>
        // Auto persist if dataFrame is reused later
        val preparedSubFeed = if (context.dataFrameReuseStatistics.contains((output.id, subFeed.partitionValues))) {
          val partitionValuesStr = if (subFeed.partitionValues.nonEmpty) s" and partitionValues ${subFeed.partitionValues.mkString(", ")}" else ""
          logger.info(s"($id) Caching dataframe for ${output.id}$partitionValuesStr")
          subFeed.persist
        } else subFeed
        // Write in batch mode
        assert(!preparedSubFeed.isStreaming.getOrElse(false), s"($id) Input from ${preparedSubFeed.dataObjectId} is a streaming DataFrame, but executionMode!=${SparkStreamingMode.getClass.getSimpleName}")
        assert(!preparedSubFeed.isDummy, s"($id) Input from ${preparedSubFeed.dataObjectId} is a dummy. Cannot write dummy DataFrame.")
        assert(!preparedSubFeed.isSkipped, s"($id) Input from ${preparedSubFeed.dataObjectId} is a skipped. Cannot write skipped DataFrame.")
        output.writeDataFrame(preparedSubFeed.dataFrame.get, preparedSubFeed.partitionValues, isRecursiveInput, saveModeOptions)
        // return
        None // unknown
      case x => throw new IllegalStateException( s"($id) ExecutionMode $x is not supported")
    }
  }
  private def getStreamingQueryName(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext) = {
    s"${context.appConfig.appName} $id writing ${dataObjectId}"
  }

  /**
   * Apply many-to-many transformers to SubFeeds.
   * Keep outputs of previous transformers as input for next transformer, but in the end only return outputs of last transformer.
   * @return outputDataFrameMap and outputPartitionValues of last transformer
   */
  protected def applyTransformers(transformers: Seq[GenericDfsTransformerDef], inputPartitionValues: Seq[PartitionValues], inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed])(implicit context: ActionPipelineContext): Seq[DataFrameSubFeed] = {
    val inputDfsMap = inputSubFeeds.map(subFeed => (subFeed.dataObjectId.id, subFeed.dataFrame.get)).toMap
    val (outputDfsMap, _) = transformers.foldLeft((inputDfsMap,inputPartitionValues)){
      case ((inputDfsMap, inputPartitionValues), transformer) =>
        val (outputDfsMap, outputPartitionValues) = transformer.applyTransformation(id, inputPartitionValues, inputDfsMap, getExecutionModeResultOptions)
        (inputDfsMap ++ outputDfsMap, outputPartitionValues)
    }
    // create output subfeeds from transformed dataframes
    outputSubFeeds.map { subFeed=>
        val df = outputDfsMap.getOrElse(subFeed.dataObjectId.id, throw ConfigurationException(s"($id) No result found for output ${subFeed.dataObjectId}. Available tesults are ${outputDfsMap.keys.mkString(", ")}."))
        subFeed.withDataFrame(Some(df))
    }
  }

  /**
   * apply transformer to partition values
   */
  protected def applyTransformers(transformers: Seq[PartitionValueTransformer], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    transformers.foldLeft(PartitionValues.oneToOneMapping(partitionValues)){
      case (partitionValuesMap, transformer) => transformer.applyTransformation(id, partitionValuesMap, getExecutionModeResultOptions)
    }
  }

  /**
   * The transformed DataFrame is validated to have the output's partition columns included, partition columns are moved to the end and SubFeeds partition values updated.
   *
   * @param output output DataObject
   * @param subFeed SubFeed with transformed DataFrame
   * @return validated and updated SubFeed
   */
   def validateAndUpdateSubFeedCustomized(output: DataObject, subFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    output match {
      case partitionedDO: CanHandlePartitions =>
        // validate output partition columns exist in DataFrame
        subFeed.dataFrame.foreach(df => validateDataFrameContainsCols(df, partitionedDO.partitions, s"for ${output.id}"))
        // adapt subfeed
        subFeed
          .updatePartitionValues(partitionedDO.partitions, breakLineageOnChange = false)
          .movePartitionColumnsLast(partitionedDO.partitions)
      case _ => subFeed.clearPartitionValues(breakLineageOnChange = false)
    }
  }

  /**
   * Validate that DataFrame contains a given list of columns, throwing an exception otherwise.
   *
   * @param df DataFrame to validate
   * @param columns Columns that must exist in DataFrame
   * @param debugName name to mention in exception
   */
  def validateDataFrameContainsCols(df: GenericDataFrame, columns: Seq[String], debugName: String): Unit = {
    val missingColumns = columns.diff(df.schema.columns)
    assert(missingColumns.isEmpty, s"DataFrame $debugName doesn't include columns ${missingColumns.mkString(", ")}")
  }

  override def preExec(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    registerStageMetricsListener
    super.preExec(subFeeds)
  }

  override def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    super.postExec(inputSubFeeds, outputSubFeeds)
    // auto-unpersist DataFrames no longer needed
    inputSubFeeds
      .collect { case subFeed: DataFrameSubFeed => subFeed }
      .foreach { subFeed =>
        if (context.forgetDataFrameReuse(subFeed.dataObjectId, subFeed.partitionValues, id).contains(0)) {
          logger.info(s"($id) Removing cached DataFrame for ${subFeed.dataObjectId} and partitionValues=${subFeed.partitionValues.mkString(", ")}")
          subFeed.unpersist
        }
    }
    if (!isAsynchronous) unregisterStageMetricsListener
  }

  override def postExecFailed(implicit context: ActionPipelineContext): Unit = {
    super.postExecFailed
    unregisterStageMetricsListener
  }
}
