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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions._
import io.smartdatalake.metrics.{SparkStageMetricsListener, SparkStreamingQueryListener}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DataFrameUtil
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.util.streaming.DummyStreamProvider
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.action.sparktransformer._
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, SparkSubFeed, SubFeed}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.Duration
import java.util.concurrent.Semaphore

private[smartdatalake] abstract class SparkAction extends Action {

  /**
   * Stop propagating input DataFrame through action and instead get a new DataFrame from DataObject.
   * This can help to save memory and performance if the input DataFrame includes many transformations from previous Actions.
   * The new DataFrame will be initialized according to the SubFeed's partitionValues.
   */
  def breakDataFrameLineage: Boolean

  /**
   * Force persisting input DataFrame's on Disk.
   * This improves performance if dataFrame is used multiple times in the transformation and can serve as a recovery point
   * in case a task get's lost.
   * Note that DataFrames are persisted automatically by the previous Action if later Actions need the same data. To avoid this
   * behaviour set breakDataFrameLineage=false.
   */
  def persist: Boolean

  override def isAsynchronous: Boolean = executionMode.exists(_.isAsynchronous)

  override def getRuntimeDataImpl: RuntimeData = {
    // override runtime data implementation for SparkStreamingMode
    if (executionMode.exists(_.isInstanceOf[SparkStreamingMode])) AsynchronousRuntimeData(Option(Environment.globalConfig).map(_.runtimeDataNumberOfExecutionsToKeep).getOrElse(10))
    else super.getRuntimeDataImpl
  }

  // stage metrics listener to collect metrics
  private var _stageMetricsListener: Option[SparkStageMetricsListener] = None
  private def registerStageMetricsListener(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    if (_stageMetricsListener.isEmpty) {
      _stageMetricsListener = Some(new SparkStageMetricsListener(this))
      session.sparkContext.addSparkListener(_stageMetricsListener.get)
    }
  }
  private def unregisterStageMetricsListener(implicit session: SparkSession): Unit = {
    if (_stageMetricsListener.isDefined) {
      session.sparkContext.removeSparkListener(_stageMetricsListener.get)
      _stageMetricsListener = None
    }
  }

  // remember streaming query
  private var streamingQuery: Option[StreamingQuery] = None
  private[smartdatalake] def notifyStreamingQueryTerminated(): Unit = {
    streamingQuery = None
    _stageMetricsListener = None
  }

  // reset streaming query
  override private[smartdatalake] def reset(): Unit = {
    super.reset()
    streamingQuery = None
    _stageMetricsListener = None
  }

  override def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.prepare
    executionMode.foreach(_.prepare(id))
  }

  /**
   * Enriches SparkSubFeed with DataFrame if not existing
   *
   * @param input input data object.
   * @param subFeed input SubFeed.
   */
  def enrichSubFeedDataFrame(input: DataObject with CanCreateDataFrame, subFeed: SparkSubFeed, phase: ExecutionPhase)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    assert(input.id == subFeed.dataObjectId, s"($id) DataObject.Id ${input.id} doesnt match SubFeed.DataObjectId ${subFeed.dataObjectId} ")
    assert(phase!=ExecutionPhase.Prepare, "Strangely enrichSubFeedDataFrame got called in phase prepare. It should only be called in Init and Exec.")
    executionMode match {
      case Some(m: SparkStreamingMode) if !context.simulation =>
        if (subFeed.dataFrame.isEmpty || phase==ExecutionPhase.Exec) { // in exec phase we always needs a fresh streaming DataFrame
          // recreate DataFrame from DataObject
          assert(input.isInstanceOf[CanCreateStreamingDataFrame], s"($id) DataObject ${input.id} doesn't implement CanCreateStreamingDataFrame. Can not create StreamingDataFrame for executionMode=SparkStreamingOnceMode")
          logger.info(s"getting streaming DataFrame for ${input.id}")
          val df = input.asInstanceOf[CanCreateStreamingDataFrame].getStreamingDataFrame(m.inputOptions, subFeed.dataFrame.map(_.schema))
            .colNamesLowercase // convert to lower case by default
          subFeed.copy(dataFrame = Some(df), partitionValues = Seq()) // remove partition values for streaming mode
        } else if (subFeed.isStreaming.contains(false)) {
          // convert to dummy streaming DataFrame
          val emptyStreamingDataFrame = subFeed.dataFrame.map(df => DummyStreamProvider.getDummyDf(df.schema))
          subFeed.copy(dataFrame = emptyStreamingDataFrame, partitionValues = Seq()) // remove partition values for streaming mode
        } else subFeed
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
              case partitionedInput: DataObject with CanHandlePartitions if subFeed.partitionValues.nonEmpty && (context.phase == ExecutionPhase.Exec || subFeed.isDAGStart) =>
                val completePartitionValues = subFeed.partitionValues.filter(_.keys == partitionedInput.partitions.toSet)
                val expectedPartitions = partitionedInput.filterExpectedPartitionValues(completePartitionValues)
                val missingPartitionValues = if (expectedPartitions.nonEmpty) PartitionValues.checkExpectedPartitionValues(partitionedInput.listPartitions, expectedPartitions) else Seq()
                assert(missingPartitionValues.isEmpty, s"($id) partitions ${missingPartitionValues.mkString(", ")} missing for ${input.id}")
              case _ => Unit
            }
            // recreate DataFrame from DataObject if not skipped
            val df = if (!subFeed.isSkipped) {
              logger.info(s"($id) getting DataFrame for ${input.id}" + (if (subFeed.partitionValues.nonEmpty) s" filtered by partition values ${subFeed.partitionValues.mkString(" ")}" else ""))
              input.getDataFrame(subFeed.partitionValues)
                .colNamesLowercase // convert to lower case by default
            } else {
              // if skipped create empty DataFrame
              createEmptyDataFrame(input, subFeed)
            }
            val filteredDf = filterDataFrame(df, subFeed.partitionValues, subFeed.getFilterCol)
            subFeed.copy(dataFrame = Some(filteredDf))
          } else {
            // existing DataFrame can be used
            subFeed
          }
        } else {
          // phase != exec
          if (subFeed.dataFrame.isEmpty) {
            // create a dummy dataframe if possible as we are not in exec phase
            val df = createEmptyDataFrame(input, subFeed)
            val filteredDf = filterDataFrame(df, subFeed.partitionValues, subFeed.getFilterCol)
            subFeed.copy(dataFrame = Some(filteredDf), isDummy = true)
          } else if (subFeed.isStreaming.contains(true)) {
            // convert to empty normal DataFrame
            val emptyNormalDataFrame = subFeed.dataFrame.map(df => DataFrameUtil.getEmptyDataFrame(df.schema))
            subFeed.copy(dataFrame = emptyNormalDataFrame)
          } else subFeed
        }
    }
  }

  def createEmptyDataFrame(dataObject: DataObject with CanCreateDataFrame, subFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): DataFrame = {
    val schema = dataObject match {
      case sparkFileInput: SparkFileDataObject => sparkFileInput.getSchema(false).map(dataObject.createReadSchema)
      case userDefInput: UserDefinedSchema => userDefInput.schema.map(dataObject.createReadSchema)
      case _ => None
    }
    schema.map( s => DataFrameUtil.getEmptyDataFrame(s))
      .getOrElse(dataObject.getDataFrame(subFeed.partitionValues).where("false"))
      .colNamesLowercase // convert to lower case by default
  }

  /**
   * writes subfeed to output respecting given execution mode
   * @return true if no data was transfered, otherwise false
   */
  def writeSubFeed(subFeed: SparkSubFeed, output: DataObject with CanWriteDataFrame, isRecursiveInput: Boolean = false)(implicit session: SparkSession, context: ActionPipelineContext): Boolean = {
    assert(!subFeed.isDummy, s"($id) Can not write dummy DataFrame to ${output.id}")
    // write
    executionMode match {
      case Some(m: SparkStreamingMode) if m.isAsynchronous && context.appConfig.streaming =>
        // Use spark streaming mode asynchronously - first microbatch is executed synchronously then it runs on asynchronously.
        assert(subFeed.dataFrame.get.isStreaming, s"($id) ExecutionMode ${m.getClass} needs streaming DataFrame in SubFeed")
        // check if streaming query already started. This is needed if dag is restarted for pseudo-streaming.
        if (streamingQuery.isEmpty) {
          // initialize listener to release lock after first progress
          // semaphore with size=1 used as Lock.
          val firstProgressWaitLock = new Semaphore(1)
          // add metrics listener for this action if not yet done
          val queryName = getStreamingQueryName(output.id)
          new SparkStreamingQueryListener(this, output.id, queryName, Some(firstProgressWaitLock)) // self-registering, listener will release firstProgressWaitLock after first progress event.
          // start streaming query
          val streamingQueryLocalVal = output.writeStreamingDataFrame(subFeed.dataFrame.get, m.trigger, m.outputOptions, m.checkpointLocation, queryName, m.outputMode)
          // wait for first progress
          firstProgressWaitLock.acquire() // empty semaphore
          firstProgressWaitLock.acquire() // wait for SparkStreamingQueryListener releasing a sempahore permit
          streamingQueryLocalVal.exception.foreach(throw _) // throw exception if failed
          val noData = streamingQueryLocalVal.lastProgress.numInputRows == 0
          if (noData) logger.info(s"($id) no data to process for ${output.id} in first micro-batch streaming mode")
          streamingQuery = Some(streamingQueryLocalVal) // remember streaming query
          // return
          noData
        } else {
          logger.debug("($id) streaming query already started")
          false // noData
        }
      case Some(m: SparkStreamingMode) =>
        // Use spark streaming mode synchronously (Trigger.once & awaitTermination)
        assert(subFeed.dataFrame.get.isStreaming, s"($id) ExecutionMode ${m.getClass} needs streaming DataFrame in SubFeed")
        // add metrics listener for this action if not yet done
        val queryName = getStreamingQueryName(output.id)
        new SparkStreamingQueryListener(this, output.id, queryName)
        // start streaming query - use Trigger.Once for synchronous one-time execution
        val streamingQuery = output.writeStreamingDataFrame(subFeed.dataFrame.get, Trigger.Once, m.outputOptions, m.checkpointLocation, queryName, m.outputMode)
        // wait for termination
        streamingQuery.awaitTermination
        val noData = streamingQuery.lastProgress.numInputRows == 0
        if (noData) logger.info(s"($id) no data to process for ${output.id} in streaming mode")
        // return
        noData
      case None | Some(_: PartitionDiffMode) | Some(_: SparkIncrementalMode) | Some(_: FailIfNoPartitionValuesMode) | Some(_: CustomPartitionMode) | Some(_: ProcessAllMode) =>
        // Auto persist if dataFrame is reused later
        val preparedSubFeed = if (context.dataFrameReuseStatistics.contains((output.id, subFeed.partitionValues))) {
          val partitionValuesStr = if (subFeed.partitionValues.nonEmpty) s" and partitionValues ${subFeed.partitionValues.mkString(", ")}" else ""
          logger.info(s"($id) Caching dataframe for ${output.id}$partitionValuesStr")
          subFeed.persist
        } else subFeed
        // Write in batch mode
        assert(!preparedSubFeed.dataFrame.get.isStreaming, s"($id) Input from ${preparedSubFeed.dataObjectId} is a streaming DataFrame, but executionMode!=${SparkStreamingMode.getClass.getSimpleName}")
        assert(!preparedSubFeed.isDummy, s"($id) Input from ${preparedSubFeed.dataObjectId} is a dummy. Cannot write dummy DataFrame.")
        assert(!preparedSubFeed.isSkipped, s"($id) Input from ${preparedSubFeed.dataObjectId} is a skipped. Cannot write skipped DataFrame.")
        output.writeDataFrame(preparedSubFeed.dataFrame.get, preparedSubFeed.partitionValues, isRecursiveInput)
        // return noData
        false
      case x => throw new IllegalStateException( s"($id) ExecutionMode $x is not supported")
    }
  }
  private def getStreamingQueryName(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext) = {
    s"${context.appConfig.appName} $id writing ${dataObjectId}"
  }


  /**
   * apply transformer to partition values
   */
  protected def applyTransformers(transformers: Seq[PartitionValueTransformer], partitionValues: Seq[PartitionValues])(implicit session: SparkSession, context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    transformers.foldLeft(PartitionValues.oneToOneMapping(partitionValues)){
      case (partitionValuesMap, transformer) => transformer.applyTransformation(id, partitionValuesMap)
    }
  }

  /**
   * applies all the transformations above
   */
  def getTransformers(transformation: Option[CustomDfTransformerConfig],
                      columnBlacklist: Option[Seq[String]],
                      columnWhitelist: Option[Seq[String]],
                      additionalColumns: Option[Map[String,String]],
                      standardizeDatatypes: Boolean,
                      additionalTransformers: Seq[DfTransformer],
                      filterClauseExpr: Option[Column] = None)
                     (implicit session: SparkSession, context: ActionPipelineContext): Seq[DfTransformer] = {
    Seq(
      transformation.map(t => t.impl),
      columnBlacklist.map(l => BlacklistTransformer(columnBlacklist = l)),
      columnWhitelist.map(l => WhitelistTransformer(columnWhitelist = l)),
      additionalColumns.map(cs => AdditionalColumnsTransformer(additionalColumns = cs)),
      filterClauseExpr.map(f => DfTransformerFunctionWrapper("filter", df => df.where(f))),
      if (standardizeDatatypes) Some(StandardizeDatatypesTransformer()) else None // currently we cast decimals away only but later we may add further type casts
    ).flatten ++ additionalTransformers
  }

  /**
   * The transformed DataFrame is validated to have the output's partition columns included, partition columns are moved to the end and SubFeeds partition values updated.
   *
   * @param output output DataObject
   * @param subFeed SubFeed with transformed DataFrame
   * @return validated and updated SubFeed
   */
  def validateAndUpdateSubFeed(output: DataObject, subFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
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
  def validateDataFrameContainsCols(df: DataFrame, columns: Seq[String], debugName: String): Unit = {
    val missingColumns = columns.diff(df.columns)
    assert(missingColumns.isEmpty, s"DataFrame $debugName doesn't include columns ${missingColumns.mkString(", ")}")
  }

  /**
   * Filter DataFrame with given partition values
   *
   * @param df DataFrame to filter
   * @param partitionValues partition values to use as filter condition
   * @param genericFilter filter expression to apply
   * @return filtered DataFrame
   */
  def filterDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], genericFilter: Option[Column]): DataFrame = {
    // apply partition filter
    val partitionValuesColumn = partitionValues.flatMap(_.keys).distinct
    val dfPartitionFiltered = if (partitionValues.isEmpty) df
    else if (partitionValuesColumn.size == 1) {
      // filter with Sql "isin" expression if only one column
      val filterExpr = col(partitionValuesColumn.head).isin(partitionValues.flatMap(_.elements.values):_*)
      df.where(filterExpr)
    } else {
      // filter with and/or expression if multiple partition columns
      val filterExpr = partitionValues.map(_.getSparkExpr).reduce( (a,b) => a or b)
      df.where(filterExpr)
    }
    // apply generic filter
    if (genericFilter.isDefined) dfPartitionFiltered.where(genericFilter.get)
    else dfPartitionFiltered
  }

  /**
   * Applies changes to a SubFeed from a previous action in order to be used as input for this actions transformation.
   */
  def prepareInputSubFeed(input: DataObject with CanCreateDataFrame, subFeed: SparkSubFeed, ignoreFilters: Boolean = false)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    // persist if requested
    var preparedSubFeed = if (persist) subFeed.persist else subFeed
    // create dummy DataFrame if read schema is different from write schema on this DataObject
    val writeSchema = preparedSubFeed.dataFrame.map(_.schema)
    val readSchema = preparedSubFeed.dataFrame.map(df => input.createReadSchema(df.schema))
    val schemaChanges = writeSchema != readSchema
    require(!context.simulation || !schemaChanges, s"($id) write & read schema is not the same for ${input.id}. Need to create a dummy DataFrame, but this is not allowed in simulation!")
    preparedSubFeed = if (schemaChanges) preparedSubFeed.convertToDummy(readSchema.get) else preparedSubFeed
    // remove potential filter and partition values added by execution mode
    if (ignoreFilters) preparedSubFeed = preparedSubFeed.clearFilter().clearPartitionValues()
    // break lineage if requested or if it's a streaming DataFrame or if a filter expression is set
    if (breakDataFrameLineage || preparedSubFeed.isStreaming.contains(true) || preparedSubFeed.filter.isDefined) preparedSubFeed = preparedSubFeed.breakLineage
    // return
    preparedSubFeed
  }

  override def preExec(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    registerStageMetricsListener
    super.preExec(subFeeds)
  }

  override def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.postExec(inputSubFeeds, outputSubFeeds)
    // auto-unpersist DataFrames no longer needed
    inputSubFeeds
      .collect { case subFeed: SparkSubFeed => subFeed }
      .foreach { subFeed =>
        if (context.forgetDataFrameReuse(subFeed.dataObjectId, subFeed.partitionValues, id).contains(0)) {
          logger.info(s"($id) Removing cached DataFrame for ${subFeed.dataObjectId} and partitionValues=${subFeed.partitionValues.mkString(", ")}")
          subFeed.dataFrame.foreach(_.unpersist)
        }
    }
    if (!isAsynchronous) unregisterStageMetricsListener
  }

  def logWritingStarted(subFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    val msg = s"writing to ${subFeed.dataObjectId}" + (if (subFeed.partitionValues.nonEmpty) s", partitionValues ${subFeed.partitionValues.mkString(" ")}" else "")
    logger.info(s"($id) start " + msg)
    setSparkJobMetadata(Some(msg))
  }

  def logWritingFinished(subFeed: SparkSubFeed, noData: Boolean, duration: Duration)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    setSparkJobMetadata()
    val metricsLog = if (noData) ", no data found"
    else runtimeData.getFinalMetrics(subFeed.dataObjectId).map(_.getMainInfos).map(" "+_.map( x => x._1+"="+x._2).mkString(" ")).getOrElse("")
    logger.info(s"($id) finished writing DataFrame to ${subFeed.dataObjectId.id}: jobDuration=$duration" + metricsLog)
  }

}
