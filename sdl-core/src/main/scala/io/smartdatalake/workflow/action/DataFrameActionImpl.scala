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
import io.smartdatalake.metrics.{SparkStreamingMetrics, SparkStreamingQueryListener}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.ScalaUtil
import io.smartdatalake.util.spark.{DummyStreamProvider, SparkPlanNoDataWarning}
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.action.executionMode.SparkStreamingMode
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformerDef, PartitionValueTransformer}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkObservation, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{CombinedObservation, GenericDataFrame, PrefixedObservation}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.expectation.{ActionExpectation, Expectation, ExpectationScope}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Implementation of logic needed for Spark Actions.
 * This is a generic implementation that supports many input and output SubFeeds.
 */
abstract class DataFrameActionImpl extends ActionSubFeedsImpl[DataFrameSubFeed] {

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

  override def isAsynchronousProcessStarted: Boolean = isAsynchronous && sparkStreamingQuery.nonEmpty

  /**
   * Optionally override and parametrize saveMode in output DataObject configurations when writing to DataObjects.
   */
  def saveModeOptions: Option[SaveModeOptions] = None

  /**
   * List of expectation definitions to evaluate when executing this Action, see [[Expectation]] for details.
   *
   * Note: Expectations defined on DataObjects measure data quality and are evaluated against the output only.
   * Expectations defined on Actions measure quality of the transformation process and can measure and compare metrics between all input DataObjects and the main output DataObject.
   *
   * Expectations defined at Action level are executed together with the expectations of the main output DataObject.
   */
  def expectations: Seq[ActionExpectation] = Seq()
  assert(!expectations.exists(_.scope == ExpectationScope.JobPartition), s"($id) Calculating input metrics for expectations with scope JobPartition not supported")

  /**
   * Common DataFrameSubFeed type needed by transformers
   * If None there are no transformers or all of them can work with GenericDataFrames.
   */
  def transformerSubFeedType: Option[Type]

  // Determine DataFrameSubFeed type of this DataFrameAction
  // This has to be done at runtime as it depends on the types of input & output DataObjects.
  // It is a "lazy val" so it is executed after inputs & outputs are defined by subclass initialization.
  private[smartdatalake] lazy val subFeedType: Type = {
    def explodeGenericType(subFeedTypes: Seq[Type]): Seq[Type] = {
      subFeedTypes.flatMap(tpe => if (tpe =:= typeOf[DataFrameSubFeed]) DataFrameSubFeed.getKnownSubFeedTypes else Seq(tpe))
    }
    val allInputTypes = inputs.map(_.getSubFeedSupportedTypes).map(explodeGenericType)
    val commonInputTypes = allInputTypes.toSet.reduce(_ intersect _)
    val commonOutputTypes = outputs.map(_.writeSubFeedSupportedTypes).map(explodeGenericType).toSet.reduce(_ intersect _)
    // search common types in input & output
    val commonTypes = commonInputTypes.intersect(commonOutputTypes)
    if (commonTypes.isEmpty) throw ConfigurationException(s"($id) No common subfeed type found between inputs & outputs")
    val commonType = if (transformerSubFeedType.isDefined && !(transformerSubFeedType.get =:= typeOf[DataFrameSubFeed])) {
      // if transformerSubFeedType is defined and not generic, we have to take that one and assert it is in common types list
      assert(commonTypes.contains(transformerSubFeedType.get), s"($id) subfeed type of transformers (${transformerSubFeedType.get}) doesnt exist in common subfeed types of inputs & outputs (${commonTypes.mkString(", ")})")
      transformerSubFeedType.get
    } else {
      // if transformerSubFeedType is None or generic, take the first matching entry from the inputs list
      allInputTypes.flatten.find(commonTypes.contains).get
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

  // remember streaming query
  // TODO: this is still spark specific!
  private var sparkStreamingQuery: Option[StreamingQuery] = None
  private[smartdatalake] def notifySparkStreamingQueryTerminated(implicit context: ActionPipelineContext): Unit = {
    sparkStreamingQuery = None
  }

  // reset streaming query
  override private[smartdatalake] def reset(implicit context: ActionPipelineContext): Unit = {
    super.reset
    sparkStreamingQuery = None
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
        if (phase==ExecutionPhase.Exec || context.simulation) {
          // check if dataFrame must be created
          if (subFeed.dataFrame.isEmpty || subFeed.isDummy || subFeed.isStreaming.contains(true)) {
            // validate partition values existing for input
            input match {
              case partitionedInput: DataObject with CanHandlePartitions => validatePartitionValuesExisting(partitionedInput, subFeed)
              case _ => ()
            }
            // check if data is existing, otherwise create empty dataframe for recursive input
            val isDataExisting = input match {
              case tableInput: TableDataObject => tableInput.isTableExisting
              case _ => true // default is that data is existing
            }
            // recreate DataFrame from DataObject if not skipped
            if (!subFeed.isSkipped && (!isRecursive || isDataExisting)) {
              try {
                logger.info(s"($id) getting DataFrame for ${input.id}" + (if (subFeed.partitionValues.nonEmpty) s" filtered by partition values ${subFeed.partitionValues.mkString(" ")}" else ""))
                input.getSubFeed(subFeed.partitionValues, subFeedType) // get SubFeed of specified type with fresh DataFrame
                  .withFilter(subFeed.partitionValues, subFeed.filter)
              } catch {
                // if there is no data, but it's an action with multiple inputs, we need to avoid that that the action gets skipped because of the thrown NoDataToProcessWarning
                case _: NoDataToProcessWarning if inputs.size > 1 => subFeed.withDataFrame(Some(createEmptyDataFrame(input)))
              }
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
    if (ignoreFilters) preparedSubFeed = preparedSubFeed.breakLineage.clearFilter().clearPartitionValues().clearSkipped().asInstanceOf[DataFrameSubFeed]
    // break lineage if requested or if it's a streaming DataFrame or if a filter expression is set
    if (breakDataFrameLineage || preparedSubFeed.isStreaming.contains(true) || preparedSubFeed.filter.isDefined) preparedSubFeed = preparedSubFeed.breakLineage
    // enrich with fresh DataFrame if needed
    preparedSubFeed = enrichSubFeedDataFrame(input, preparedSubFeed, context.phase, isRecursive)
    // add observations on input DataFrame
    if (Environment.enableInputDataObjectCount) {
      input match {
        case evDataObject: ExpectationValidation => preparedSubFeed.dataFrame.foreach { df =>
          // collect additional aggregate expressions for Action with scope Job
          val inputJobExpectations = expectations.filter(_.scope == ExpectationScope.Job)
          val inputJobAggExpressionColumns = inputJobExpectations.flatMap(_.getInputAggExpressionColumns(id))
          val forceGenericObservation = inputJobExpectations.exists(!_.calculateAsJobDataFrameObservation)
          val mainInputJobAggExpressionColumns = inputJobAggExpressionColumns.filter(c => c.getName.exists(n => !n.contains("#") && input.id == prioritizedMainInputCandidates.head.id))
          val specificInputJobAggExpressionColumns = inputJobAggExpressionColumns.filter(c => c.getName.exists(n => n.endsWith(s"#${input.id.id}"))).map(c => c.as(c.getName.get.stripSuffix(s"#${input.id.id}")))
          // validate constraints and expectations on read if this is DataObject is not written by a DataFrame-Action, otherwise just add default expectations, e.g. count
          val validateOnRead = context.instanceRegistry.shouldValidateDataObjectOnRead(subFeed.dataObjectId)
          // setup observation
          val (dfExpectations, observations) = evDataObject.setupConstraintsAndJobExpectations(df, defaultExpectationsOnly = !validateOnRead, pushDownTolerant = true,
            additionalJobAggExpressionColumns = specificInputJobAggExpressionColumns ++ mainInputJobAggExpressionColumns, forceGenericObservation
          )
          preparedSubFeed = preparedSubFeed.withDataFrame(Some(dfExpectations)).withObservation(Some(CombinedObservation.create(observations)))
        }
        case _ => ()
      }
    }
    // return
    preparedSubFeed
  }

  override def postprocessOutputSubFeedCustomized(subFeed: DataFrameSubFeed, inputSubFeeds: Seq[DataFrameSubFeed])(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    assert(subFeed.dataFrame.isDefined)
    val output = outputs.find(_.id == subFeed.dataObjectId).get
    // initialize outputs
    if (context.phase == ExecutionPhase.Init) {
      output.init(subFeed.dataFrame.get, subFeed.partitionValues, saveModeOptions)
    }
    // apply expectation validation
    output match {
      case evDataObject: DataObject with ExpectationValidation =>
        // collect additional aggregate expressions for Action expectations with scope Job
        val additionalJobExpectations = expectations.filter(_.scope == ExpectationScope.Job)
        val additionalJobAggExpressionColumns = additionalJobExpectations.flatMap(_.getAggExpressionColumns(evDataObject.id))
        val forceGenericObservation = additionalJobExpectations.exists(!_.calculateAsJobDataFrameObservation)
        // setup output observation
        val (dfExpectations, outputObservations) = evDataObject.setupConstraintsAndJobExpectations(subFeed.dataFrame.get, additionalJobAggExpressionColumns = additionalJobAggExpressionColumns, forceGenericObservation = forceGenericObservation)
        // setup extracting Spark observations metrics on input DataFrames and custom observation metrics together with output observation
        outputObservations.collect{case x: SparkObservation => x}.foreach { outputSparkObservation =>
          inputSubFeeds.flatMap(_.observation).collect{ case x: SparkObservation => x}.map(_.getName)
          val inputSparkObservationNames = inputSubFeeds.flatMap(_.observation).collect{ case x: SparkObservation => x}.map(_.getName)
          outputSparkObservation.setOtherObservationNames(inputSparkObservationNames)
          outputSparkObservation.setOtherObservationsPrefix(id.id+"#")
        }
        // Combine non-Spark observations on input DataFrames with output observation into one combined observation, which is then assigned to corresponding property of the SubFeed.
        // Combining input observations with output observation is needed because a SubFeed can only carry one observation.
        val inputObservationsToCombine = inputSubFeeds.flatMap { subFeed =>
          subFeed.observation match {
            case Some(_: SparkObservation) => None // ignore as this is handled by output observation above
            case Some(otherObservation) => Some(PrefixedObservation(otherObservation, subFeed.dataObjectId.id+"#")) // add input DataObjectId prefix to metrics
            case None => None
          }
        }
        val combinedObservation = CombinedObservation.create(inputObservationsToCombine ++ outputObservations)
        // add updated dataframe and observation to SubFeed
        subFeed
          .withDataFrame(Some(dfExpectations))
          .withObservation(Some(combinedObservation))
      case _ => subFeed
    }
  }

  override protected def convertToOutputSubFeed(subFeed: DataFrameSubFeed): DataFrameSubFeed = {
    subFeed.dataFrame.flatMap(df =>
      saveModeOptions.map(options => subFeed.withDataFrame(Some(options.convertToTargetSchema(df))))
    ).getOrElse(subFeed)
  }

  override protected def writeSubFeed(subFeed: DataFrameSubFeed, isRecursive: Boolean)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    // write subfeed to output
    setSparkJobMetadata(Some(s"writing to ${subFeed.dataObjectId}"))
    val output = outputs.find(_.id == subFeed.dataObjectId).getOrElse(throw new IllegalStateException(s"($id) output for subFeed ${subFeed.dataObjectId} not found"))
    var outputSubFeed = writeSubFeed(subFeed, output, isRecursive)
    setSparkJobMetadata(None)
    if (breakDataFrameOutputLineage) outputSubFeed = outputSubFeed.breakLineage
    val isMainOutput = mainOutput.id == output.id
    // get expectations metrics and check violations
    outputSubFeed = output match {
      case evDataObject: DataObject with ExpectationValidation with CanCreateDataFrame =>
        // get metrics with scope Job from observations
        val scopeJobExpectationMetrics = subFeed.observation.map(_.waitForElseNoData()).getOrElse(Map())
        // get input metrics for this actions expectations with scope All (scope=Job is calculated with preprocessInputSubFeedCustomized, scope=JobPartition is not supported on input)
        // Note that scope All metrics are only calculated if this is the main output.
        val actionExpectationsInputMetrics = if (isMainOutput) calculateInputAggMetricsWithScopeAll(subFeed) else Map()
        // if this is mainOutput, enrich main input metrics
        val enrichmentFunc: Map[String,_] => Map[String,_] = if (isMainOutput) enrichMainInputMetrics else identity
        // evaluate and validate expectations
        var (metrics, exceptions) = evDataObject.validateExpectations(subFeedType, subFeed.dataFrame, evDataObject.getDataFrame(Seq(), subFeed.tpe), subFeed.partitionValues, scopeJobExpectationMetrics ++ actionExpectationsInputMetrics, if (isMainOutput) expectations else Seq(), enrichmentFunc, loggerContext = "output")
        // evaluate and validate expectations of input DataObjects to be validated on read
        val inputExpectationsToEvaluateOnRead = inputs.filter(i => context.instanceRegistry.shouldValidateDataObjectOnRead(i.id))
          .collect{case x: DataObject with ExpectationValidation => x}
        inputExpectationsToEvaluateOnRead.foreach { dataObject =>
          val metricsSuffix = "#"+dataObject.id.id
          val inputMetrics = metrics.filter(_._1.endsWith(metricsSuffix)).map{case (k,v) => (k.stripSuffix(metricsSuffix), v)}
          if (inputMetrics.nonEmpty) {
            val (updatedInputMetrics, inputExceptions) = dataObject.validateExpectations(subFeedType, None, dataObject.getDataFrame(Seq(), subFeed.tpe), partitionValues = Seq(), enrichmentFunc = identity, scopeJobAndInputMetrics = inputMetrics, loggerContext = s"input ${dataObject.id}")
            metrics = metrics ++ updatedInputMetrics.map { case (k, v) => (k + metricsSuffix, v) }
            exceptions = exceptions ++ inputExceptions
          }
        }
        // throw first validation exceptions if any, enriched with metrics...
        outputSubFeed = outputSubFeed.appendMetrics(metrics).asInstanceOf[DataFrameSubFeed]
        exceptions.foreach(ex => throw TaskFailedException(id.id, ex, Some(Seq(outputSubFeed))))
        outputSubFeed
      case _ =>
        outputSubFeed
    }
    // cleanup inconsistent Spark recordsWritten-metric
    val recordsWritten = outputSubFeed.metrics.flatMap(_.get("records_written"))
    val count = outputSubFeed.metrics.flatMap(_.get("count"))
    if (recordsWritten.contains(0) && count.nonEmpty) outputSubFeed = outputSubFeed.withMetrics(outputSubFeed.metrics.get - "records_written" - "bytes_written").asInstanceOf[DataFrameSubFeed]
    // add no_data metric
    if (count.contains(0) || (count.isEmpty && recordsWritten.contains(0))) outputSubFeed = outputSubFeed.appendMetrics(Map[String,Any]("no_data" -> true)).asInstanceOf[DataFrameSubFeed]
    // return
    outputSubFeed
  }

  def calculateInputAggMetricsWithScopeAll(subFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): Map[String, Any] = {
    // prepare input aggregation metrics columns from actions expectations
    val exprNameRegex = "([^#]+)#([^#]+)".r.anchored
    val actionExpectationsInputAggColumns = expectations.filter(_.scope == ExpectationScope.All).flatMap(_.getInputAggExpressionColumns(id))
      .map( expr => expr.getName match {
        case Some(exprNameRegex(name, dataObjectId)) => (DataObjectId(dataObjectId), expr.as(name))
        case Some(name) => (prioritizedMainInputCandidates.head.id, expr)
        case None => throw new IllegalStateException(s"($id) name of aggregate expression unknown: $expr")
      })
    // calculate metrics on input DataObject
    val inputAggColumns = actionExpectationsInputAggColumns
      .groupBy(_._1).mapValues(_.map(_._2)).toMap
    inputAggColumns.flatMap { case (dataObjectId, aggExpressions) =>
      val dataObject = inputMap(dataObjectId) match {
        case evDataObject: DataObject with ExpectationValidation with CanCreateDataFrame => evDataObject
        case _ => throw new IllegalStateException(s"($id) Cannot calculate input metric on $dataObjectId not supporting ExpectationValidation")
      }
      dataObject.calculateMetrics(dataObject.getDataFrame(Seq(),subFeed.tpe), aggExpressions, ExpectationScope.All)
        .map{ case (k,v) => (k+"#"+dataObjectId.id, v)}
    }
  }

  def enrichMainInputMetrics(metrics: Map[String, _]): Map[String, _] = {
    val mainInputIdSuffix = s"#${prioritizedMainInputCandidates.head.id.id}"
    // copy all metrics with name `<metric>#<dataObjectId>` as `<metric>#mainInput`
    metrics ++ metrics.filterKeys(_.endsWith(mainInputIdSuffix)).map{case (k,v) => (k.stripSuffix(mainInputIdSuffix)+"#mainInput" -> v)}
  }

  /**
   * writes subfeed to output respecting given execution mode
   */
  def writeSubFeed(subFeed: DataFrameSubFeed, output: DataObject with CanWriteDataFrame, isRecursiveInput: Boolean = false)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    assert(!subFeed.isDummy, s"($id) Can not write dummy DataFrame to ${output.id}")
    // write
    executionMode match {
      case Some(m: SparkStreamingMode) if m.isAsynchronous && context.appConfig.streaming =>
        // Use spark streaming mode asynchronously - first microbatch is executed synchronously then it runs on asynchronously.
        assert(subFeed.isStreaming.getOrElse(false), s"($id) ExecutionMode ${m.getClass} needs streaming DataFrame in SubFeed")
        // check if streaming query already started. This is needed if dag is restarted for pseudo-streaming.
        if (sparkStreamingQuery.isEmpty) {
          // add metrics listener for this action if not yet done
          val queryName = getStreamingQueryName(output.id)
          val queryListener = new SparkStreamingQueryListener(this, output.id, queryName) // self-registering, listener will release firstProgressWaitLock after first progress event.
          // start streaming query
          val streamingQueryLocalVal = output.writeStreamingDataFrame(subFeed.dataFrame.get, m.trigger, m.outputOptions, m.checkpointLocation, queryName, m.outputMode, saveModeOptions)
          // wait for first progress
          queryListener.waitForFirstProgress()
          streamingQueryLocalVal.exception.foreach(throw _) // throw exception if failed
          val queryProgress = streamingQueryLocalVal.lastProgress
          val streamingMetrics = SparkStreamingMetrics(queryProgress)
          if (streamingMetrics.noData) logger.info(s"($id) no data to process for ${output.id} in first micro-batch streaming mode")
          sparkStreamingQuery = Some(streamingQueryLocalVal) // remember streaming query
          val sparkMetrics = runtimeData.getMetrics(output.id).map(_.getMainInfos).getOrElse(Map())
          // return
          subFeed.withMetrics(streamingMetrics.getMainInfos ++ sparkMetrics).asInstanceOf[DataFrameSubFeed]
        } else {
          logger.debug(s"($id) streaming query already started")
          subFeed // unknown progress
        }
      case Some(m: SparkStreamingMode) =>
        // Use spark streaming mode synchronously (Trigger.once & awaitTermination)
        assert(subFeed.isStreaming.getOrElse(false), s"($id) ExecutionMode ${m.getClass} needs streaming DataFrame in SubFeed")
        // add metrics listener for this action if not yet done
        val queryName = getStreamingQueryName(output.id)
        val queryListener = new SparkStreamingQueryListener(this, output.id, queryName)
        // start streaming query - use Trigger.Once for synchronous one-time execution
        val streamingQuery = output.writeStreamingDataFrame(subFeed.dataFrame.get, Trigger.Once(), m.outputOptions, m.checkpointLocation, queryName, m.outputMode, saveModeOptions)
        // wait for termination
        streamingQuery.awaitTermination()
        // wait for first progress (otherwise metrics might not yet be ready)
        queryListener.waitForFirstProgress()
        val queryProgress = streamingQuery.lastProgress
        val streamingMetrics = SparkStreamingMetrics(queryProgress)
        if (streamingMetrics.noData) logger.info(s"($id) no data to process for ${output.id} in first micro-batch streaming mode")
        val sparkMetrics = runtimeData.getMetrics(output.id).map(_.getMainInfos).getOrElse(Map())
        // return
        subFeed.withMetrics(streamingMetrics.getMainInfos ++ sparkMetrics).asInstanceOf[DataFrameSubFeed]
      case _ =>
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
        val df = preparedSubFeed.dataFrame.get
        val metrics = try {
          output.writeDataFrame(df, preparedSubFeed.partitionValues, isRecursiveInput, saveModeOptions)
        } catch {
          // map exception from enableSparkPlanNoDataCheck
          case e: SparkPlanNoDataWarning => throw NoDataToProcessWarning(id.id, s"($id) ${e.getMessage}")
        }
        // return
        preparedSubFeed.withMetrics(metrics).asInstanceOf[DataFrameSubFeed]
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
  private[smartdatalake] def applyTransformers(transformers: Seq[GenericDfsTransformerDef], inputPartitionValues: Seq[PartitionValues], inputSubFeeds: Seq[DataFrameSubFeed])(implicit context: ActionPipelineContext): Map[String, GenericDataFrame] = {
    val inputDfsMap = inputSubFeeds.map(subFeed => (subFeed.dataObjectId.id, subFeed.dataFrame.get)).toMap
    val (outputDfsMap, _) = transformers.foldLeft((inputDfsMap,inputPartitionValues)){
      case ((inputDfsMap, inputPartitionValues), transformer) =>
        val (outputDfsMap, outputPartitionValues) = transformer.applyTransformation(id, inputPartitionValues, inputDfsMap, executionModeResultOptions, outputs.map(_.id))
        (inputDfsMap ++ outputDfsMap, outputPartitionValues)
    }
    outputDfsMap
  }


  /**
   * apply transformer to partition values
   */
  protected def applyTransformers(transformers: Seq[PartitionValueTransformer], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    transformers.foldLeft(PartitionValues.oneToOneMapping(partitionValues)){
      case (partitionValuesMap, transformer) => transformer.applyTransformation(id, partitionValuesMap, executionModeResultOptions)
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
    super.preExec(subFeeds)
  }

  override def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Unit = {
    super.postExec(inputSubFeeds, outputSubFeeds)
    // auto-unpersist DataFrames no longer needed
    inputSubFeeds
      .collect { case subFeed: DataFrameSubFeed => subFeed }
      .foreach { subFeed =>
        if (context.forgetDataFrameReuse(subFeed.dataObjectId, subFeed.partitionValues, id).contains(0)) {
          val partitionValuesLog = if (subFeed.partitionValues.nonEmpty) s" and partitionValues=${subFeed.partitionValues.mkString(", ")}" else ""
          logger.info(s"($id) Removing cached DataFrame for ${subFeed.dataObjectId}$partitionValuesLog")
          subFeed.unpersist
        }
    }
  }
}
