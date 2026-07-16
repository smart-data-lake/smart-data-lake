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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject._
import io.smartdatalake.definitions._
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.ScalaUtil
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.action.executionMode.DataFrameStreamingExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformerDef, PartitionValueTransformer}
import io.smartdatalake.workflow.dataframe.{CombinedObservation, GenericDataFrame, SuffixedObservation}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.expectation.{ActionExpectation, Expectation, ExpectationScope}
import io.smartdatalake.workflow.dataobject.generic._

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
   * in case a task gets lost.
   * Note that DataFrames are persisted automatically by the previous Action if later Actions need the same data. To avoid this
   * behaviour set breakDataFrameLineage=false.
   */
  def persist: Boolean

  override def isAsynchronous: Boolean = executionMode.exists(_.isAsynchronous)

  override def isAsynchronousProcessStarted: Boolean = isAsynchronous && executionMode.exists(_.isStreamingStarted)

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
    val commonInputOutputTypes = commonInputTypes.intersect(commonOutputTypes)
    if (commonInputOutputTypes.isEmpty) throw ConfigurationException(s"($id) No common subfeed type found between inputs & outputs")
    val commonTypes: Seq[Type] = commonInputOutputTypes.filter(_ =:= getEngineConnection(instanceRegistry).subFeedType)
    if (commonTypes.isEmpty) throw ConfigurationException(s"($id) No common subfeed type found between inputs/outputs and engine connection")
    val commonType = if (transformerSubFeedType.isDefined && !(transformerSubFeedType.get =:= typeOf[DataFrameSubFeed])) {
      // if transformerSubFeedType is defined and not generic, we have to take that one and assert it is in common types list
      assert(commonTypes.contains(transformerSubFeedType.get),
        s"($id) subfeed type of transformers (${transformerSubFeedType.get}) doesn't exist in common subfeed types" +
          s" of inputs & outputs (${commonInputOutputTypes.mkString(", ")})")
      transformerSubFeedType.get
    } else {
      // if transformerSubFeedType is None or generic, take the first matching entry from the inputs list
      allInputTypes.flatten.find(commonInputOutputTypes.contains).get
    }
    logger.info(s"($id) selected subFeedType ${commonType.typeSymbol.name}")
    commonType
  }
  private[smartdatalake] implicit lazy val subFeedHelper: DataFrameSubFeedCompanion = {
    ScalaUtil.companionOf[DataFrameSubFeedCompanion](subFeedType)
  }

  private[smartdatalake] override def subFeedConverter: SubFeedConverter[DataFrameSubFeed] = subFeedHelper

  override def getRuntimeDataImpl: RuntimeData = {
    // use AsynchronousRuntimeData for streaming execution modes
    if (executionMode.exists(_.isStreamingMode)) AsynchronousRuntimeData(Environment.runtimeDataNumberOfExecutionsToKeep)
    else super.getRuntimeDataImpl
  }

  private[smartdatalake] def notifySparkStreamingQueryTerminated: Unit = {
    executionMode.foreach(_.notifyStreamingQueryTerminated())
  }

  override private[smartdatalake] def reset(implicit context: ActionPipelineContext): Unit = {
    super.reset
    executionMode.foreach(_.resetStreamingState())
  }

  /**
   * Enriches SparkSubFeed with DataFrame if not existing
   *
   * @param input       input data object.
   * @param subFeed     input SubFeed.
   * @param phase       current execution phase
   * @param isRecursive true if this input is a recursive input
   */
  def enrichSubFeedDataFrame(input: DataObject with CanCreateDataFrame,
                             subFeed: DataFrameSubFeed,
                             phase: ExecutionPhase,
                             isRecursive: Boolean = false)
                            (implicit context: ActionPipelineContext): DataFrameSubFeed = {
    logger.debug(s"($id) enrichSubFeedDataFrame: subFeed = $subFeed, isRecursive = $isRecursive")
    assert(input.id == subFeed.dataObjectId, s"($id) DataObject.Id ${input.id} doesn't match SubFeed.DataObjectId ${subFeed.dataObjectId} ")
    assert(phase != ExecutionPhase.Prepare, "Strangely enrichSubFeedDataFrame got called in phase prepare. It should only be called in Init and Exec.")
    executionMode match {
      case Some(m: DataFrameStreamingExecutionMode) if !context.simulation =>
        val refreshDataFrame = subFeed.dataFrame.isEmpty || phase == ExecutionPhase.Exec
        m.enrichSubFeedForStreamingInput(input, subFeed, phase, refreshDataFrame)
      case _ =>
        // count reuse of subFeed.dataFrame for caching/release in exec phase
        if (phase == ExecutionPhase.Init && subFeed.hasReusableDataFrame && Environment.enableAutomaticDataFrameCaching)
          context.rememberDataFrameReuse(subFeed.dataObjectId, subFeed.partitionValues, id)
        // process subfeed
        if (phase == ExecutionPhase.Exec || context.simulation) {
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
                logger.info(s"($id) enrichSubFeedDataFrame: getting DataFrame for ${input.id}" +
                  (if (subFeed.partitionValues.nonEmpty) s" filtered by partition values ${subFeed.partitionValues.mkString(" ")}" else "") +
                  subFeed.filter.map(f => s" filtered by $f").getOrElse(""))
                input.getSubFeed(subFeed.partitionValues, subFeedType) // get SubFeed of specified type with fresh DataFrame
                  .withFilter(subFeed.partitionValues, subFeed.filter)
              } catch {
                // if there is no data, but it's an action with multiple inputs, we need to avoid that the action gets skipped because of the thrown NoDataToProcessWarning
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

  def createEmptyDataFrame(dataObject: DataObject with CanCreateDataFrame)
                          (implicit context: ActionPipelineContext): GenericDataFrame = {
    val schema = dataObject match {
      case input: UserDefinedSchema if input.schema.isDefined => input.schema
      case input: SchemaValidation if input.schemaMin.isDefined => input.schemaMin
      case _ if context.globalConfig.dataObjectsSchemaSource.isDefined && !context.isExecPhase =>
        context.globalConfig.getSchemaFromSource(dataObject.id)(context.hadoopConf)
      case _ => None
    }
    val readSchema = schema.map(dataObject.createReadSchema)
    readSchema
      .map(s => subFeedHelper.getEmptyDataFrame(s, dataObject.id))
      .getOrElse(dataObject.getDataFrame(Seq(), subFeedType).filter(subFeedHelper.lit(false)))
  }

  override protected def preprocessInputSubFeedCustomized(subFeed: DataFrameSubFeed,
                                                          ignoreFilters: Boolean,
                                                          isRecursive: Boolean)
                                                         (implicit context: ActionPipelineContext): DataFrameSubFeed = {
    logger.debug(s"($id) preprocessInputSubFeedCustomized: subFeed = $subFeed, ignoreFilters = $ignoreFilters, , isRecursive = $isRecursive")
    val inputMap = (inputs ++ recursiveInputs).map(i => i.id -> i).toMap
    val input = inputMap(subFeed.dataObjectId)
    // persist if requested
    var preparedSubFeed = if (persist) subFeed.persist else subFeed
    // create dummy DataFrame if read schema is different from write schema on this DataObject
    val writeSchema = preparedSubFeed.schema
    val readSchema = writeSchema.map(schema => input.createReadSchema(schema))
    val schemaChanges = writeSchema != readSchema
    require(!context.simulation || !schemaChanges,
      s"($id) write & read schema is not the same for ${input.id}. Need to create a dummy DataFrame, but this is not allowed in simulation!")
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
    preparedSubFeed = enrichSubFeedDataFrame(input = input, subFeed = preparedSubFeed,
      phase = context.phase, isRecursive = isRecursive)
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
          preparedSubFeed = preparedSubFeed.withDataFrame(Some(dfExpectations))
          if (observations.nonEmpty) preparedSubFeed = preparedSubFeed.withObservation(Some(CombinedObservation.create(observations)))
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
        // Link output observations with sibling input observations so engine-specific observations (e.g. SparkObservation)
        // can extract combined metrics. Default implementation is a no-op; overridden by SparkObservation.
        outputObservations.foreach { obs =>
          obs.linkWithInputObservations(inputSubFeeds.flatMap(_.observation), id.id + "#")
        }
        // Combine non-engine-specific input observations with the output observation into a single combined
        // observation carried by the SubFeed.  Engine-specific observations (e.g. SparkObservation) opt out
        // via includeInInputObservationCombine=false because they are already handled by cross-linking above.
        val inputObservationsToCombine = inputSubFeeds.flatMap { subFeed =>
          subFeed.observation match {
            case Some(obs) if !obs.includeInInputObservationCombine => None // handled by cross-linking above
            case Some(otherObservation) => Some(SuffixedObservation(otherObservation, "#" + subFeed.dataObjectId.id)) // add input DataObjectId suffix to metrics, e.g. count#src1
            case None => None
          }
        }
        // add updated dataframe and observation to SubFeed
        var postSubFeed = subFeed.withDataFrame(Some(dfExpectations))
        val observations = inputObservationsToCombine ++ outputObservations
        if (observations.nonEmpty) postSubFeed = postSubFeed.withObservation(Some(CombinedObservation.create(inputObservationsToCombine ++ outputObservations)))
        postSubFeed
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
    context.engineConnection.foreach(_.activate(Some(s"writing to ${subFeed.dataObjectId}")))
    val output = outputs.find(_.id == subFeed.dataObjectId).getOrElse(throw new IllegalStateException(s"($id) output for subFeed ${subFeed.dataObjectId} not found"))
    var outputSubFeed = writeSubFeed(subFeed, output, isRecursive)
    context.engineConnection.foreach(_.activate(None))
    if (breakDataFrameOutputLineage) outputSubFeed = outputSubFeed.breakLineage
    val isMainOutput = mainOutput.id == output.id
    // get expectations metrics and check violations
    outputSubFeed = output match {
      case evDataObject: DataObject with ExpectationValidation with CanCreateDataFrame =>
        // get metrics with scope Job from observations
        val scopeJobExpectationMetrics = subFeed.observation.map(_.waitForElseNoData()).getOrElse(Map())
        // get input metrics for these actions expectations with scope All
        // (scope=Job is calculated with preprocessInputSubFeedCustomized, scope=JobPartition is not supported on input)
        // Note that scope All metrics are only calculated if this is the main output.
        val actionExpectationsInputMetrics = if (isMainOutput) calculateInputAggMetricsWithScopeAll(subFeed) else Map()
        // if this is mainOutput, enrich main input metrics
        val enrichmentFunc: Map[String, _] => Map[String, _] = if (isMainOutput) enrichMainInputMetrics else identity
        // evaluate and validate expectations
        var (metrics, exceptions) = evDataObject
          .validateExpectations(subFeedType, subFeed.dataFrame, evDataObject.getDataFrame(Seq(), subFeed.tpe), subFeed.partitionValues, scopeJobExpectationMetrics ++ actionExpectationsInputMetrics, if (isMainOutput) expectations else Seq(), enrichmentFunc, loggerContext = "output")
        // evaluate and validate expectations of input DataObjects to be validated on read
        val inputExpectationsToEvaluateOnRead = inputs
          .filter(i => context.instanceRegistry.shouldValidateDataObjectOnRead(i.id))
          .collect { case x: DataObject with ExpectationValidation => x }
        inputExpectationsToEvaluateOnRead.foreach { dataObject =>
          val metricsSuffix = "#" + dataObject.id.id
          val inputMetrics = metrics
            .filter(_._1.endsWith(metricsSuffix)).map { case (k, v) => (k.stripSuffix(metricsSuffix), v) }
          if (inputMetrics.nonEmpty) {
            val (updatedInputMetrics, inputExceptions) = dataObject
              .validateExpectations(subFeedType, None, dataObject.getDataFrame(Seq(), subFeed.tpe), partitionValues = Seq(), enrichmentFunc = identity, scopeJobAndInputMetrics = inputMetrics, loggerContext = s"input ${dataObject.id}")
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
    // cleanup inconsistent Spark recordsWritten-metric and count observation
    def getOutputMetric(metricName: String) = outputSubFeed.metrics.flatMap(_.get(metricName))
    val recordsWrittenOrg = getOutputMetric("records_written")
    val countOrg = getOutputMetric("count")
    (recordsWrittenOrg, countOrg) match {
      case (Some(rw: Long), Some(c: Long)) =>
        val countMax = math.max(rw,c)
        outputSubFeed = outputSubFeed.withMetrics(outputSubFeed.metrics.get + ("count" -> countMax) + ("records_written" -> countMax)).asInstanceOf[DataFrameSubFeed]
      case _ => ()
    }
    // add no_data metric
    if (getOutputMetric("count").orElse(getOutputMetric("records_written")).contains(0)) outputSubFeed = outputSubFeed.appendMetrics(Map[String, Any]("no_data" -> true)).asInstanceOf[DataFrameSubFeed]
    // throw NoDataToProcessWarning if there is no data to process for this output
    if (getOutputMetric("no_data").contains(true)) throw NoDataToProcessWarning(id.id, s"($id) no data to process for ${output.id}", results = Some(Seq(outputSubFeed)))
    // return
    outputSubFeed
  }

  def calculateInputAggMetricsWithScopeAll(subFeed: DataFrameSubFeed)(implicit context: ActionPipelineContext): Map[String, Any] = {
    // prepare input aggregation metrics columns from actions expectations
    val exprNameRegex = "([^#]+)#([^#]+)".r.anchored
    val actionExpectationsInputAggColumns = expectations.filter(_.scope == ExpectationScope.All).flatMap(_.getInputAggExpressionColumns(id))
      .map(expr => expr.getName match {
        case Some(exprNameRegex(name, dataObjectId)) => (DataObjectId(dataObjectId), expr.as(name))
        case Some(_) => (prioritizedMainInputCandidates.head.id, expr)
        case None => throw new IllegalStateException(s"($id) name of aggregate expression unknown: $expr")
      })
    // calculate metrics on input DataObject
    val inputAggColumns = actionExpectationsInputAggColumns
      .groupBy(_._1).view.mapValues(_.map(_._2)).toMap
    inputAggColumns.flatMap { case (dataObjectId, aggExpressions) =>
      val dataObject = inputMap(dataObjectId) match {
        case evDataObject: DataObject with ExpectationValidation with CanCreateDataFrame => evDataObject
        case _ => throw new IllegalStateException(s"($id) Cannot calculate input metric on $dataObjectId not supporting ExpectationValidation")
      }
      dataObject.calculateMetrics(dataObject.getDataFrame(Seq(), subFeed.tpe), aggExpressions, ExpectationScope.All)
        .map { case (k, v) => (k + "#" + dataObjectId.id, v) }
    }
  }

  def enrichMainInputMetrics(metrics: Map[String, _]): Map[String, _] = {
    val mainInputIdSuffix = s"#${prioritizedMainInputCandidates.head.id.id}"
    // copy all metrics with name `<metric>#<dataObjectId>` as `<metric>#mainInput`
    metrics ++
      metrics.view.filterKeys(_.endsWith(mainInputIdSuffix)).toMap
        .map { case (k, v) => k.stripSuffix(mainInputIdSuffix) + "#mainInput" -> v }
  }

  /**
   * writes subfeed to output respecting given execution mode
   */
  def writeSubFeed(subFeed: DataFrameSubFeed, output: DataObject with CanWriteDataFrame, isRecursiveInput: Boolean = false)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    assert(!subFeed.isDummy, s"($id) Can not write dummy DataFrame to ${output.id}")
    // write
    executionMode match {
      case Some(m: DataFrameStreamingExecutionMode) =>
        assert(subFeed.isStreaming.getOrElse(false), s"($id) ExecutionMode ${m.getClass.getSimpleName} needs streaming DataFrame in SubFeed")
        m.writeSubFeedStreaming(this, subFeed, output, getStreamingQueryName(output.id))
      case _ =>
        // Auto persist if dataFrame is reused later
        val preparedSubFeed = if (context.dataFrameReuseStatistics.contains((output.id, subFeed.partitionValues))) {
          val partitionValuesStr = if (subFeed.partitionValues.nonEmpty) s" and partitionValues ${subFeed.partitionValues.mkString(", ")}" else ""
          logger.info(s"($id) Caching dataframe for ${output.id}$partitionValuesStr")
          subFeed.persist
        } else subFeed
        // Write in batch mode
        assert(!preparedSubFeed.isStreaming.getOrElse(false), s"($id) Input from ${preparedSubFeed.dataObjectId} is a streaming DataFrame, but executionMode does not support streaming")
        assert(!preparedSubFeed.isDummy, s"($id) Input from ${preparedSubFeed.dataObjectId} is a dummy. Cannot write dummy DataFrame.")
        assert(!preparedSubFeed.isSkipped, s"($id) Input from ${preparedSubFeed.dataObjectId} is a skipped. Cannot write skipped DataFrame.")
        val df = preparedSubFeed.dataFrame.get
        val metrics = output.writeDataFrame(df, preparedSubFeed.partitionValues, isRecursiveInput, saveModeOptions)
        // return
        preparedSubFeed.withMetrics(metrics).asInstanceOf[DataFrameSubFeed]
    }
  }

  private def getStreamingQueryName(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext) = {
    s"${context.appConfig.appName} $id writing $dataObjectId"
  }

  /**
   * Apply many-to-many transformers to SubFeeds.
   * Keep outputs of previous transformers as input for next transformer, but in the end only return outputs of last transformer.
   *
   * @return outputDataFrameMap and outputPartitionValues of last transformer
   */
  private[smartdatalake] def applyTransformers(transformers: Seq[GenericDfsTransformerDef], inputPartitionValues: Seq[PartitionValues], inputSubFeeds: Seq[DataFrameSubFeed])(implicit context: ActionPipelineContext): Map[String, GenericDataFrame] = {
    val inputDfsMap = inputSubFeeds.map(subFeed => (subFeed.dataObjectId.id, subFeed.dataFrame.get)).toMap
    val (outputDfsMap, _) = transformers.foldLeft((inputDfsMap, inputPartitionValues)) {
      case ((inputDfsMap, inputPartitionValues), transformer) =>
        val (outputDfsMap, outputPartitionValues) = transformer.applyTransformation(id, inputPartitionValues, inputDfsMap, executionModeResultOptions, outputs.map(_.id))
        (inputDfsMap ++ outputDfsMap, outputPartitionValues)
    }
    outputDfsMap
  }


  /**
   * apply transformer to partition values
   */
  protected def applyTransformers(transformers: Seq[PartitionValueTransformer], partitionValues: Seq[PartitionValues])
                                 (implicit context: ActionPipelineContext): Map[PartitionValues, PartitionValues] = {
    transformers.foldLeft(PartitionValues.oneToOneMapping(partitionValues)) {
      case (partitionValuesMap, transformer) => transformer.applyTransformation(id, partitionValuesMap, executionModeResultOptions)
    }
  }

  /**
   * The transformed DataFrame is validated to have the output's partition columns included, partition columns are moved to the end and SubFeeds partition values updated.
   *
   * @param output  output DataObject
   * @param subFeed SubFeed with transformed DataFrame
   * @return validated and updated SubFeed
   */
  def validateAndUpdateSubFeedCustomized(output: DataObject, subFeed: DataFrameSubFeed)
                                        (implicit context: ActionPipelineContext): DataFrameSubFeed = {
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
   * @param df        DataFrame to validate
   * @param columns   Columns that must exist in DataFrame
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
