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
package io.smartdatalake.definitions

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.dag.DAGHelper.NodeId
import io.smartdatalake.util.dag.ExceptionSeverity.ExceptionSeverity
import io.smartdatalake.util.dag.{DAGException, ExceptionSeverity}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{CustomCodeUtil, ProductUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.SparkExpressionUtil
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.action.ActionHelper.{getOptionalDataFrame, searchCommonInits}
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.sql.Timestamp
import scala.reflect.runtime.universe.TypeTag

/**
 * Result of execution mode application
 */
case class ExecutionModeResult( inputPartitionValues: Seq[PartitionValues] = Seq(), outputPartitionValues: Seq[PartitionValues] = Seq()
                              , filter: Option[String] = None, fileRefs: Option[Seq[FileRef]] = None, options: Map[String,String] = Map())

/**
 * Execution mode defines how data is selected when running a data pipeline.
 * You need to select one of the subclasses by defining type, i.e.
 *
 * {{{
 * executionMode = {
 *   type = DataFrameIncrementalMode
 *   compareCol = "id"
 * }
 * }}}
 */
sealed trait ExecutionMode extends SmartDataLakeLogger {
  /**
   * Called in prepare phase to validate execution mode configuration
   */
  private[smartdatalake] def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = {
    // validate apply conditions
    applyConditionsDef.foreach(_.syntaxCheck[DefaultExecutionModeExpressionData](actionId, Some("applyCondition")))
  }
  /**
   * Called in init phase before initialization. Can be used to initialize dataObjectsState, e.g. for DataObjectStateIncrementalMode
   */
  private[smartdatalake] def preInit(subFeeds: Seq[SubFeed], dataObjectsState: Seq[DataObjectState])(implicit context: ActionPipelineContext): Unit = Unit
  /**
   * Called in init phase to apply execution mode. Result is stored and re-used in execution phase.
   */
  private[smartdatalake] def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                   , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                  (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = None
  /**
   * Called in execution phase after writing subfeed. Can be used to implement incremental processing , e.g. deleteDataAfterRead.
   */
  private[smartdatalake] def postExec(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, mainInputSubFeed: SubFeed, mainOutputSubFeed: SubFeed)(implicit context: ActionPipelineContext): Unit = Unit
  private[smartdatalake] def mainInputOutputNeeded: Boolean = false
  private[smartdatalake] val applyConditionsDef: Seq[Condition] = Seq()
  private[smartdatalake] val failConditionsDef: Seq[Condition] = Seq()

  /**
   * Evaluate apply conditions.
   * @return Some(true) if any apply conditions evaluates to true (or-logic), None if there are no apply conditions
   */
  private[smartdatalake] final def evaluateApplyConditions(actionId: ActionId, subFeed: SubFeed)(implicit context: ActionPipelineContext): Option[Boolean] = {
    val data = DefaultExecutionModeExpressionData.from(context).copy(givenPartitionValues = subFeed.partitionValues.map(_.getMapString), isStartNode = subFeed.isDAGStart)
    if (applyConditionsDef.nonEmpty) Option(applyConditionsDef.map(_.evaluate(actionId, Some("applyCondition"), data)).max)
    else None
  }

  /**
   * Evaluate fail conditions.
   * @throws ExecutionModeFailedException if any fail condition evaluates to true
   */
  private[smartdatalake] final def evaluateFailConditions[T<:Product:TypeTag](actionId: ActionId, data: T)(implicit context: ActionPipelineContext): Unit = {
    failConditionsDef.foreach(c =>
      if (c.evaluate(actionId, Some("failCondition"), data)) {
        val descriptionText = c.description.map( d => s""""$d" """).getOrElse("")
        throw ExecutionModeFailedException(actionId.id, context.phase, s"""($actionId) Execution mode failed because of failCondition ${descriptionText}expression="${c.expression}" $data""")
      }
    )
  }

  /**
   * If this execution mode should be run as asynchronous streaming process
   */
  private[smartdatalake] def isAsynchronous: Boolean = false
}

private[smartdatalake] trait ExecutionModeWithMainInputOutput {
  def alternativeOutputId: Option[DataObjectId] = None
  def alternativeOutput(implicit context: ActionPipelineContext): Option[DataObject] = {
    alternativeOutputId.map(context.instanceRegistry.get[DataObject](_))
  }
}

/**
 * Partition difference execution mode lists partitions on mainInput & mainOutput DataObject and starts loading all missing partitions.
 * Partition columns to be used for comparision need to be a common 'init' of input and output partition columns.
 * This mode needs mainInput/Output DataObjects which CanHandlePartitions to list partitions.
 * Partition values are passed to following actions for partition columns which they have in common.
 *
 * @param partitionColNb            optional number of partition columns to use as a common 'init'.
 * @param alternativeOutputId       optional alternative outputId of DataObject later in the DAG. This replaces the mainOutputId.
 *                                  It can be used to ensure processing all partitions over multiple actions in case of errors.
 * @param nbOfPartitionValuesPerRun optional restriction of the number of partition values per run.
 * @param applyCondition            Condition to decide if execution mode should be applied or not. Define a spark sql expression working with attributes of [[DefaultExecutionModeExpressionData]] returning a boolean.
 *                                  Default is to apply the execution mode if given partition values (partition values from command line or passed from previous action) are not empty.
 * @param failConditions            List of conditions to fail application of execution mode if true. Define as spark sql expressions working with attributes of [[PartitionDiffModeExpressionData]] returning a boolean.
 *                                  Default is that the application of the PartitionDiffMode does not fail the action. If there is no data to process, the following actions are skipped.
 *                                  Multiple conditions are evaluated individually and every condition may fail the execution mode (or-logic)
 * @param selectExpression          optional expression to define or refine the list of selected output partitions. Define a spark sql expression working with the attributes of [[PartitionDiffModeExpressionData]] returning a list<map<string,string>>.
 *                                  Default is to return the originally selected output partitions found in attribute selectedOutputPartitionValues.
 * @param applyPartitionValuesTransform If true applies the partition values transform of custom transformations on input partition values before comparison with output partition values.
 *                                  If enabled input and output partition columns can be different. Default is to disable the transformation of partition values.
 * @param selectAdditionalInputExpression optional expression to refine the list of selected input partitions. Note that primarily output partitions are selected by PartitionDiffMode.
 *                                  The selected output partitions are then transformed back to the input partitions needed to create the selected output partitions. This is one-to-one except if applyPartitionValuesTransform=true.
 *                                  And sometimes there is a need for additional input data to create the output partitions, e.g. if you aggregate a window of 7 days for every day.
 *                                  You can customize selected input partitions by defining a spark sql expression working with the attributes of [[PartitionDiffModeExpressionData]] returning a list<map<string,string>>.
 *                                  Default is to return the originally selected input partitions found in attribute selectedInputPartitionValues.
 */
case class PartitionDiffMode( partitionColNb: Option[Int] = None
                            , override val alternativeOutputId: Option[DataObjectId] = None
                            , nbOfPartitionValuesPerRun: Option[Int] = None
                            , applyCondition: Option[String] = None
                            , failCondition: Option[String] = None
                            , failConditions: Seq[Condition] = Seq()
                            , selectExpression: Option[String] = None
                            , applyPartitionValuesTransform: Boolean = false
                            , selectAdditionalInputExpression: Option[String] = None
                            ) extends ExecutionMode with ExecutionModeWithMainInputOutput {
  private[smartdatalake] override val applyConditionsDef = applyCondition.toSeq.map(Condition(_))
  private[smartdatalake] override val failConditionsDef = failCondition.toSeq.map(Condition(_)) ++ failConditions
  private[smartdatalake] override def mainInputOutputNeeded: Boolean = alternativeOutputId.isEmpty
  private[smartdatalake] override def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = {
    super.prepare(actionId)
    // validate fail condition
    failConditionsDef.foreach(_.syntaxCheck[PartitionDiffModeExpressionData](actionId, Some("failCondition")))
    // validate select expression
    selectExpression.foreach(expression => SparkExpressionUtil.syntaxCheck[PartitionDiffModeExpressionData, Seq[Map[String,String]]](actionId, Some("selectExpression"), expression))
    // validate select additional input expression
    selectAdditionalInputExpression.foreach(expression => SparkExpressionUtil.syntaxCheck[PartitionDiffModeExpressionData, Seq[Map[String,String]]](actionId, Some("selectAdditionalInputExpression"), expression))
    // check alternativeOutput exists
    alternativeOutput
  }
  private[smartdatalake] override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    val doApply = evaluateApplyConditions(actionId, subFeed)
      .getOrElse(subFeed.partitionValues.isEmpty) // default is to apply PartitionDiffMode if no partition values are given
    if (doApply) {
      val input = mainInput
      val output = alternativeOutput.getOrElse(mainOutput)
      (input, output) match {
        case (partitionInput: CanHandlePartitions, partitionOutput: CanHandlePartitions) =>
          if (partitionInput.partitions.nonEmpty) {
            if (partitionOutput.partitions.nonEmpty) {
              // prepare common partition columns
              val (inputPartitions, outputPartitions) = if (applyPartitionValuesTransform) (partitionInput.partitions, partitionOutput.partitions)
              else {
                val commonInits = searchCommonInits(partitionInput.partitions, partitionOutput.partitions)
                if (commonInits.nonEmpty) {
                  val commonInit = if (partitionColNb.isDefined) {
                    commonInits.find(_.size == partitionColNb.get).getOrElse(throw ConfigurationException(s"$actionId has set executionMode = 'PartitionDiffMode' but no common init with ${partitionColNb.get} was found in partition columns of ${input.id} and ${output.id} from $commonInits!"))
                  } else {
                    commonInits.maxBy(_.size)
                  }
                  (commonInit, commonInit)
                } else {
                  throw new ConfigurationException(s"$actionId has set executionMode = 'PartitionDiffMode' but no common init was found in partition columns for ${input.id} and ${output.id}. Enable applyPartitionValuesTransform to transform partition values.")
                }
              }
              // calculate missing partition values
              val filteredInputPartitionValues = partitionInput.listPartitions.map(_.filterKeys(inputPartitions))
              val filteredOutputPartitionValues = partitionOutput.listPartitions.map(_.filterKeys(outputPartitions))
              val inputOutputPartitionValuesMap = if (applyPartitionValuesTransform) {
                partitionValuesTransform(filteredInputPartitionValues).mapValues(_.filterKeys(outputPartitions))
              } else PartitionValues.oneToOneMapping(filteredInputPartitionValues) // normally this transformation is 1:1, but it can be implemented in custom transformations for aggregations
              val outputInputPartitionValuesMap = inputOutputPartitionValuesMap.toSeq.map{ case (k,v) => (v,k)}.groupBy(_._1).mapValues(_.map(_._2))
              val outputPartitionValuesToBeProcessed = inputOutputPartitionValuesMap.values.toSet.diff(filteredOutputPartitionValues.toSet).toSeq
              // sort and limit number of partitions processed
              val outputOrdering = PartitionValues.getOrdering(outputPartitions)
              var selectedOutputPartitionValues = nbOfPartitionValuesPerRun match {
                case Some(n) => outputPartitionValuesToBeProcessed.sorted(outputOrdering).take(n)
                case None => outputPartitionValuesToBeProcessed.sorted(outputOrdering)
              }
              // reverse lookup input partitions as selection of output partitions might have changed
              var selectedInputPartitionValues = selectedOutputPartitionValues.flatMap(outputInputPartitionValuesMap)
              // apply optional select expression
              var data = PartitionDiffModeExpressionData.from(context).copy(givenPartitionValues = subFeed.partitionValues.map(_.getMapString),
                inputPartitionValues = filteredInputPartitionValues.map(_.getMapString), outputPartitionValues = filteredOutputPartitionValues.map(_.getMapString),
                selectedInputPartitionValues = selectedInputPartitionValues.map(_.getMapString), selectedOutputPartitionValues = selectedOutputPartitionValues.map(_.getMapString))
              selectedOutputPartitionValues = if (selectExpression.isDefined) {
                SparkExpressionUtil.evaluate[PartitionDiffModeExpressionData, Seq[Map[String, String]]](actionId, Some("selectExpression"), selectExpression.get, data)
                  .map(_.map(PartitionValues(_)))
                  .getOrElse(selectedOutputPartitionValues)
                  .sorted(outputOrdering)
              } else selectedOutputPartitionValues
              // reverse lookup input partitions as selection of output partitions might have changed
              val inputOrdering = PartitionValues.getOrdering(inputPartitions)
              selectedInputPartitionValues = selectedOutputPartitionValues.flatMap(outputInputPartitionValuesMap)
              data = data.copy(selectedInputPartitionValues = selectedInputPartitionValues.map(_.getMapString), selectedOutputPartitionValues = selectedOutputPartitionValues.map(_.getMapString))
              // apply optional select additional input partitions expression
              selectedInputPartitionValues = if (selectAdditionalInputExpression.isDefined) {
                SparkExpressionUtil.evaluate[PartitionDiffModeExpressionData, Seq[Map[String, String]]](actionId, Some("selectAdditionalInputExpression"), selectAdditionalInputExpression.get, data)
                  .map(_.map(PartitionValues(_)))
                  .getOrElse(selectedInputPartitionValues)
                  .sorted(inputOrdering)
              } else selectedInputPartitionValues.sorted(inputOrdering)
              data = data.copy(selectedInputPartitionValues = selectedInputPartitionValues.map(_.getMapString))
              // evaluate fail conditions
              evaluateFailConditions(actionId, data) // throws exception on failed condition
              // skip processing if no new data
              val warnMsg = if (selectedOutputPartitionValues.isEmpty) Some(s"($actionId) No partitions to process found for ${input.id}") else None
              warnMsg.foreach(msg => throw NoDataToProcessWarning(actionId.id, msg))
              //return
              val inputPartitionLog = if (selectedInputPartitionValues != selectedOutputPartitionValues) s" by using input partitions ${selectedInputPartitionValues.mkString(", ")}" else ""
              logger.info(s"($actionId) PartitionDiffMode selected output partition values ${selectedOutputPartitionValues.mkString(", ")} to process$inputPartitionLog.")
              Some(ExecutionModeResult(inputPartitionValues = selectedInputPartitionValues, outputPartitionValues = selectedOutputPartitionValues))
            } else throw ConfigurationException(s"$actionId has set executionMode = PartitionDiffMode but ${output.id} has no partition columns defined!")
          } else throw ConfigurationException(s"$actionId has set executionMode = PartitionDiffMode but ${input.id} has no partition columns defined!")
        case (_: CanHandlePartitions, _) => throw ConfigurationException(s"$actionId has set executionMode = PartitionDiffMode but ${output.id} does not support partitions!")
        case (_, _) => throw ConfigurationException(s"$actionId has set executionMode = PartitionDiffMode but ${input.id} does not support partitions!")
      }
    } else None
  }
}

/**
 * @param givenPartitionValues partition values received by main input or command line
 * @param inputPartitionValues all partition values existing in main input DataObject
 * @param outputPartitionValues all partition values existing in main output DataObject
 * @param selectedInputPartitionValues input partition values selected by PartitionDiffMode
 * @param selectedOutputPartitionValues output partition values selected by PartitionDiffMode
 */
case class PartitionDiffModeExpressionData(feed: String, application: String, runId: Int, attemptId: Int, referenceTimestamp: Option[Timestamp]
                                           , runStartTime: Timestamp, attemptStartTime: Timestamp
                                           , givenPartitionValues: Seq[Map[String,String]], inputPartitionValues: Seq[Map[String,String]], outputPartitionValues: Seq[Map[String,String]]
                                           , selectedInputPartitionValues: Seq[Map[String,String]], selectedOutputPartitionValues: Seq[Map[String,String]]) {
  override def toString: String = ProductUtil.formatObj(this)
}
private[smartdatalake] object PartitionDiffModeExpressionData {
  def from(context: ActionPipelineContext): PartitionDiffModeExpressionData = {
    PartitionDiffModeExpressionData(context.feed, context.application, context.executionId.runId, context.executionId.attemptId, context.referenceTimestamp.map(Timestamp.valueOf)
      , Timestamp.valueOf(context.runStartTime), Timestamp.valueOf(context.attemptStartTime), Seq(), Seq(), Seq(), Seq(), Seq())
  }
}

/**
 * Spark streaming execution mode uses Spark Structured Streaming to incrementally execute data loads and keep track of processed data.
 * This mode needs a DataObject implementing CanCreateStreamingDataFrame and works only with SparkSubFeeds.
 * This mode can be executed synchronously in the DAG by using triggerType=Once, or asynchronously as Streaming Query with triggerType = ProcessingTime or Continuous.
 * @param checkpointLocation location for checkpoints of streaming query to keep state
 * @param triggerType define execution interval of Spark streaming query. Possible values are Once (default), ProcessingTime & Continuous. See [[Trigger]] for details.
                      Note that this is only applied if SDL is executed in streaming mode. If SDL is executed in normal mode, TriggerType=Once is used always.
 *                    If triggerType=Once, the action is repeated with Trigger.Once in SDL streaming mode.
 * @param triggerTime Time as String in triggerType = ProcessingTime or Continuous. See [[Trigger]] for details.
 * @param inputOptions additional option to apply when reading streaming source. This overwrites options set by the DataObjects.
 * @param outputOptions additional option to apply when writing to streaming sink. This overwrites options set by the DataObjects.
 */
case class SparkStreamingMode(checkpointLocation: String, triggerType: String = "Once", triggerTime: Option[String] = None, inputOptions: Map[String,String] = Map(), outputOptions: Map[String,String] = Map(), outputMode: OutputMode = OutputMode.Append) extends ExecutionMode {
  // parse trigger from config attributes
  private[smartdatalake] val trigger = triggerType.toLowerCase match {
    case "once" =>
      assert(triggerTime.isEmpty, "triggerTime must not be set for SparkStreamingMode with triggerType=Once")
      Trigger.Once()
    case "processingtime" =>
      assert(triggerTime.isDefined, "triggerTime must be set for SparkStreamingMode with triggerType=ProcessingTime")
      Trigger.ProcessingTime(triggerTime.get)
    case "continuous" =>
      assert(triggerTime.isDefined, "triggerTime must be set for SparkStreamingMode with triggerType=Continuous")
      Trigger.Continuous(triggerTime.get)
  }
  override private[smartdatalake] def isAsynchronous = trigger != Trigger.Once
}


/**
 * Compares max entry in "compare column" between mainOutput and mainInput and incrementally loads the delta.
 * This mode works only with SparkSubFeeds. The filter is not propagated to following actions.
 * @param compareCol a comparable column name existing in mainInput and mainOutput used to identify the delta. Column content should be bigger for newer records.
 * @param alternativeOutputId optional alternative outputId of DataObject later in the DAG. This replaces the mainOutputId.
 *                            It can be used to ensure processing all partitions over multiple actions in case of errors.
 * @param applyCondition Condition to decide if execution mode should be applied or not. Define a spark sql expression working with attributes of [[DefaultExecutionModeExpressionData]] returning a boolean.
 *                       Default is to apply the execution mode if given partition values (partition values from command line or passed from previous action) are not empty.
 */
case class DataFrameIncrementalMode(compareCol: String
                                    , override val alternativeOutputId: Option[DataObjectId] = None
                                    , applyCondition: Option[Condition] = None
                               ) extends ExecutionMode with ExecutionModeWithMainInputOutput {
  private[smartdatalake] override val applyConditionsDef = applyCondition.toSeq
  private[smartdatalake] override def mainInputOutputNeeded: Boolean = alternativeOutputId.isEmpty
  private[smartdatalake] override def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = {
    super.prepare(actionId)
    // check alternativeOutput exists
    alternativeOutput
  }
  private[smartdatalake] override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    assert(subFeed.isInstanceOf[DataFrameSubFeed], s"($actionId) DataFrameIncrementalMode needs DataFrameSubFeed to operate but received ${subFeed.getClass.getSimpleName}")
    val doApply = evaluateApplyConditions(actionId, subFeed)
      .getOrElse(true) // default is to apply DataFrameIncrementalMode
    if (doApply) {
      val input = mainInput
      val output = alternativeOutput.getOrElse(mainOutput)
      val dfSubFeed = subFeed.asInstanceOf[DataFrameSubFeed]
      import dfSubFeed.companion._
      (input, output) match {
        case (doDfInput: CanCreateDataFrame, doDfOutput: CanCreateDataFrame) =>
          // if data object is new, it might not be able to create a DataFrame
          val dfInputOpt = getOptionalDataFrame(doDfInput, Seq(), dfSubFeed.tpe)
          val dfOutputOpt = getOptionalDataFrame(doDfOutput, Seq(), dfSubFeed.tpe)
          (dfInputOpt, dfOutputOpt) match {
            // if both DataFrames exist, compare and create filter
            case (Some(dfInput), Some(dfOutput)) =>
              val inputColType = dfInput.schema.getDataType(compareCol)
              require(inputColType.isSortable, s"($actionId) Type of compare column $compareCol must be sortable, but is ${inputColType.typeName} in ${doDfInput.id}")
              val outputColType = dfOutput.schema.getDataType(compareCol)
              require(outputColType.isSortable, s"($actionId) Type of compare column $compareCol must be sortable, but is ${inputColType.typeName} in ${doDfInput.id}")
              require(inputColType == outputColType, s"($actionId) Type of compare column $compareCol is different between ${doDfInput.id} (${inputColType.typeName}) and ${doDfOutput.id} (${outputColType.typeName})")
              // get latest values
              val inputLatestValue = dfInput.agg(Seq(max(col(compareCol)).cast(stringType))).collect.head.getAs[String](0)
              val outputLatestValue = dfOutput.agg(Seq(max(col(compareCol)).cast(stringType))).collect.head.getAs[String](0)
              // skip processing if no new data
              val warnMsg = if (inputLatestValue == null) {
                Some(s"($actionId) No increment to process found for ${output.id}: ${input.id} is empty")
              } else if (outputLatestValue == inputLatestValue) {
                Some(s"($actionId) No increment to process found for ${output.id} column $compareCol (lastestValue=$outputLatestValue)")
              } else None
              warnMsg.foreach(msg => throw NoDataToProcessWarning(actionId.id, msg))
              // prepare filter
              val dataFilter = if (outputLatestValue != null) {
                logger.info(s"($actionId) DataFrameIncrementalMode selected increment for writing to ${output.id}: column $compareCol} from $outputLatestValue to $inputLatestValue to process")
                Some(s"$compareCol > cast('$outputLatestValue' as ${inputColType.sql})")
              } else {
                logger.info(s"($actionId) DataFrameIncrementalMode selected all data for writing to ${output.id}: output table is currently empty")
                None
              }
              Some(ExecutionModeResult(filter = dataFilter))
            // select all if output is empty
            case (Some(_),None) =>
              logger.info(s"($actionId) DataFrameIncrementalMode selected all records for writing to ${output.id}, because output DataObject is still empty.")
              Some(ExecutionModeResult())
            // otherwise no data to process
            case _ =>
              val warnMsg = s"($actionId) No increment to process found for ${output.id}, because ${input.id} is still empty."
              throw NoDataToProcessWarning(actionId.id, warnMsg)
          }
        case _ => throw ConfigurationException(s"$actionId has set executionMode = $DataFrameIncrementalMode but $input or $output does not support creating Spark DataFrames!")
      }
    } else None
  }
}

/**
 * An execution mode for incremental processing by remembering DataObjects state from last increment.
 */
case class DataObjectStateIncrementalMode() extends ExecutionMode {
  private var inputsWithIncrementalOutput: Seq[DataObject with CanCreateIncrementalOutput] = Seq()
  override def preInit(subFeeds: Seq[SubFeed], dataObjectsState: Seq[DataObjectState])(implicit context: ActionPipelineContext): Unit = {
    // initialize dataObjectsState
    val unrelatedStateDataObjectIds = dataObjectsState.map(_.dataObjectId).diff(subFeeds.map(_.dataObjectId))
    assert(unrelatedStateDataObjectIds.isEmpty, s"Got state for unrelated DataObjects ${unrelatedStateDataObjectIds.mkString(", ")}")
    // assert SDL is started with state
    assert(context.appConfig.statePath.isDefined, s"SmartDataLakeBuilder must be started with state path set. Please specify location of state with parameter '--state-path'.")
    // set DataObjects state
    inputsWithIncrementalOutput = subFeeds.map(s => context.instanceRegistry.get[DataObject](s.dataObjectId)).flatMap {
      case input: DataObject with CanCreateIncrementalOutput =>
        input.setState(dataObjectsState.find(_.dataObjectId == input.id).map(_.state))
        Some(input)
      case _ => None
    }
    assert(inputsWithIncrementalOutput.nonEmpty, s"DataObjectStateIncrementalMode needs at least one input DataObject implementing CanCreateIncrementalOutput")
  }
  override def postExec(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, mainInputSubFeed: SubFeed, mainOutputSubFeed: SubFeed)(implicit context: ActionPipelineContext): Unit = {
    // update DataObjects incremental state in DataObjectStateIncrementalMode if streaming
    if (context.appConfig.streaming) {
      inputsWithIncrementalOutput.foreach(i => i.setState(i.getState))
    }
  }
}

/**
 * An execution mode which just validates that partition values are given.
 * Note: For start nodes of the DAG partition values can be defined by command line, for subsequent nodes partition values are passed on from previous nodes.
 */
case class FailIfNoPartitionValuesMode() extends ExecutionMode {
  private[smartdatalake] override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    // check if partition values present
    if (subFeed.partitionValues.isEmpty && !context.appConfig.isDryRun) throw new IllegalStateException(s"($actionId) Partition values are empty for mainInput ${subFeed.dataObjectId.id}")
    // return: no change of given partition values and filter
    None
  }
}

/**
 * An execution mode which forces processing all data from it's inputs.
 */
case class ProcessAllMode() extends ExecutionMode {
  private[smartdatalake] override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    // return: reset given partition values and filter
    logger.info(s"($actionId) ProcessModeAll reset partition values")
    Some(ExecutionModeResult())
  }
}

/**
 * Execution mode to create custom partition execution mode logic.
 * Define a function which receives main input&output DataObject and returns partition values to process as Seq[Map[String,String]\]
 *
 * @param className class name implementing trait [[CustomPartitionModeLogic]]
 * @param alternativeOutputId optional alternative outputId of DataObject later in the DAG. This replaces the mainOutputId.
 *                            It can be used to ensure processing all partitions over multiple actions in case of errors.
 * @param options Options specified in the configuration for this execution mode
 */
case class CustomPartitionMode(className: String, override val alternativeOutputId: Option[DataObjectId] = None, options: Option[Map[String,String]] = None)
extends ExecutionMode with ExecutionModeWithMainInputOutput {
  private[smartdatalake] override def mainInputOutputNeeded: Boolean = alternativeOutputId.isEmpty
  private val impl = CustomCodeUtil.getClassInstanceByName[CustomPartitionModeLogic](className)
  private[smartdatalake] override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    val output = alternativeOutput.getOrElse(mainOutput)
    (mainInput, output) match {
      case (input: CanHandlePartitions, output: CanHandlePartitions) =>
        val partitionValuesOpt = impl.apply(options.getOrElse(Map()), actionId, input, output, subFeed.partitionValues.map(_.getMapString), context)
          .map(_.map( pv => PartitionValues(pv)))
        partitionValuesOpt.map(pvs => ExecutionModeResult(inputPartitionValues = pvs, outputPartitionValues = pvs))
      case (_: CanHandlePartitions, _) =>
        throw ConfigurationException(s"$actionId has set executionMode = CustomPartitionMode but ${output.id} does not support partitions!")
      case (_, _) =>
        throw ConfigurationException(s"$actionId has set executionMode = CustomPartitionMode but $mainInput does not support partitions!")
    }
  }
}
trait CustomPartitionModeLogic {
  def apply(options: Map[String,String], actionId: ActionId, input: DataObject with CanHandlePartitions, output: DataObject with CanHandlePartitions, givenPartitionValues: Seq[Map[String,String]], context: ActionPipelineContext): Option[Seq[Map[String,String]]]
}

/**
 * Execution mode to create custom execution mode logic.
 * Define a function which receives main input&output DataObject and returns execution mode result
 *
 * @param className class name implementing trait [[CustomModeLogic]]
 * @param alternativeOutputId optional alternative outputId of DataObject later in the DAG. This replaces the mainOutputId.
 *                            It can be used to ensure processing over multiple actions in case of errors.
 * @param options Options specified in the configuration for this execution mode
 */
case class CustomMode(className: String, override val alternativeOutputId: Option[DataObjectId] = None, options: Option[Map[String,String]] = None)
  extends ExecutionMode with ExecutionModeWithMainInputOutput {
  private[smartdatalake] override def mainInputOutputNeeded: Boolean = alternativeOutputId.isEmpty
  private val impl = CustomCodeUtil.getClassInstanceByName[CustomModeLogic](className)
  private[smartdatalake] override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    val output = alternativeOutput.getOrElse(mainOutput)
    impl.apply(options.getOrElse(Map()), actionId, mainInput, output, subFeed.partitionValues.map(_.getMapString), context)
  }
}
trait CustomModeLogic {
  def apply(options: Map[String,String], actionId: ActionId, input: DataObject, output: DataObject, givenPartitionValues: Seq[Map[String,String]], context: ActionPipelineContext): Option[ExecutionModeResult]
}

/**
 * Execution mode to incrementally process file-based DataObjects, e.g. FileRefDataObjects and SparkFileDataObjects.
 * For FileRefDataObjects:
 * - All existing files in the input DataObject are processed and removed (deleted or archived) after processing
 * - Input partition values are applied to search for files and also used as output partition values
 * For SparkFileDataObjects:
 * - Files processed are read from the DataFrames execution plan and removed (deleted or archived) after processing.
 *   Note that is only correct if no additional filters are applied in the DataFrame.
 *   A better implementation would be to observe files by a custom metric. Unfortunately there is a problem in Spark with that, see also [[CollectSetDeterministic]]
 * - Partition values preserved.
 * @param archivePath if an archive directory is configured, files are moved into that directory instead of deleted, preserving partition layout.
 *                    If this is a relative path, e.g. "_archive", it is appended after the path of the DataObject.
 *                    If this is an absolute path it replaces the path of the DataObject.
 */
case class FileIncrementalMoveMode(archivePath: Option[String] = None) extends ExecutionMode {
  assert(archivePath.forall(_.nonEmpty)) // empty string not allowed

  private var sparkFilesObserver: Option[FilesSparkObservation] = None

  /**
   * Check for files in input data object.
   */
  private[smartdatalake] override def apply(actionId: ActionId, mainInput: DataObject
                                            , mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues,PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    (mainInput,subFeed) match {
      case (inputDataObject: FileRefDataObject, inputSubFeed: FileSubFeed) =>
        // search FileRefs if not present from previous actions
        val fileRefs = inputSubFeed.fileRefs.getOrElse(inputDataObject.getFileRefs(inputSubFeed.partitionValues))
        // skip processing if no new data
        if (fileRefs.isEmpty) throw NoDataToProcessWarning(actionId.id,s"($actionId) No files to process found for ${inputDataObject.id}, partitionValues=${inputSubFeed.partitionValues.mkString(", ")}")
        Some(ExecutionModeResult(fileRefs = Some(fileRefs), inputPartitionValues = inputSubFeed.partitionValues, outputPartitionValues = inputSubFeed.partitionValues))
      case (inputDataObject: SparkFileDataObject, inputSubFeed: SparkSubFeed) =>
        if (!inputDataObject.checkFilesExisting) throw NoDataToProcessWarning(actionId.id, s"($actionId) No files to process found for ${mainInput.id} by FileIncrementalMoveMode.")
        // setup observation of files processed
        sparkFilesObserver = Some(inputDataObject.setupFilesObserver(actionId))
        Some(ExecutionModeResult(inputPartitionValues = inputSubFeed.partitionValues, outputPartitionValues = inputSubFeed.partitionValues))
      case _ => throw ConfigurationException(s"($actionId) FileIncrementalMoveMode needs FileRefDataObject with FileSubFeed or SparkFileDataObject with SparkSubFeed as input")
    }
  }

  /**
   * Remove/archive files after read
   */
  private[smartdatalake] override def postExec(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, mainInputSubFeed: SubFeed, mainOutputSubFeed: SubFeed)(implicit context: ActionPipelineContext): Unit = {
    (mainInput, mainOutputSubFeed) match {
      case (fileRefInput: FileRefDataObject, fileSubFeed: FileSubFeed) =>
        fileSubFeed.fileRefMapping.foreach {
          fileRefs =>
            logger.info(s"Cleaning up ${fileRefs.size} processed input files")
            val inputFiles = fileRefs.map(_.src.fullPath)
            if (archivePath.isDefined) {
              val newBasePath = if (fileRefInput.isAbsolutePath(archivePath.get)) archivePath.get
              else fileRefInput.concatPath(fileRefInput.getPath, archivePath.get)
              inputFiles.foreach{ file =>
                val archiveFile = fileRefInput.concatPath(newBasePath, fileRefInput.relativizePath(file))
                fileRefInput.renameFileHandleAlreadyExisting(file, archiveFile)
              }
            } else {
              inputFiles.foreach(file => fileRefInput.deleteFile(file))
            }
        }
      case (sparkDataObject: SparkFileDataObject, sparkSubFeed: SparkSubFeed) =>
        val files = sparkFilesObserver
          .getOrElse(throw new IllegalStateException(s"($actionId) FilesObserver not setup for ${mainInput.id}"))
          .getFilesProcessed
        if (files.isEmpty) throw NoDataToProcessWarning(actionId.id, s"($actionId) No files to process found for ${mainInput.id} by FileIncrementalMoveMode.")
        logger.info(s"Cleaning up ${files.size} processed input files")
        if (archivePath.isDefined) {
          val archiveHadoopPath = new Path(archivePath.get)
          val newBasePath = if (archiveHadoopPath.isAbsolute) archiveHadoopPath
          else new Path(sparkDataObject.hadoopPath, archiveHadoopPath)
          // create archive file names
          val filePairs = files.map(file => (file, new Path(newBasePath, sparkDataObject.relativizePath(file))))
          // create directories if not existing (otherwise hadoop rename fails)
          filePairs.map(_._2).map(_.getParent).distinct
            .foreach(p => if (!sparkDataObject.filesystem.exists(p)) sparkDataObject.filesystem.mkdirs(p))
          // rename files
          filePairs.foreach { case (file,newFile) =>
            sparkDataObject.renameFileHandleAlreadyExisting(file, newFile.toString)
          }
        } else {
          files.foreach(sparkDataObject.deleteFile)
        }
      case _ => throw ConfigurationException(s"($actionId) FileIncrementalMoveMode needs FileRefDataObject with FileSubFeed or SparkFileDataObject with SparkSubFeed as input")
    }
  }

}

/**
 * Attributes definition for spark expressions used as ExecutionMode conditions.
 * @param givenPartitionValues Partition values specified with command line (start action) or passed from previous action
 * @param isStartNode True if the current action is a start node of the DAG.
 */
case class DefaultExecutionModeExpressionData( feed: String, application: String, runId: Int, attemptId: Int, referenceTimestamp: Option[Timestamp]
                                             , runStartTime: Timestamp, attemptStartTime: Timestamp
                                             , givenPartitionValues: Seq[Map[String,String]], isStartNode: Boolean) {
  override def toString: String = ProductUtil.formatObj(this)
}
private[smartdatalake] object DefaultExecutionModeExpressionData {
  def from(context: ActionPipelineContext): DefaultExecutionModeExpressionData = {
    DefaultExecutionModeExpressionData(context.feed, context.application, context.executionId.runId, context.executionId.attemptId, context.referenceTimestamp.map(Timestamp.valueOf)
      , Timestamp.valueOf(context.runStartTime), Timestamp.valueOf(context.attemptStartTime), Seq(), isStartNode = false)
  }
}

private[smartdatalake] case class ExecutionModeFailedException(id: NodeId, phase: ExecutionPhase, msg: String) extends DAGException(msg) {
  // don't fail in init phase, but skip action to continue with exec phase
  override val severity: ExceptionSeverity = if (phase == ExecutionPhase.Init) ExceptionSeverity.FAILED_DONT_STOP else ExceptionSeverity.FAILED
  override def getDAGRootExceptions: Seq[DAGException] = Seq(this)
}
