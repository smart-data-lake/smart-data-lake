/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.executionMode

import com.typesafe.config.Config
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.definitions.Condition
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.ProductUtil
import io.smartdatalake.util.spark.SparkExpressionUtil
import io.smartdatalake.workflow.action.ActionHelper.searchCommonInits
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataobject.{CanHandlePartitions, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SubFeed}

import java.sql.Timestamp

/**
 * Partition difference execution mode lists partitions on mainInput & mainOutput DataObject and starts loading all missing partitions.
 * Partition columns to be used for comparision need to be a common 'init' of input and output partition columns.
 * This mode needs mainInput/Output DataObjects which CanHandlePartitions to list partitions.
 * Partition values are passed to following actions for partition columns which they have in common.
 *
 * @param partitionColNb                  optional number of partition columns to use as a common 'init'.
 * @param alternativeOutputId             optional alternative outputId of DataObject later in the DAG. This replaces the mainOutputId.
 *                                        It can be used to ensure processing all partitions over multiple actions in case of errors.
 * @param nbOfPartitionValuesPerRun       optional restriction of the number of partition values per run.
 * @param applyCondition                  Condition to decide if execution mode should be applied or not. Define a spark sql expression working with attributes of [[DefaultExecutionModeExpressionData]] returning a boolean.
 *                                        Default is to apply the execution mode if given partition values (partition values from command line or passed from previous action) are empty.
 * @param failConditions                  List of conditions to fail application of execution mode if true. Define as spark sql expressions working with attributes of [[PartitionDiffModeExpressionData]] returning a boolean.
 *                                        Default is that the application of the PartitionDiffMode does not fail the action. If there is no data to process, the following actions are skipped.
 *                                        Multiple conditions are evaluated individually and every condition may fail the execution mode (or-logic)
 * @param selectExpression                optional expression to define or refine the list of selected output partitions. Define a spark sql expression working with the attributes of [[PartitionDiffModeExpressionData]] returning a list<map<string,string>>.
 *                                        Default is to return the originally selected output partitions found in attribute selectedOutputPartitionValues.
 * @param applyPartitionValuesTransform   If true applies the partition values transform of custom transformations on input partition values before comparison with output partition values.
 *                                        If enabled input and output partition columns can be different. Default is to disable the transformation of partition values.
 * @param selectAdditionalInputExpression optional expression to refine the list of selected input partitions. Note that primarily output partitions are selected by PartitionDiffMode.
 *                                        The selected output partitions are then transformed back to the input partitions needed to create the selected output partitions. This is one-to-one except if applyPartitionValuesTransform=true.
 *                                        And sometimes there is a need for additional input data to create the output partitions, e.g. if you aggregate a window of 7 days for every day.
 *                                        You can customize selected input partitions by defining a spark sql expression working with the attributes of [[PartitionDiffModeExpressionData]] returning a list<map<string,string>>.
 *                                        Default is to return the originally selected input partitions found in attribute selectedInputPartitionValues.
 */
case class PartitionDiffMode(partitionColNb: Option[Int] = None
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
    selectExpression.foreach(expression => SparkExpressionUtil.syntaxCheck[PartitionDiffModeExpressionData, Seq[Map[String, String]]](actionId, Some("selectExpression"), expression))
    // validate select additional input expression
    selectAdditionalInputExpression.foreach(expression => SparkExpressionUtil.syntaxCheck[PartitionDiffModeExpressionData, Seq[Map[String, String]]](actionId, Some("selectAdditionalInputExpression"), expression))
    // check alternativeOutput exists
    alternativeOutput
  }

  private[smartdatalake] override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues, PartitionValues])
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
              val outputInputPartitionValuesMap = inputOutputPartitionValuesMap.toSeq.map { case (k, v) => (v, k) }.groupBy(_._1).mapValues(_.map(_._2))
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
  override def factory: FromConfigFactory[ExecutionMode] = PartitionDiffMode
}

object PartitionDiffMode extends FromConfigFactory[ExecutionMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): PartitionDiffMode = {
    extract[PartitionDiffMode](config)
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
