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
import io.smartdatalake.definitions.ExecutionModeWithMainInputOutput
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.PerformanceUtils
import io.smartdatalake.workflow.action.sparktransformer.DfsTransformer
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanReceiveScriptNotification, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ScriptSubFeed, SparkSubFeed, SubFeed}
import org.apache.spark.sql.SparkSession

abstract class ScriptSubFeedsAction extends Action {

  override def inputs: Seq[DataObject]
  override def outputs: Seq[DataObject with CanReceiveScriptNotification]

  def mainInputId: Option[DataObjectId]
  def mainOutputId: Option[DataObjectId]

  override def recursiveInputs: Seq[DataObject] = Seq()

  // prepare main input / output
  // this must be lazy because inputs / outputs is evaluated later in subclasses
  // Note: we don't yet decide for a main input as inputs might be skipped at runtime, but we can already create a prioritized list.
  lazy val prioritizedMainInputCandidates: Seq[DataObject] = ActionHelper.getMainDataObjectCandidates(mainInputId, inputs, Seq(), "input", executionModeNeedsMainInputOutput, id)
  lazy val mainOutput: DataObject with CanReceiveScriptNotification = ActionHelper.getMainDataObjectCandidates(mainOutputId, outputs, Seq(), "output", executionModeNeedsMainInputOutput, id).head
  def getMainInput(inputSubFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): DataObject = {
    // take first data object which has as SubFeed which is not skipped
    prioritizedMainInputCandidates.find(dataObject => !inputSubFeeds.find(_.dataObjectId == dataObject.id).get.isSkipped || context.appConfig.isDryRun)
      .getOrElse(prioritizedMainInputCandidates.head) // otherwise just take first candidate
  }

  override def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.prepare
    // check skip condition
    executionCondition.foreach(_.syntaxCheck[SubFeedsExpressionData](id, Some("executionCondition")))
    // check main input/output by triggering lazy values
    prioritizedMainInputCandidates
    mainOutput
  }

  def doTransform(subFeeds: Seq[SubFeed], doExec: Boolean)(implicit session: SparkSession, context: ActionPipelineContext): Seq[ScriptSubFeed] = {
    val mainInput = getMainInput(subFeeds)
    val inputMap = (inputs ++ recursiveInputs).map(i => i.id -> i).toMap
    val outputMap = outputs.map(i => i.id -> i).toMap
    // convert subfeeds to ScriptSubFeed type or initialize if not yet existing
    var inputSubFeeds = subFeeds.map( subFeed =>
      ActionHelper.updateInputPartitionValues(inputMap(subFeed.dataObjectId), ScriptSubFeed.fromSubFeed(subFeed))
    )
    val mainInputSubFeed = inputSubFeeds.find(_.dataObjectId == mainInput.id).get
    // create output subfeeds with transformed partition values from main input
    var outputSubFeeds = outputs.map(output => ActionHelper.updateOutputPartitionValues(output, mainInputSubFeed.toOutput(output.id)))
    // (re-)apply execution mode in init phase, streaming iteration or if not first action in pipeline (search for calls to resetExecutionResults for details)
    if (executionModeResult.isEmpty) applyExecutionMode(mainInput, mainOutput, mainInputSubFeed, PartitionValues.oneToOneMapping)
    // apply execution mode result
    executionModeResult.get.get match { // throws exception if execution mode is Failure
      case Some(result) =>
        inputSubFeeds = inputSubFeeds.map { subFeed =>
          ActionHelper.updateInputPartitionValues(inputMap(subFeed.dataObjectId), subFeed.copy(partitionValues = result.inputPartitionValues, isSkipped = false).breakLineage)
        }
        outputSubFeeds = outputSubFeeds.map(subFeed =>
          // we need to transform inputPartitionValues again to outputPartitionValues so that partition values from partitions not existing in mainOutput are not lost.
          ActionHelper.updateOutputPartitionValues(outputMap(subFeed.dataObjectId), subFeed.copy(partitionValues = result.inputPartitionValues).breakLineage)
        )
      case _ => Unit
    }
    outputSubFeeds = outputSubFeeds.map(subFeed => ActionHelper.addRunIdPartitionIfNeeded(outputMap(subFeed.dataObjectId), subFeed))
    //TODO: transform if doExec=true
    //TODO: update partition values to output's partition columns and update dataObjectId
    //outputSubFeeds.map { subFeed =>
    //    val output = outputMap.getOrElse(subFeed.dataObjectId, throw ConfigurationException(s"No output found for result ${subFeed.dataObjectId} in $id. Configured outputs are ${outputs.map(_.id.id).mkString(", ")}."))
    //    validateAndUpdateSubFeed(output, subFeed)
    //}
    outputSubFeeds
  }

  /**
   * Generic init implementation for Action.init
   * */
  override final def init(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == inputs.size + recursiveInputs.size, s"Number of subFeed's must match number of inputs for SparkSubFeedActions (Action $id, subfeed's ${subFeeds.map(_.dataObjectId).mkString(",")}, inputs ${inputs.map(_.id).mkString(",")})")
    // transform
    val transformedSubFeeds = doTransform(subFeeds, doExec = false)
    // check output
    outputs.foreach{
      output =>
        transformedSubFeeds.find(_.dataObjectId == output.id)
          .getOrElse(throw new IllegalStateException(s"subFeed for output ${output.id} not found"))
    }
    // return
    transformedSubFeeds
  }

  /**
   * Action.exec implementation
   */
  override final def exec(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == inputs.size + recursiveInputs.size, s"Number of subFeed's must match number of inputs for SparkSubFeedActions (Action $id, subfeed's ${subFeeds.map(_.dataObjectId).mkString(",")}, inputs ${inputs.map(_.id).mkString(",")})")
    val mainInput = getMainInput(subFeeds)
    val mainInputSubFeed = subFeeds.find(_.dataObjectId == mainInput.id).getOrElse(throw new IllegalStateException(s"subFeed for main input ${mainInput.id} not found"))
    // transform
    val transformedSubFeeds = doTransform(subFeeds, doExec = true)
    // notify outputs
    outputs.foreach { output =>
      val subFeed = transformedSubFeeds.find(_.dataObjectId == output.id).getOrElse(throw new IllegalStateException(s"subFeed for output ${output.id} not found"))
      logger.info( s"($id) start script notification to ${output.id}" + (if (subFeed.partitionValues.nonEmpty) s", partitionValues ${subFeed.partitionValues.mkString(" ")}" else ""))
      val isRecursiveInput = recursiveInputs.exists(_.id == subFeed.dataObjectId)
      val (noData, d) = PerformanceUtils.measureDuration {
        output.scriptNotification(subFeed.parameters.getOrElse(Map()))
      }
      logger.info(s"($id) finished script notification to ${output.id}: duration=$d")
    }
    // return
    transformedSubFeeds
  }

  private def executionModeNeedsMainInputOutput: Boolean = {
    executionMode.exists{_.isInstanceOf[ExecutionModeWithMainInputOutput]}
  }

  override def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.postExec(inputSubFeeds, outputSubFeeds)
    val mainInput = getMainInput(inputSubFeeds)
    val mainInputSubFeed = inputSubFeeds.find(_.dataObjectId == mainInput.id).get
    val mainOutputSubFeed = outputSubFeeds.find(_.dataObjectId == mainOutput.id).get
    executionMode.foreach(_.postExec(id, mainInput, mainOutput, mainInputSubFeed, mainOutputSubFeed))
  }
}