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
import io.smartdatalake.util.misc.PerformanceUtils
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed, SubFeed}
import org.apache.spark.sql.SparkSession

import scala.util.Try

abstract class SparkSubFeedsAction extends SparkAction {

  override def inputs: Seq[DataObject with CanCreateDataFrame]
  override def outputs: Seq[DataObject with CanWriteDataFrame]
  override def recursiveInputs: Seq[DataObject with CanCreateDataFrame] = Seq()

  def mainInputId: Option[DataObjectId]
  def mainOutputId: Option[DataObjectId]

  def inputIdsToIgnoreFilter: Seq[DataObjectId]

  // prepare main input / output
  // this must be lazy because inputs / outputs is evaluated later in subclasses
  lazy val mainInput: DataObject with CanCreateDataFrame = ActionHelper.getMainDataObject[DataObject with CanCreateDataFrame](mainInputId, inputs, "input", executionModeNeedsMainInputOutput, id)
  lazy val mainOutput: DataObject with CanWriteDataFrame = ActionHelper.getMainDataObject[DataObject with CanWriteDataFrame](mainOutputId, outputs, "output", executionModeNeedsMainInputOutput, id)

  /**
   * Transform [[SparkSubFeed]]'s.
   * To be implemented by subclasses.
   *
   * @param subFeeds [[SparkSubFeed]]'s to be transformed
   * @return transformed [[SparkSubFeed]]'s
   */
  def transform(subFeeds: Seq[SparkSubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SparkSubFeed]

  private def doTransform(subFeeds: Seq[SparkSubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SparkSubFeed] = {
    // apply execution mode
    var preparedSubFeeds = executionModeResult.get match {
      case Some((newPartitionValues, newFilter)) =>
        subFeeds.map( subFeed => subFeed.copy(partitionValues = newPartitionValues, filter = newFilter))
      case None => subFeeds
    }
    preparedSubFeeds = preparedSubFeeds.map{ subFeed =>
      val input = (inputs ++ recursiveInputs).find(_.id == subFeed.dataObjectId).get
      val ignoreFilter = inputIdsToIgnoreFilter.contains(input.id)
      // prepare as input SubFeed
      val preparedSubFeed = prepareInputSubFeed(subFeed, input, ignoreFilter)
      // enrich with fresh DataFrame if needed
      enrichSubFeedDataFrame(input, preparedSubFeed, context.phase)
    }
    // transform
    val transformedSubFeeds = transform(preparedSubFeeds)
    // update partition values to output's partition columns and update dataObjectId
    transformedSubFeeds.map {
      subFeed =>
        val output = outputs.find(_.id == subFeed.dataObjectId)
          .getOrElse(throw ConfigurationException(s"No output found for result ${subFeed.dataObjectId} in $id. Configured outputs are ${outputs.map(_.id.id).mkString(", ")}."))
        validateAndUpdateSubFeedPartitionValues(output, subFeed)
    }
  }

  /**
   * Generic init implementation for Action.init
   * */
  override final def init(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == inputs.size + recursiveInputs.size, s"Number of subFeed's must match number of inputs for SparkSubFeedActions (Action $id, subfeed's ${subFeeds.map(_.dataObjectId).mkString(",")}, inputs ${inputs.map(_.id).mkString(",")})")
    // convert subfeeds to SparkSubFeed type or initialize if not yet existing
    val preparedSubFeeds = subFeeds.map( SparkSubFeed.fromSubFeed )
    val mainInputSubFeed = subFeeds.find(_.dataObjectId == mainInput.id).getOrElse(throw new IllegalStateException(s"subFeed for main input ${mainInput.id} not found"))
    // evaluate execution mode and store result
    executionModeResult = Try(
      executionMode.flatMap(_.apply(id, mainInput, mainOutput, mainInputSubFeed))
    ).recover {
      case ex: NoDataToProcessDontStopWarning =>
        // return empty output subfeeds if "no data dont stop"
        val outputSubFeeds = outputs.map {
          output =>
            val subFeed = SparkSubFeed(dataFrame = None, dataObjectId = output.id, partitionValues = Seq())
            // update partition values to output's partition columns and update dataObjectId
            validateAndUpdateSubFeedPartitionValues(output, subFeed)
        }
        // rethrow exception with fake results added. The DAG will pass the fake results to further actions.
        throw ex.copy(results = Some(outputSubFeeds))
    }
    // transform
    val transformedSubFeeds = doTransform(preparedSubFeeds)
    // check output
    outputs.foreach{
      output =>
        val subFeed = transformedSubFeeds.find(_.dataObjectId == output.id).getOrElse(throw new IllegalStateException(s"subFeed for output ${output.id} not found"))
        output.init(subFeed.dataFrame.get, subFeed.partitionValues)
    }
    // return
    transformedSubFeeds.map( transformedSubFeed => updateSubFeedAfterWrite(transformedSubFeed))
  }

  /**
   * Action.exec implementation
   */
  override final def exec(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == inputs.size + recursiveInputs.size, s"Number of subFeed's must match number of inputs for SparkSubFeedActions (Action $id, subfeed's ${subFeeds.map(_.dataObjectId).mkString(",")}, inputs ${inputs.map(_.id).mkString(",")})")
    // convert subfeeds to SparkSubFeed type or initialize if not yet existing
    val preparedSubFeeds = subFeeds.map( SparkSubFeed.fromSubFeed )
    // transform
    val transformedSubFeeds = doTransform(preparedSubFeeds)
    // write output
    outputs.foreach { output =>
      val subFeed = transformedSubFeeds.find(_.dataObjectId == output.id).getOrElse(throw new IllegalStateException(s"subFeed for output ${output.id} not found"))
      val msg = s"writing DataFrame to ${output.id}" + (if (subFeed.partitionValues.nonEmpty) s", partitionValues ${subFeed.partitionValues.mkString(" ")}" else "")
      logger.info(s"($id) start " + msg)
      setSparkJobMetadata(Some(msg))
      val isRecursiveInput = recursiveInputs.exists(_.id == subFeed.dataObjectId)
      val (noData,d) = PerformanceUtils.measureDuration {
        writeSubFeed(subFeed, output, isRecursiveInput)
      }
      setSparkJobMetadata()
      val metricsLog = if (noData) ", no data found"
      else getFinalMetrics(output.id).map(_.getMainInfos).map(" "+_.map( x => x._1+"="+x._2).mkString(" ")).getOrElse("")
      logger.info(s"($id) finished writing DataFrame to ${output.id}: jobDuration=$d" + metricsLog)
    }
    // return
    transformedSubFeeds.map( transformedSubFeed => updateSubFeedAfterWrite(transformedSubFeed))
  }

  private def executionModeNeedsMainInputOutput: Boolean = {
    executionMode.exists{_.isInstanceOf[ExecutionModeWithMainInputOutput]}
  }
}
