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

import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.PerformanceUtils
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, SparkSubFeed, SubFeed}
import org.apache.spark.sql.SparkSession

import scala.util.{Success, Try}

abstract class SparkSubFeedAction extends SparkAction {

  /**
   * Input [[DataObject]] which can CanCreateDataFrame
   */
  def input: DataObject with CanCreateDataFrame

  /**
   * Output [[DataObject]] which can CanWriteDataFrame
   */
  def output:  DataObject with CanWriteDataFrame

  /**
   * Recursive Inputs cannot be set by configuration for SparkSubFeedActions, but they are implicitly used in
   * DeduplicateAction and HistorizeAction for existing data.
   * Default is empty.
   */
  override def recursiveInputs: Seq[DataObject with CanCreateDataFrame] = Seq()

  /**
   * Transform a [[SparkSubFeed]].
   * To be implemented by subclasses.
   *
   * @param inputSubFeed [[SparkSubFeed]] to be transformed
   * @param outputSubFeed [[SparkSubFeed]] to be enriched with transformed result
   * @return transformed output [[SparkSubFeed]]
   */
  def transform(inputSubFeed: SparkSubFeed, outputSubFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed

  /**
   * Transform partition values
   */
  def transformPartitionValues(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues]

  private def doTransform(subFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    // convert subfeed to SparkSubFeed type or initialize if not yet existing
    var inputSubFeed = ActionHelper.updatePartitionValues(input, SparkSubFeed.fromSubFeed(subFeed))
      .clearFilter // subFeed filter is not passed to the next action
    // create output subfeed with transformed partition values
    var outputSubFeed = ActionHelper.updatePartitionValues(output, inputSubFeed.toOutput(output.id), Some(transformPartitionValues))
    // apply execution mode in init phase and store result
    if (context.phase == ExecutionPhase.Init) {
      executionModeResult = Try(
        executionMode.flatMap(_.apply(id, input, output, inputSubFeed, transformPartitionValues))
      ).recover{
        case ex: NoDataToProcessDontStopWarning =>
          // return empty output subfeed if "no data dont stop"
          val outputSubFeed = SparkSubFeed(dataFrame = None, dataObjectId = output.id, partitionValues = Seq())
          // update partition values to output's partition columns and update dataObjectId
          validateAndUpdateSubFeed(output, outputSubFeed)
          // rethrow exception with fake results added. The DAG will pass the fake results to further actions.
          throw ex.copy(results = Some(Seq(outputSubFeed)))
      }
    }
    executionModeResult match {
      case Success(Some((inputPartitionValues, outputPartitionValues, newFilter))) =>
        inputSubFeed = inputSubFeed.copy(partitionValues = inputPartitionValues, filter = newFilter).breakLineage
        outputSubFeed = outputSubFeed.copy(partitionValues = outputPartitionValues, filter = newFilter).breakLineage
      case _ => Unit
    }
    // prepare input SubFeed
    inputSubFeed = prepareInputSubFeed(input, inputSubFeed)
    // enrich with fresh DataFrame if needed
    inputSubFeed = enrichSubFeedDataFrame(input, inputSubFeed, context.phase)
    // transform
    outputSubFeed = transform(inputSubFeed, outputSubFeed)
    // update partition values to output's partition columns and update dataObjectId
    validateAndUpdateSubFeed(output, outputSubFeed)
  }

  /**
   * Action.init implementation
   */
  override final def init(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == 1, s"Only one subfeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${subFeeds.map(_.dataObjectId).mkString(",")})")
    val subFeed = subFeeds.head
    // transform
    val transformedSubFeed = doTransform(subFeed)
    // check output
    output.init(transformedSubFeed.dataFrame.get, transformedSubFeed.partitionValues)
    // return
    Seq(transformedSubFeed)
  }

  /**
   * Action.exec implementation
   */
  override final def exec(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == 1, s"Only one subfeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${subFeeds.map(_.dataObjectId).mkString(",")})")
    val subFeed = subFeeds.head
    // transform
    val transformedSubFeed = doTransform(subFeed)
    // write output
    val msg = s"writing to ${output.id}" + (if (transformedSubFeed.partitionValues.nonEmpty) s", partitionValues ${transformedSubFeed.partitionValues.mkString(" ")}" else "")
    logger.info(s"($id) start " + msg)
    setSparkJobMetadata(Some(msg))
    val isRecursiveInput = recursiveInputs.exists(_.id == output.id)
    val (noData,d) = PerformanceUtils.measureDuration {
      writeSubFeed(transformedSubFeed, output, isRecursiveInput)
    }
    setSparkJobMetadata()
    val metricsLog = if (noData) ", no data found"
    else getFinalMetrics(output.id).map(_.getMainInfos).map(" "+_.map( x => x._1+"="+x._2).mkString(" ")).getOrElse("")
    logger.info(s"($id) finished writing DataFrame to ${output.id}: jobDuration=$d" + metricsLog)
    // return
    Seq(transformedSubFeed)
  }

  override final def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.postExec(inputSubFeeds, outputSubFeeds)
    assert(inputSubFeeds.size == 1, s"Only one inputSubFeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${inputSubFeeds.map(_.dataObjectId).mkString(",")})")
    assert(outputSubFeeds.size == 1, s"Only one outputSubFeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${outputSubFeeds.map(_.dataObjectId).mkString(",")})")
    postExecSubFeed(inputSubFeeds.head, outputSubFeeds.head)
  }

  def postExecSubFeed(inputSubFeed: SubFeed, outputSubFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): Unit = Unit /* NOP */

}
