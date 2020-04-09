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
import io.smartdatalake.definitions.ExecutionMode
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, CanWriteDataStream,DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, InitSubFeed, SparkSubFeed, SubFeed}
import org.apache.spark.sql.SparkSession

abstract class SparkSubFeedAction extends Action {

  /**
   * Input [[DataObject]] which can CanCreateDataFrame
   */
  def input: DataObject with CanCreateDataFrame

  /**
   * Output [[DataObject]] which can CanWriteDataFrame
   */
  def output:  DataObject with CanWriteDataFrame

  /**
   * Transform a [[SparkSubFeed]].
   * To be implemented by subclasses.
   *
   * @param subFeed [[SparkSubFeed]] to be transformed
   * @return transformed [[SparkSubFeed]]
   */
  def transform(subFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed

  private def doTransform(subFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    // convert subfeed to SparkSubFeed type or initialize if not yet existing
    var preparedSubFeed = SparkSubFeed.fromSubFeed(subFeed)
    // apply init execution mode if there are no partition values given in command line
    preparedSubFeed = if (subFeed.isInstanceOf[InitSubFeed] && preparedSubFeed.partitionValues.isEmpty) {
      preparedSubFeed.copy( partitionValues = ActionHelper.applyExecutionMode(initExecutionMode, id, input, output, preparedSubFeed.partitionValues))
    } else preparedSubFeed
    // break lineage if requested
    preparedSubFeed = if (breakDataFrameLineage) preparedSubFeed.breakLineage() else preparedSubFeed
    // persist if requested
    preparedSubFeed = if (persist) preparedSubFeed.persist else preparedSubFeed
    // transform
    val transformedSubFeed = transform(preparedSubFeed)
    // update partition values to output's partition columns and update dataObjectId
    ActionHelper.validateAndUpdateSubFeedPartitionValues(output, transformedSubFeed).copy(dataObjectId = output.id)
  }

  /**
   * Action.init implementation
   */
  override final def init(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == 1, s"Only one subfeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${subFeeds.map(_.dataObjectId).mkString(",")})")
    Seq(doTransform(subFeeds.head))
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
    logger.info(s"writing to DataObject ${output.id}, partitionValues ${subFeed.partitionValues}")
    setSparkJobDescription( s"writing to DataObject ${output.id}" )

    // Write from a streaming input
    if(transformedSubFeed.dataFrame.get.isStreaming){
      assert(output.isInstanceOf[CanWriteDataStream], s"Output type ${output.getClass.getSimpleName} does not support streaming inputs")
      output.asInstanceOf[CanWriteDataStream].writeDataStream(transformedSubFeed.dataFrame.get, transformedSubFeed.partitionValues)
    // Write from a batch input
    } else {
      output.writeDataFrame(transformedSubFeed.dataFrame.get, transformedSubFeed.partitionValues)
    }

    // return
    Seq(transformedSubFeed)
  }

  override final def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    assert(inputSubFeeds.size == 1, s"Only one inputSubFeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${inputSubFeeds.map(_.dataObjectId).mkString(",")})")
    assert(outputSubFeeds.size == 1, s"Only one outputSubFeed allowed for SparkSubFeedActions (Action $id, inputSubfeed's ${outputSubFeeds.map(_.dataObjectId).mkString(",")})")
    postExecSubFeed(inputSubFeeds.head, outputSubFeeds.head)
  }

  def postExecSubFeed(inputSubFeed: SubFeed, outputSubFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): Unit = Unit /* NOP */

  /**
   * Stop propagating input DataFrame through action and instead get a new DataFrame from DataObject.
   * This can help to save memory and performance if the input DataFrame includes many transformations from previous Actions.
   * The new DataFrame will be initialized according to the SubFeed's partitionValues.
   */
  def breakDataFrameLineage: Boolean

  /**
   * Force persisting DataFrame on Disk.
   * This helps to reduce memory needed for caching the DataFrame content and can serve as a recovery point in case an task get's lost.
   */
  def persist: Boolean

  /**
   * Execution mode if this Action is a start node of a DAG run
   */
  def initExecutionMode: Option[ExecutionMode]

}
