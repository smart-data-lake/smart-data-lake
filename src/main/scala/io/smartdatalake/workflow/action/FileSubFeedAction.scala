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

import io.smartdatalake.definitions.ExecutionMode
import io.smartdatalake.workflow.dataobject.{CanCreateInputStream, CanCreateOutputStream, FileRefDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, FileSubFeed, InitSubFeed, SubFeed}
import org.apache.spark.sql.SparkSession

abstract class FileSubFeedAction extends Action {

  /**
   * Input [[FileRefDataObject]] which can CanCreateInputStream
   */
  def input: FileRefDataObject with CanCreateInputStream

  /**
   * Output [[FileRefDataObject]] which can CanCreateOutputStream
   */
  def output:  FileRefDataObject with CanCreateOutputStream

  /**
   * Initialize Action with for a given [[FileSubFeed]]
   * Note that is only checks the prerequisits to do the processing and simulates the output FileRef's that would be created.
   *
   * @param subFeed subFeed to be processed (referencing files to be read)
   * @return processed subFeed (referencing files that would be written by this action)
   */
  def initSubFeed(subFeed: FileSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): FileSubFeed

  /**
   * Executes Action for a given [[FileSubFeed]]
   *
   * @param subFeed subFeed to be processed (referencing files to be read)
   * @return processed subFeed (referencing files written by this action)
   */
  def execSubFeed(subFeed: FileSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): FileSubFeed

  /**
   * Action.init implementation
   */
  override final def init(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == 1, s"Only one subfeed allowed for FileSubFeedAction (Action $id, inputSubfeed's ${subFeeds.map(_.dataObjectId).mkString(",")}")
    val subFeed = subFeeds.head
    // convert subfeeds to FileSubFeed type or initialize if not yet existing
    var preparedSubFeed = FileSubFeed.fromSubFeed(subFeed)
    // apply init execution mode if there are no partition values given in command line
    preparedSubFeed = if (subFeed.isInstanceOf[InitSubFeed] && preparedSubFeed.partitionValues.isEmpty) {
      preparedSubFeed.copy( partitionValues = ActionHelper.applyExecutionMode(initExecutionMode, id, input, output, preparedSubFeed.partitionValues))
    } else preparedSubFeed
    // break lineage if requested
    preparedSubFeed = if (breakFileRefLineage) preparedSubFeed.breakLineage() else preparedSubFeed
    // transform
    val transformedSubFeed = initSubFeed(preparedSubFeed)
    // update partition values to output's partition columns and update dataObjectId
    Seq(transformedSubFeed.updatePartitionValues(output.partitions).copy(dataObjectId = output.id))
  }

  /**
   * Action.exec implementation
   */
  override final def exec(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == 1, s"Only one subfeed allowed for FileSubFeedActions (Action $id, inputSubfeed's ${subFeeds.map(_.dataObjectId).mkString(",")})")
    val subFeed = subFeeds.head
    // convert subfeeds to FileSubFeed type or initialize if not yet existing
    var preparedSubFeed = FileSubFeed.fromSubFeed(subFeed)
    // apply init execution mode if there are no partition values given in command line
    preparedSubFeed = if (subFeed.isInstanceOf[InitSubFeed] && preparedSubFeed.partitionValues.isEmpty) {
      preparedSubFeed.copy( partitionValues = ActionHelper.applyExecutionMode(initExecutionMode, id, input, output, preparedSubFeed.partitionValues))
    } else preparedSubFeed
    // break lineage if requested
    preparedSubFeed = if (breakFileRefLineage) preparedSubFeed.breakLineage() else preparedSubFeed
    // transform
    val transformedSubFeed = execSubFeed(preparedSubFeed)
    // update partition values to output's partition columns and update dataObjectId
    Seq(transformedSubFeed.updatePartitionValues(output.partitions).copy(dataObjectId = output.id))
  }

  override final def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    assert(inputSubFeeds.size == 1, s"Only one inputSubFeed allowed for FileSubFeedAction (Action $id, inputSubfeed's ${inputSubFeeds.map(_.dataObjectId).mkString(",")})")
    assert(outputSubFeeds.size == 1, s"Only one outputSubFeed allowed for FileSubFeedAction (Action $id, inputSubfeed's ${outputSubFeeds.map(_.dataObjectId).mkString(",")})")
    postExecSubFeed(inputSubFeeds.head, outputSubFeeds.head)
  }

  def postExecSubFeed(inputSubFeed: SubFeed, outputSubFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): Unit


  /**
   * Stop propagating input FileRefs through action and instead get new FileRefs from DataObject according to the SubFeed's partitionValue.
   * This is needed to reprocess all files of a path/partition instead of the FileRef's passed from the previous Action.
   */
  def breakFileRefLineage: Boolean

  /**
   * Execution mode if this Action is a start node of a DAG run
   */
  def initExecutionMode: Option[ExecutionMode]

}
