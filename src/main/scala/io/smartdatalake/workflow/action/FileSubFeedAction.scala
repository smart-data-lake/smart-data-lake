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
import io.smartdatalake.util.misc.PerformanceUtils
import io.smartdatalake.workflow.dataobject.{CanCreateInputStream, CanCreateOutputStream, CanHandlePartitions, FileRefDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, FileSubFeed, InitSubFeed, SparkSubFeed, SubFeed}
import org.apache.spark.sql.{SaveMode, SparkSession}

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
   * Initialize Action with a given [[FileSubFeed]]
   * Note that this only checks the prerequisits to do the processing and simulates the output FileRef's that would be created.
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
    preparedSubFeed = if (initExecutionMode.isDefined && preparedSubFeed.isDAGStart && preparedSubFeed.partitionValues.isEmpty) {
      val (newPartitionValues,_) = ActionHelper.applyExecutionMode(initExecutionMode.get, id, input, output, context.phase, preparedSubFeed.partitionValues, None)
      preparedSubFeed.copy(partitionValues = newPartitionValues)
    } else preparedSubFeed
    // break lineage if requested
    preparedSubFeed = if (breakFileRefLineage) preparedSubFeed.breakLineage else preparedSubFeed
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
    preparedSubFeed = if (initExecutionMode.isDefined && preparedSubFeed.isDAGStart && preparedSubFeed.partitionValues.isEmpty) {
      val (newPartitionValues,_) = ActionHelper.applyExecutionMode(initExecutionMode.get, id, input, output, context.phase, preparedSubFeed.partitionValues, None)
      preparedSubFeed.copy(partitionValues = newPartitionValues)
    } else preparedSubFeed
    // break lineage if requested
    preparedSubFeed = if (breakFileRefLineage) preparedSubFeed.breakLineage else preparedSubFeed
    // delete existing files on overwrite
    if (output.saveMode == SaveMode.Overwrite) {
      if (output.partitions.nonEmpty)
        if (subFeed.partitionValues.nonEmpty) output.deletePartitions(subFeed.partitionValues)
        else logger.warn(s"($id) Cannot delete data from partitioned data object ${output.id} as no partition values are given but saveMode=overwrite")
      else output.deleteAll
    }
    // transform
    logger.info( s"($id) start writing files to ${output.id}" + (if (preparedSubFeed.partitionValues.nonEmpty) s", partitionValues ${preparedSubFeed.partitionValues.mkString(" ")}" else ""))
    val (transformedSubFeed,d) = PerformanceUtils.measureDuration {
      execSubFeed(preparedSubFeed)
    }
    logger.info(s"($id) finished writing files to ${output.id}, took $d")
    // update partition values to output's partition columns and update dataObjectId
    Seq(transformedSubFeed.updatePartitionValues(output.partitions).copy(dataObjectId = output.id))
  }

  override final def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.postExec(inputSubFeeds,outputSubFeeds)
    assert(inputSubFeeds.size == 1, s"Only one inputSubFeed allowed for FileSubFeedAction (Action $id, inputSubfeed's ${inputSubFeeds.map(_.dataObjectId).mkString(",")})")
    assert(outputSubFeeds.size == 1, s"Only one outputSubFeed allowed for FileSubFeedAction (Action $id, inputSubfeed's ${outputSubFeeds.map(_.dataObjectId).mkString(",")})")
    postExecSubFeed(inputSubFeeds.head, outputSubFeeds.head)
  }

  def postExecSubFeed(inputSubFeed: SubFeed, outputSubFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    // delete Input Files if desired
    if (deleteDataAfterRead()) (input, outputSubFeed) match {
      case (fileRefInput: FileRefDataObject, fileSubFeed: FileSubFeed) =>
        fileSubFeed.processedInputFileRefs.foreach(fileRefs => fileRefInput.deleteFileRefs(fileRefs))
      case x => throw new IllegalStateException(s"Unmatched case $x")
    }
  }

  /**
   * Stop propagating input FileRefs through action and instead get new FileRefs from DataObject according to the SubFeed's partitionValue.
   * This is needed to reprocess all files of a path/partition instead of the FileRef's passed from the previous Action.
   */
  def breakFileRefLineage: Boolean

  /**
   * Execution mode if this Action is a start node of a DAG run
   */
  def initExecutionMode: Option[ExecutionMode]

  /**
   * If true delete files after they are successfully processed.
   */
  def deleteDataAfterRead(): Boolean

}
