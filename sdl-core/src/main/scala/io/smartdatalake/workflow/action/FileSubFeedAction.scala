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
import io.smartdatalake.definitions.{Environment, ExecutionMode, SDLSaveMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.PerformanceUtils
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Try

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
   * Recursive Inputs on FileSubFeeds are not supported so empty Seq is set.
   *  @return
   */
  override def recursiveInputs: Seq[FileRefDataObject with CanCreateInputStream] = Seq()

  /**
   * "Transforms" a given [[FileSubFeed]]
   * Note usage of doExec to choose between initialization or actual execution.
   *
   * @param inputSubFeed subFeed to be processed (referencing files to be read)
   * @param outputSubFeed prepared output subFeed
   * @param doExec true if action should be executed. If false this only checks the prerequisits to do the processing and simulates the output FileRef's that would be created.
   * @return processed output subFeed (referencing files written by this action)
   */
  def doTransform(inputSubFeed: FileSubFeed, outputSubFeed: FileSubFeed, doExec: Boolean)(implicit session: SparkSession, context: ActionPipelineContext): FileSubFeed

  override def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.prepare
    executionMode.foreach(_.prepare(id))
    // make sure all output partitions exist in input
    val unknownPartitions = output.partitions.diff(input.partitions :+ Environment.runIdPartitionColumnName)
    if (unknownPartitions.nonEmpty) throw ConfigurationException(s"($id) Partition columns ${unknownPartitions.mkString(", ")} not found in input")
    // check for unsupported save mode
    assert(output.saveMode!=SDLSaveMode.OverwritePreserveDirectories, s"($id) saveMode OverwritePreserveDirectories not supported for now.")
    assert(output.saveMode!=SDLSaveMode.OverwriteOptimized, s"($id) saveMode OverwriteOptimized not supported for now.")
  }

  private def prepareSubFeed(subFeed: SubFeed)(implicit session: SparkSession, context: ActionPipelineContext): (FileSubFeed,FileSubFeed) = {
    // convert subfeeds to FileSubFeed type or initialize if not yet existing
    var inputSubFeed = ActionHelper.updateInputPartitionValues(input, FileSubFeed.fromSubFeed(subFeed))
    // create output subfeed
    var outputSubFeed = ActionHelper.updateOutputPartitionValues(output, inputSubFeed.toOutput(output.id))
    // (re-)apply execution mode in init phase, streaming iteration or if not first action in pipeline (search for calls to resetExecutionResults for details)
    if (executionModeResult.isEmpty) applyExecutionMode(input, output, inputSubFeed, PartitionValues.oneToOneMapping)
    // apply execution mode result
    executionModeResult.get.get match { // throws exception if execution mode is Failure
      case Some(result) =>
        inputSubFeed = inputSubFeed.copy(partitionValues = result.inputPartitionValues, fileRefs = result.fileRefs, isSkipped = false).breakLineage
        outputSubFeed = outputSubFeed.copy(partitionValues = result.outputPartitionValues).breakLineage
      case _ => Unit
    }
    outputSubFeed = ActionHelper.addRunIdPartitionIfNeeded(output, outputSubFeed)
    // validate partition values existing for input
    if (inputSubFeed.partitionValues.nonEmpty && (context.phase==ExecutionPhase.Exec || inputSubFeed.isDAGStart)) {
      val expectedPartitions = input.filterExpectedPartitionValues(inputSubFeed.partitionValues)
      val missingPartitionValues = if (expectedPartitions.nonEmpty) PartitionValues.checkExpectedPartitionValues(input.listPartitions, expectedPartitions) else Seq()
      assert(missingPartitionValues.isEmpty, s"($id) partitions $missingPartitionValues missing for ${input.id}")
    }
    // update fileRefs
    if (inputSubFeed.fileRefs.isEmpty) {
      val fileRefs = inputSubFeed.fileRefs.getOrElse(input.getFileRefs(inputSubFeed.partitionValues))
      inputSubFeed = inputSubFeed.copy(fileRefs = Some(fileRefs))
    }
    // break lineage if requested
    if (breakFileRefLineage) inputSubFeed = inputSubFeed.breakLineage
    // return
    (inputSubFeed,outputSubFeed)
  }

  /**
   * Action.init implementation
   */
  override final def init(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == 1, s"Only one subfeed allowed for FileSubFeedAction (Action $id, inputSubfeed's ${subFeeds.map(_.dataObjectId).mkString(",")}")
    val (inputSubFeed, outputSubFeed) = prepareSubFeed(subFeeds.head)
    // transform (file transformation is limited to initialization in init phase)
    val transformedSubFeed = doTransform(inputSubFeed, outputSubFeed, doExec = false)
    // update subFeed
    Seq(transformedSubFeed)
  }

  /**
   * Action.exec implementation
   */
  override final def exec(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = {
    assert(subFeeds.size == 1, s"Only one subfeed allowed for FileSubFeedActions (Action $id, inputSubfeed's ${subFeeds.map(_.dataObjectId).mkString(",")})")
    var (inputSubFeed,outputSubFeed) = prepareSubFeed(subFeeds.head)
    // delete existing files on overwrite
    if (output.saveMode == SDLSaveMode.Overwrite) {
      if (output.partitions.nonEmpty)
        if (outputSubFeed.partitionValues.nonEmpty) output.deletePartitions(outputSubFeed.partitionValues)
        else logger.warn(s"($id) Cannot delete data from partitioned data object ${output.id} as no partition values are given but saveMode=overwrite")
      else output.deleteAll
    }
    // transform
    logger.info( s"($id) start writing files to ${output.id}" + (if (outputSubFeed.partitionValues.nonEmpty) s", partitionValues ${outputSubFeed.partitionValues.mkString(" ")}" else ""))
    val (transformedSubFeed,d) = PerformanceUtils.measureDuration {
      doTransform(inputSubFeed, outputSubFeed, doExec = true)
    }
    val filesWritten = transformedSubFeed.fileRefs.get.size.toLong
    logger.info(s"($id) finished writing files to ${output.id}: duration=$d files_written=$filesWritten")
    // make sure empty partitions are created as well
    output.createMissingPartitions(outputSubFeed.partitionValues)
    // send metric to action (for file subfeeds this has to be done manually while spark subfeeds get's the metrics via spark events listener)
    addRuntimeMetrics(Some(context.executionId), Some(output.id), GenericMetrics(s"$id-${output.id}", 1, Map("duration"->d, "files_written"->filesWritten)))
    // update subFeed
    Seq(transformedSubFeed)
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
    executionMode.foreach(_.postExec(id, input, output, inputSubFeed, outputSubFeed))
  }

  /**
   * Stop propagating input FileRefs through action and instead get new FileRefs from DataObject according to the SubFeed's partitionValue.
   * This is needed to reprocess all files of a path/partition instead of the FileRef's passed from the previous Action.
   */
  def breakFileRefLineage: Boolean

  /**
   * If true delete files after they are successfully processed.
   */
  def deleteDataAfterRead(): Boolean

}
