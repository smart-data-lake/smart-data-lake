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
import io.smartdatalake.definitions.{Environment, SDLSaveMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.PerformanceUtils
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.dataobject._
import org.apache.spark.sql.SparkSession

/**
 * Implementation of logic needed to use FileSubFeeds with only one input and one output SubFeed.
 */
abstract class FileOneToOneActionImpl extends ActionSubFeedsImpl[FileSubFeed] {

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
   */
  override def recursiveInputs: Seq[FileRefDataObject with CanCreateInputStream] = Seq()

  /**
   * Stop propagating input FileRefs through action and instead get new FileRefs from DataObject according to the SubFeed's partitionValue.
   * This is needed to reprocess all files of a path/partition instead of the FileRef's passed from the previous Action.
   */
  def breakFileRefLineage: Boolean

  override def validateConfig(): Unit = {
    super.validateConfig()
    // make sure all output partitions exist in input
    val unknownPartitions = output.partitions.diff(input.partitions :+ Environment.runIdPartitionColumnName)
    if (unknownPartitions.nonEmpty) throw ConfigurationException(s"($id) Partition columns ${unknownPartitions.mkString(", ")} not found in input")
    // check for unsupported save mode
    assert(output.saveMode!=SDLSaveMode.OverwritePreserveDirectories, s"($id) saveMode OverwritePreserveDirectories not supported for now.")
    assert(output.saveMode!=SDLSaveMode.OverwriteOptimized, s"($id) saveMode OverwriteOptimized not supported for now.")
  }

  private[smartdatalake] override def subFeedConverter(): SubFeedConverter[FileSubFeed] = FileSubFeed

  /**
   * Transform a [[SparkSubFeed]].
   * To be implemented by subclasses.
   *
   * @param inputSubFeed [[SparkSubFeed]] to be transformed
   * @param outputSubFeed [[SparkSubFeed]] to be enriched with transformed result
   * @return transformed output [[SparkSubFeed]]
   */
  def transform(inputSubFeed: FileSubFeed, outputSubFeed: FileSubFeed)(implicit context: ActionPipelineContext): FileSubFeed

  override protected def transform(inputSubFeeds: Seq[FileSubFeed], outputSubFeeds: Seq[FileSubFeed])(implicit context: ActionPipelineContext): Seq[FileSubFeed] = {
    assert(inputSubFeeds.size == 1, s"($id) Only one inputSubFeed allowed")
    assert(outputSubFeeds.size == 1, s"($id) Only one outputSubFeed allowed")
    val transformedSubFeed = transform(inputSubFeeds.head, outputSubFeeds.head)
    Seq(transformedSubFeed)
  }

  override def preprocessInputSubFeedCustomized(subFeed: FileSubFeed, ignoreFilter: Boolean, isRecursive: Boolean)(implicit context: ActionPipelineContext): FileSubFeed = {
    validatePartitionValuesExisting(input, subFeed)
    // get input files
    if (subFeed.fileRefs.isEmpty || breakFileRefLineage) {
      subFeed.copy(fileRefs = Some(input.getFileRefs(subFeed.partitionValues)))
    } else subFeed
  }
}
