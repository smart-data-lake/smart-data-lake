/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.file.{CanCreateInputStream, CanCreateOutputStream, FileRef, FileRefDataObject, HadoopFileDataObject}
import org.apache.hadoop.fs.Path

import scala.util.matching.Regex

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
   * If set to true, file references passed on from previous action are ignored by this action. and instead get new FileRefs from DataObject according to the SubFeed's partitionValue.
   * This is needed to reprocess all files of a path/partition instead of the FileRef's passed from the previous Action.
   * Default is false.
   */
  def breakFileRefLineage: Boolean = false

  /**
   * If set to true, this action might transform partition values of the input into different partition values of the output.
   * Then output partition columns are not required to exist in the input, and must be validated at runtime instead.
   * Default is false.
   */
  protected def transformsPartitionValues: Boolean = false

  override def validateConfig(): Unit = {
    super.validateConfig()
    // make sure all output partitions exist in input
    if (!transformsPartitionValues) {
      val unknownPartitions = output.partitions.diff(input.partitions :+ Environment.runIdPartitionColumnName)
      if (unknownPartitions.nonEmpty) throw ConfigurationException(s"($id) Partition columns ${unknownPartitions.mkString(", ")} not found in input")
    }
    // check for unsupported save mode
    assert(output.saveMode!=SDLSaveMode.OverwritePreserveDirectories, s"($id) saveMode OverwritePreserveDirectories not supported for now.")
    assert(output.saveMode!=SDLSaveMode.OverwriteOptimized, s"($id) saveMode OverwriteOptimized not supported for now.")
  }

  override def subFeedConverter: SubFeedConverter[FileSubFeed] = FileSubFeed

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

  /**
   * Create target file references for the given input files.
   * The partition values of the input files are transformed with [[transformPartitionValues]], and it is validated
   * that the resulting partition values contain all partition columns of the output.
   */
  protected def translateFileRefs(fileRefs: Seq[FileRef], executionModeResultOptions: Map[String, String], filenameExtractorRegex: Option[Regex] = None)
                                 (implicit context: ActionPipelineContext): Seq[FileRefMapping] = {
    val partitionValuesMapping = transformPartitionValues(fileRefs.map(_.partitionValues).distinct, executionModeResultOptions)
    // validate output partition values before creating target paths
    val outputPartitions = output.partitions.diff(Seq(Environment.runIdPartitionColumnName))
    partitionValuesMapping.values.toSeq.distinct.foreach { pv =>
      val missingPartitions = outputPartitions.diff(pv.keys.toSeq)
      if (missingPartitions.nonEmpty) throw new IllegalStateException(s"($id) Partition columns ${missingPartitions.mkString(", ")} of ${output.id} not found in partition values $pv." +
        " Output partition columns must exist in input or be created by transformPartitionValues of a transformer.")
    }
    fileRefs.map { src =>
      val translatedFileRef = output.translateFileRefs(Seq(src.copy(partitionValues = partitionValuesMapping(src.partitionValues))), filenameExtractorRegex).head
      translatedFileRef.copy(src = src)
    }
  }

  /**
   * Check if the input file to create a sample file from exists.
   * In init phase the FileRefs passed on from a previous file action are only a prediction, one file per input file
   * with the default file name, as no files are written in init phase and a file transformer might create other files.
   * Sample file creation is then skipped. The check is only possible for Hadoop inputs, other inputs are assumed to exist.
   */
  protected def sampleInputFileExists(fileRef: FileRef)(implicit context: ActionPipelineContext): Boolean = {
    val exists = input match {
      case hadoopInput: HadoopFileDataObject => hadoopInput.filesystem.exists(new Path(fileRef.fullPath))
      case _ => true
    }
    if (!exists) logger.info(s"($id) skipping creation of sample file, as input file ${fileRef.fullPath} does not exist yet")
    exists
  }

  override def preprocessInputSubFeedCustomized(subFeed: FileSubFeed, ignoreFilter: Boolean, isRecursive: Boolean)(implicit context: ActionPipelineContext): FileSubFeed = {
    validatePartitionValuesExisting(input, subFeed)
    // get input files
    if (subFeed.fileRefs.isEmpty || breakFileRefLineage) {
      subFeed.copy(fileRefs = Some(input.getFileRefs(subFeed.partitionValues)))
    } else subFeed
  }
}
