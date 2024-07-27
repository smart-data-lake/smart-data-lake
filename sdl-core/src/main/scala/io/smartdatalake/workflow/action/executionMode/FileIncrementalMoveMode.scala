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
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.{DataObject, FileRefDataObject, SparkFileDataObject, SparkFilenameObservation}
import io.smartdatalake.workflow.{ActionPipelineContext, FileSubFeed, SubFeed}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row

/**
 * Execution mode to incrementally process file-based DataObjects, e.g. FileRefDataObjects and SparkFileDataObjects.
 * For FileRefDataObjects:
 * - All existing files in the input DataObject are processed and removed (deleted or archived) after processing
 * - Input partition values are applied to search for files and also used as output partition values
 * For SparkFileDataObjects:
 * - Files processed are read from the DataFrames execution plan and removed (deleted or archived) after processing.
 * Note that is only correct if no additional filters are applied in the DataFrame.
 * A better implementation would be to observe files by a custom metric. Unfortunately there is a problem in Spark with that, see also [[CollectSetDeterministic]]
 * - Partition values preserved.
 *
 * @param archivePath if an archive directory is configured, files are moved into that directory instead of deleted, preserving partition layout. See also `archiveInsidePartition` option.
 *                    If this is a relative path, e.g. "_archive", it is appended after the path of the DataObject.
 *                    If this is an absolute path it replaces the path of the DataObject.
 * @param archiveInsidePartition By default archiveDir moves files for partitioned DataObjects as according to the following directory layout:
 *                               {{{<dataObject-root>/<archivePath>/<partitionDirs>/<filename>}}}
 *                               This is good because the partition does no longer exist afterwards.
 *                               By setting archiveInsidePartition=true files are moved inside the partition directory as follows:
 *                               {{{<dataObject-root>/<partitionDirs>/<archivePath>/<filename>}}}
 *                               This is good to keep the file in the original directory structure.
 *                               Note that archivePath needs to be relative if setting archiveInsidePartition=true.
 */
case class FileIncrementalMoveMode(archivePath: Option[String] = None, archiveInsidePartition: Boolean = false) extends ExecutionMode {
    assert(archivePath.forall(_.nonEmpty)) // empty string not allowed
  assert(!archiveInsidePartition || archivePath.exists(isRelativePath), s"archivePath needs to be relative if setting archiveInsidePartition=true (archivePath=$archivePath)")

  private var sparkFilesObserver: Option[SparkFilenameObservation[Row]] = None

  /**
   * Check for files in input data object.
   */
  override def apply(actionId: ActionId, mainInput: DataObject
                                            , mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues, PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    (mainInput, subFeed) match {
      case (inputDataObject: FileRefDataObject, inputSubFeed: FileSubFeed) =>
        // search FileRefs if not present from previous actions
        val fileRefs = inputSubFeed.fileRefs.getOrElse(inputDataObject.getFileRefs(inputSubFeed.partitionValues))
        // skip processing if no new data
        if (fileRefs.isEmpty) throw NoDataToProcessWarning(actionId.id, s"($actionId) No files to process found for ${inputDataObject.id}, partitionValues=${inputSubFeed.partitionValues.mkString(", ")}")
        Some(ExecutionModeResult(fileRefs = Some(fileRefs), inputPartitionValues = inputSubFeed.partitionValues, outputPartitionValues = inputSubFeed.partitionValues))
      case (inputDataObject: SparkFileDataObject, inputSubFeed: SparkSubFeed) =>
        if (!inputDataObject.checkFilesExisting) throw NoDataToProcessWarning(actionId.id, s"($actionId) No files to process found for ${mainInput.id} by FileIncrementalMoveMode.")
        if (inputDataObject.isV2ReadDataSource) { // for V1 DataSources this needs to be implemented in postExec
          // setup observation of files processed
          sparkFilesObserver = Some(inputDataObject.setupFilesObserver(actionId))
        }
        Some(ExecutionModeResult(inputPartitionValues = inputSubFeed.partitionValues, outputPartitionValues = inputSubFeed.partitionValues))
      case _ => throw ConfigurationException(s"($actionId) FileIncrementalMoveMode needs FileRefDataObject with FileSubFeed or SparkFileDataObject with SparkSubFeed as input")
    }
  }

  /**
   * Remove/archive files after read
   */
  override def postExec(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, mainInputSubFeed: SubFeed, mainOutputSubFeed: SubFeed)(implicit context: ActionPipelineContext): Unit = {
    (mainInput, mainOutputSubFeed) match {
      case (fileRefInput: FileRefDataObject, fileSubFeed: FileSubFeed) =>
        fileSubFeed.fileRefMapping.foreach {
          fileRefs =>
            logger.info(s"Cleaning up ${fileRefs.size} processed input files")
            val inputFiles = fileRefs.map(_.src.fullPath)
            if (archivePath.isDefined) {
              val newBasePath = if (fileRefInput.isAbsolutePath(archivePath.get)) archivePath.get
              else fileRefInput.concatPath(fileRefInput.getPath, archivePath.get)
              inputFiles.foreach { file =>
                val archiveFile = if (archiveInsidePartition) insertBeforeFilename(file, archivePath.get)
                else fileRefInput.concatPath(newBasePath, fileRefInput.relativizePath(file))
                fileRefInput.renameFileHandleAlreadyExisting(file, archiveFile)
              }
            } else {
              inputFiles.foreach(file => fileRefInput.deleteFile(file))
            }
        }
      case (sparkDataObject: SparkFileDataObject, sparkSubFeed: SparkSubFeed) =>
        val files = if (sparkDataObject.isV2ReadDataSource) {
          sparkFilesObserver
            .getOrElse(throw new IllegalStateException(s"($actionId) FilesObserver not setup for ${mainInput.id}"))
            .getFilesProcessed
        } else {
          sparkDataObject.getFileRefs(sparkSubFeed.partitionValues)
            .map(_.fullPath)
        }
        if (files.isEmpty) throw NoDataToProcessWarning(actionId.id, s"($actionId) No files to process found for ${mainInput.id} by FileIncrementalMoveMode.")
        logger.info(s"Cleaning up ${files.size} processed input files")
        if (archivePath.isDefined) {
          val archiveHadoopPath = new Path(archivePath.get)
          val newBasePath = if (archiveHadoopPath.isAbsolute) archiveHadoopPath
          else new Path(sparkDataObject.hadoopPath, archiveHadoopPath)
          // create archive file names
          val filePairs = files.map(file => (file, if (archiveInsidePartition) new Path(insertBeforeFilename(file, archivePath.get)) else new Path(newBasePath, sparkDataObject.relativizePath(file))))
          // create directories if not existing (otherwise hadoop rename fails)
          filePairs.map(_._2).map(_.getParent).distinct
            .foreach(p => if (!sparkDataObject.filesystem.exists(p)) sparkDataObject.filesystem.mkdirs(p))
          // rename files
          filePairs.foreach { case (file, newFile) =>
            sparkDataObject.renameFileHandleAlreadyExisting(file, newFile.toString)
          }
        } else {
          files.foreach(sparkDataObject.deleteFile)
        }
      case _ => throw ConfigurationException(s"($actionId) FileIncrementalMoveMode needs FileRefDataObject with FileSubFeed or SparkFileDataObject with SparkSubFeed as input")
    }
  }

  private def isSeparator(c: Char): Boolean = c=='/' || c=='\\'
  private def isRelativePath(path: String) = !path.contains(":") && !isSeparator(path.charAt(0))
  private def splitPathAndFilename(file: String) = {
    val splitIdx = file.lastIndexWhere(isSeparator)
    (file.substring(0,splitIdx), file.charAt(splitIdx), file.substring(splitIdx+1))
  }
  private def strip(str: String, fun: Char => Boolean) = {str.dropWhile(fun).reverse.dropWhile(fun).reverse}
  private def insertBeforeFilename(file: String, relativePath: String) = {
    val (path, separator, filename) = splitPathAndFilename(file)
    path + separator + strip(relativePath, isSeparator) + separator + filename
  }

  override def factory: FromConfigFactory[ExecutionMode] = ProcessAllMode
}

object FileIncrementalMoveMode extends FromConfigFactory[ExecutionMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): FileIncrementalMoveMode = {
    extract[FileIncrementalMoveMode](config)
  }
}
