/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.misc

import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.spark.sql.SparkSession

object CompactionUtil extends SmartDataLakeLogger {

  /**
   * Compacting hadoop partitions is not supported out-of-the-box by hadoop, as files need to be read with the correct format and written again.
   * The following steps are used to compact partitions with Spark:
   * 1. Check if compaction is already in progress by looking for a special file "_SDL_COMPACTING" in data objects root hadoop path. If it exists
   *    and is not older than 12h exit compaction with Exception. Otherwise create/update special file "_COMPACTION". If the file is older than 12h
   *    the compaction process is assumed to be crashed.
   * 2. As step 5 is not atomic (delete and move are two operations), we need to check for possibly incomplete compactions of previous crashed runs and fix them.
   *    Incomplete compactions are marked with a special file "_SDL_MOVING" in the temporary path.
   *    Incomplete compacted partitions must be moved from temporary path to hadoop path (see step 5)
   *    and marked as compacted (see step 6).
   * 3. Filter already compacted partitions from given partitions by looking for "_SDL_COMPACTED" file, see step 5
   * 4. Data from partitions to be compacted is rewritten into a temporary path under this data objects hadoop path.
   * 5. Partitions to be compacted are deleted from the hadoop path and moved from the temporary path to the hadoop path. This should be done one-by-one to reduce risk of data loss.
   *    To recover in case of unexpected abort between delete and move, a special file "_SDL_MOVING" is created in temporary path before deleting hadoop path.
   *    After moving the temporary path, this file is deleted again. Mark compacted partitions by creating a special file "_SDL_COMPACTED" and
   * 6.  Delete "_SDL_COMPACTING" file created in step 1.
   * @param dataObject: DataObject with partition values to compact. The DataObject must be partitioned, able to read & write DataFrames and have a hadoop standard partition layout.
   * @param partitionValues: partition values to compact
   */
  def compactHadoopStandardPartitions(dataObject: DataObject with CanHandlePartitions with CanCreateDataFrame with CanWriteDataFrame with HasHadoopStandardFilestore, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    implicit val session: SparkSession = context.sparkSession
    implicit val fs: FileSystem = dataObject.filesystem
    assert(dataObject.partitions.nonEmpty, "compactPartitions needs a partitionend DataObject")
    assert(partitionValues.flatMap(_.keys).forall(dataObject.partitions.contains), "keys of partitionValues must exist as DataObject partitions")
    val partitionLayout = dataObject.partitionLayout().getOrElse(s"(${dataObject.id}) partitionLayout must be defined for compactPartitions")
    val compactingFileName = "_SDL_COMPACTING"
    val movingFileName = "_SDL_MOVING"
    val compactedFileName = "_SDL_COMPACTED"
    val tempPath = new Path(dataObject.hadoopPath, "_tempSdlCompacting")
    val trashPath = new Path(dataObject.hadoopPath, "_trashSdlCompacting")
    logger.info(s"(${dataObject.id}) starting compaction for ${partitionValues.mkString(", ")}")

    // 1. check / touch compacting file
    val compactingFile = new Path(dataObject.hadoopPath, compactingFileName)
    if (dataObject.filesystem.exists(compactingFile)) {
      if (dataObject.filesystem.getFileStatus(compactingFile).getModificationTime > System.currentTimeMillis - 12*60*60*1000) throw new IllegalStateException(s"(${dataObject.id}) Compaction already running! Compacting file younger than 12h found, please make sure there is no compaction running and clenaup file $compactingFile")
      else logger.warn(s"(${dataObject.id}) $compactingFileName older than 12h found - it seems the last compaction crashed")
    }
    HdfsUtil.touchFile(compactingFile)

    // 2. fix crashed compaction if needed
    if (dataObject.filesystem.exists(tempPath)) {
      val pathsToFix = dataObject.filesystem.globStatus(new Path(tempPath, s"*/$movingFileName"))
      pathsToFix.foreach { path =>
        val partitionDir = path.toString.stripSuffix(movingFileName).stripPrefix(tempPath.toString)
        logger.warn(s"(${dataObject.id}) Found compacted partition data to recover: $partitionDir - it seems there was a crash during the last compaction operation")
        val tempPartitionPath = new Path(tempPath, partitionDir)
        val targetPartitionPath = new Path(dataObject.hadoopPath, partitionDir)
        if (!dataObject.filesystem.exists(targetPartitionPath)) dataObject.filesystem.rename(tempPartitionPath, targetPartitionPath)
        else {
          // some filesystems (S3?) might be renaming/moving files in the directory one-by-one. This implements an incremental process for recovery in case of existing target files...
          // TODO: what if movingFileName was already moved, but others still missing?
          HdfsUtil.moveFiles(new Path(tempPartitionPath, "*"), targetPartitionPath, customFilter = !_.getPath.toString.endsWith(movingFileName))
        }
        HdfsUtil.touchFile(new Path(targetPartitionPath, compactedFileName))
        dataObject.filesystem.delete(new Path(targetPartitionPath, movingFileName), /*recursive*/ false)
        logger.info(s"(${dataObject.id}) Recovered partition $partitionDir")
      }
      HdfsUtil.deletePath(tempPath, doWarn = true)
    }

    // 3. Filter already compacted partitions
    val partitionValuesToCompact = partitionValues.filter { pv =>
      val compactedFile = new Path(new Path(dataObject.hadoopPath, pv.getPartitionString(partitionLayout)), compactedFileName)
      !dataObject.filesystem.exists(compactedFile)
    }
    if (partitionValuesToCompact.isEmpty) {
      logger.info(s"(${dataObject.id}) All partitions have already been compacted, there is no partition left")
      dataObject.filesystem.delete(compactingFile, /*recursive*/ false)
      return Seq()
    }
    logger.info(s"(${dataObject.id}) compacting partitions ${partitionValuesToCompact.mkString(", ")} (filtered already compacted partitions)")

    // 4. Rewrite data from partitions to be compacted to temp path
    val dfRewrite = dataObject.getDataFrame(partitionValuesToCompact)
    dataObject.writeDataFrameToPath(dfRewrite, tempPath, SDLSaveMode.Overwrite) // if defined this uses DataObjects options for repartition definition, otherwise Spark default parameters/optimizations are applied.
    logger.info(s"(${dataObject.id}) partitions rewritten")

    // 5. Move compacted partitions
    partitionValuesToCompact.foreach { pv =>
      val partitionDir = pv.getPartitionString(partitionLayout)
      val tempPartitionPath = new Path(tempPath, partitionDir)
      val targetPartitionPath = new Path(dataObject.hadoopPath, partitionDir)
      val trashPartitionPath = new Path(trashPath, partitionDir)
      HdfsUtil.touchFile(new Path(targetPartitionPath, movingFileName))
      fs.mkdirs(trashPath) // this is needed with Hadoop 2.7.4 on windows
      fs.mkdirs(tempPath) // this is needed with Hadoop 2.7.4 on windows
      fs.rename(targetPartitionPath, trashPartitionPath)
      fs.rename(tempPartitionPath, targetPartitionPath)
      HdfsUtil.touchFile(new Path(targetPartitionPath, compactedFileName))
      fs.delete(new Path(targetPartitionPath, movingFileName), /*recursive*/ false)
      fs.delete(trashPartitionPath, /*recursive*/ true)
    }
    HdfsUtil.deletePath(tempPath, doWarn = true)
    HdfsUtil.deletePath(trashPath, doWarn = true)

    // 6. remove compacting file
    dataObject.filesystem.delete(compactingFile, /*recursive*/ false)
    logger.info(s"(${dataObject.id}) finished compaction successfully")
    partitionValuesToCompact
  }
}
