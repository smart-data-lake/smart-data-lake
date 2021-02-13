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

package io.smartdatalake.util.hdfs

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataobject.{FileRef, SparkFileDataObject}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, hash, lit, pmod}

/**
 * This controls repartitioning of the DataFrame before writing with Spark to Hadoop.
 *
 * When writing multiple partitions of a partitioned DataObject, the number of spark tasks created is equal to numberOfTasksPerPartition
 * multiplied with the number of partitions to write. To spread the records of a partition only over numberOfTasksPerPartition spark tasks,
 * keyCols must be given which are used to derive a task number inside the partition (hashvalue(keyCols) modulo numberOfTasksPerPartition).
 *
 * When writing to an unpartitioned DataObject or only one partition of a partitioned DataObject, the number of spark tasks created is equal
 * to numberOfTasksPerPartition. Optional keyCols can be used to keep corresponding records together in the same task/file.
 *
 * @param numberOfTasksPerPartition Number of Spark tasks to create per partition before writing to DataObject by repartitioning the DataFrame. This controls how many files are created in each Hadoop partition.
 * @param keyCols Optional key columns to distribute records over Spark tasks inside a Hadoop partition. If numberOfTasksPerPArtition is 1 this setting has no effect. If DataObject has Hadoop partitions defined, keyCols must be defined.
 * @param sortCols Optional columns to sort records inside files created.
 * @param filename Option filename to rename target file(s). If numberOfTasksPerPartition is greater than 1,
 *                 multiple files can exist in a directory and a number is inserted into the filename after the first '.'.
 *                 Example: filename=data.csv -> files created are data.1.csv, data.2.csv, ...
 */
case class SparkRepartitionDef(numberOfTasksPerPartition: Int,
                               keyCols: Seq[String] = Seq(),
                               sortCols: Seq[String] = Seq(),
                               filename: Option[String] = None
                              ) extends SmartDataLakeLogger {
  assert(numberOfTasksPerPartition > 0, s"numberOfTasksPerPartition must be greater than 0")

  /**
   *
   * @param df DataFrame to repartition
   * @param partitions DataObjects partition columns
   * @param nbOfPartitionValues Number fo PartitionsValues to be written with this DataFrame
   * @param dataObjectId id of DataObject for logging
   * @return
   */
  def prepareDataFrame(df: DataFrame, partitions: Seq[String], nbOfPartitionValues: Int, dataObjectId: DataObjectId): DataFrame = {
    // repartition spark DataFrame
    val dfRepartitioned = if (partitions.nonEmpty && nbOfPartitionValues > 1) {
      if (numberOfTasksPerPartition > 1 && keyCols.isEmpty ) logger.warn(s"($dataObjectId) SparkRepartitionDef: cannot distribute records over Spark tasks within Hadoop partitions with no keyCols defined. Define keyCols!")
      // to distribute records across tasks within partitions, we need calculate a task number from keyCols
      val taskNbCol = pmod(hash(keyCols.map(col):_*),lit(numberOfTasksPerPartition))
      df.repartition(numberOfTasksPerPartition * nbOfPartitionValues, partitions.map(col) :+ taskNbCol :_*)
    } else {
      if (partitions.nonEmpty && nbOfPartitionValues == 0) logger.warn(s"($dataObjectId) SparkRepartitionDef: cannot multiply numberOfTasksPerPartition when writing with no partitions values to partitioned table ")
      if (keyCols.isEmpty) df.repartition(numberOfTasksPerPartition)
      else df.repartition(numberOfTasksPerPartition, keyCols.map(col):_*)
    }
    // sort within spark partitions
    if (sortCols.nonEmpty) dfRepartitioned.sortWithinPartitions(sortCols.map(col):_*)
    else dfRepartitioned
  }

  def renameFiles(fileRefs: Seq[FileRef])(implicit filesystem: FileSystem): Unit = {
    filename.foreach { filename =>
      fileRefs.groupBy(_.partitionValues).values.foreach { files =>
        if (numberOfTasksPerPartition > 1) {
          files.zipWithIndex.foreach {
            case (fileRef, idx) => renameFile(fileRef, filename, Some(idx+1))
          }
        } else {
          assert(files.size == 1, "number of files for a partition value should be 1 if numberOfTasksPerPartition=1!")
          renameFile(files.head, filename)
        }
      }
    }
  }

  private def renameFile(fileRef: FileRef, filename: String, idxOpt: Option[Int] = None)(implicit filesystem: FileSystem): Unit = {
    val path = new Path(fileRef.fullPath)
    def addIndexToFileName(filename: String, idx: Int): String = {
      val elements = filename.split('.')
      (Seq(elements.head, idx.toString) ++ elements.drop(1)).mkString(".")
    }
    val newFilename = idxOpt.map(idx => addIndexToFileName(filename, idx))
      .getOrElse(filename)
    val newPath = new Path(path.getParent, newFilename)
    filesystem.rename(new Path(fileRef.fullPath), newPath)
  }
}