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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.{ConfigObjectId, DataObjectId}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataobject.FileRef
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

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
 * @param numberOfTasksPerPartition Number of Spark tasks to create per partition before writing to DataObject by repartitioning the DataFrame.
 *                                  This controls how many files are created in each Hadoop partition.
 * @param keyCols  Optional key columns to distribute records over Spark tasks inside a Hadoop partition.
 *                 If DataObject has Hadoop partitions defined, keyCols must be defined.
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
  import SparkRepartitionDef._
  assert(numberOfTasksPerPartition > 0, s"numberOfTasksPerPartition must be greater than 0")

  /**
   * @param df DataFrame to repartition
   * @param partitions DataObjects partition columns
   * @param partitionValues PartitionsValues to be written with this DataFrame
   * @param dataObjectId id of DataObject for logging
   * @return
   */
  def prepareDataFrame(df: DataFrame, partitions: Seq[String], partitionValues: Seq[PartitionValues], dataObjectId: DataObjectId): DataFrame = {
    // filter partition values having all partition columns specified
    val partitionsSet = partitions.toSet
    val completePartitionValues = partitionValues.filter(_.keys == partitionsSet)
    // process Dataframe
    val dfRepartitioned = repartitionDataFrame(df, completePartitionValues, dataObjectId, keyCols, numberOfTasksPerPartition)
    sortDataFrame(dfRepartitioned, dataObjectId, sortCols)
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
}

object SparkRepartitionDef extends SmartDataLakeLogger {

  def repartitionDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], configObjectId: ConfigObjectId, keyCols: Seq[String], numberOfTasksPerPartition: Int): DataFrame = {
    val partitions = partitionValues.flatMap(_.keys)
    if (partitions.nonEmpty) {
      if (partitionValues.isEmpty) { // writing partitioned data object with no partition values given
        logger.info(s"($configObjectId) SparkRepartitionDef: cannot multiply numberOfTasksPerPartition when writing with no partitions values to partitioned table. Will let spark decide about the number of tasks created, but use keyCols/rand to limit number of tasks with data.")
        repartitionForMultiplePartitionValues(df, partitions, None, configObjectId, keyCols, numberOfTasksPerPartition)
      } else if (partitionValues.size == 1) { // writing partitioned data object with 1 partition value
        repartitionForOnePartitionValue(df, keyCols, numberOfTasksPerPartition) // this is the same as writing an un-partitioned data object
      } else { // nbOfPartitionValues > 1 -> writing partitioned data object with multiple partition values given
        repartitionForMultiplePartitionValues(df, partitions, Some(partitionValues.size), configObjectId, keyCols, numberOfTasksPerPartition)
      }
    } else { // un-partitioned data object
      repartitionForOnePartitionValue(df, keyCols, numberOfTasksPerPartition) // this is the same as writing only one partition value
    }
  }

  private def repartitionForOnePartitionValue(df: DataFrame, keyCols: Seq[String], numberOfTasksPerPartition: Int): DataFrame = {
    if (keyCols.isEmpty || numberOfTasksPerPartition == 1) df.repartition(numberOfTasksPerPartition)
    else df.repartition(numberOfTasksPerPartition, keyCols.map(col):_*)
  }

  private def repartitionForMultiplePartitionValues(df: DataFrame, partitions: Seq[String], nbOfPartitionValues: Option[Int], configObjectId: ConfigObjectId, keyCols: Seq[String], numberOfTasksPerPartition: Int): DataFrame = {
    if (numberOfTasksPerPartition > 1 && keyCols.isEmpty) logger.info(s"($configObjectId) SparkRepartitionDef: distribution of records over Spark tasks within Hadoop partitions not defined, using random value now. Define keyCols to have better control over the distribution.")
    // to distribute records across tasks within partitions, we need calculate a task number from keyCols
    val repartitionCols = if (numberOfTasksPerPartition == 1) partitions.map(col)
    else if (keyCols.nonEmpty) partitions.map(col) :+ pmod(hash(keyCols.map(col): _*), lit(numberOfTasksPerPartition))
    else partitions.map(col) :+ floor(rand() * numberOfTasksPerPartition).cast(IntegerType)
    if (nbOfPartitionValues.isDefined) {
      df.repartition(numberOfTasksPerPartition * nbOfPartitionValues.get, repartitionCols: _*)
    } else {
      df.repartition(repartitionCols: _*)
    }
  }

  def sortDataFrame(df: DataFrame, configObjectId: ConfigObjectId, sortCols: Seq[String]): DataFrame = {
    // sort within spark partitions
    if (sortCols.nonEmpty) {
      val sortColDirRegex = "([^\\s]+)\\s([^\\s]+)".r
      val sortColRegex = "([^\\s]+)".r
      val sortExprs = sortCols.map {
        case sortColDirRegex(colName, sortDir) if sortDir == "asc" => col(colName).asc
        case sortColDirRegex(colName, sortDir) if sortDir == "desc" => col(colName).desc
        case sortColDirRegex(colName, sortDir) => throw new ConfigurationException(s"""($configObjectId) Wrong sort direction ($sortDir) provided in sortCols entry "$sortCols". Sort direction must be asc or desc.""", Some("sparkRepartition.sortCols"))
        case sortColRegex(colName) => col(colName).asc
        case entry => throw new ConfigurationException(s"""($configObjectId) Too many arguments provided in [sparkRepartition.sortCols] entry "$entry". Just provide colName or colName and sortDir separated by whitespace.""")
      }
      df.sortWithinPartitions(sortExprs: _*)
    }
    else df
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