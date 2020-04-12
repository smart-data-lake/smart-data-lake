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
import io.smartdatalake.workflow.dataobject.SparkFileDataObject
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, hash, lit, pmod}

/**
 * This controls repartitioning of the DataFrame before writing with Spark to Hadoop
 *
 * @param numberOfTasksPerPartition Number of Spark tasks to create per partition before writing to DataObject by repartitioning the DataFrame. This controls how many files are created in each Hadoop partition.
 * @param keyCols Optional key columns to distribute records over Spark tasks inside a Hadoop partition. If numberOfTasksPerPArtition is 1 this setting has no effect. If DataObject has Hadoop partitions defined, keyCols must be defined.
 * @param sortCols Optional columns to sort records inside files created.
 * @param filename Option filename to rename target file if numberOfTasksPerPartition is 1
 */
case class SparkRepartitionDef(numberOfTasksPerPartition: Int,
                               keyCols: Seq[String] = Seq(),
                               sortCols: Seq[String] = Seq(),
                               filename: Option[String] = None
                              ) extends SmartDataLakeLogger {
  assert(numberOfTasksPerPartition > 0, s"numberOfTasksPerPartition must be greater than 0")
  assert(filename.isEmpty || numberOfTasksPerPartition == 1, s"if filename is defined then numberOfTasksPerPartition must be set to 1.")

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
}