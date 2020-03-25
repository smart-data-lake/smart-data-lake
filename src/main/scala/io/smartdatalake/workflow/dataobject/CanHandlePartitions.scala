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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.util.hdfs.PartitionValues
import org.apache.spark.sql.SparkSession

/**
 * A trait to be implemented by DataObjects which store partitioned data
 */
private[smartdatalake] trait CanHandlePartitions {

  /**
   * Definition of partition columns
   */
  def partitions: Seq[String]

  /**
   * Delete given partitions. This is used to cleanup partitions after they are processed.
   */
  def deletePartitions(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = throw new RuntimeException(s"deletePartitions not implemented")

  /**
   * list partition values
   */
  def listPartitions(implicit session: SparkSession): Seq[PartitionValues]

  /**
   * create empty partition
   */
  def createEmptyPartition(partitionValues: PartitionValues)(implicit session: SparkSession): Unit = throw new RuntimeException(s"createEmptyPartition not implemented")
}

