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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SerializableHadoopConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

trait HasHadoopStandardFilestore extends CanHandlePartitions { this: DataObject =>

  /**
   * Creates a cached hadoop [[FileSystem]] for the provided [[SparkSession]].
   */
  private[smartdatalake] def filesystem(implicit session: SparkSession): FileSystem = {
    if (filesystemHolder == null) filesystemHolder = getFilesystem(hadoopPath)
    filesystemHolder
  }
  @transient private var filesystemHolder: FileSystem = _
  private var serializableHadoopConf: SerializableHadoopConfiguration = _ // we must serialize hadoop config for CustomFileAction running transformation on executors

  /**
   * Creates a hadoop [[FileSystem]] for the provided [[SparkSession]] and [[Path]].
   */
  private[smartdatalake] def getFilesystem(path: Path)(implicit session: SparkSession) = {
    if (serializableHadoopConf == null) {
      serializableHadoopConf = new SerializableHadoopConfiguration(session.sparkContext.hadoopConfiguration)
    }
    HdfsUtil.getHadoopFsWithConf(path, serializableHadoopConf.get)
  }

  /**
   * Return a [[String]] specifying the partition layout.
   * For Hadoop the default partition layout is colname1=<value1>/colname2=<value2>/.../
   */
  def partitionLayout(): Option[String] = {
    if (partitions.nonEmpty) Some(HdfsUtil.getHadoopPartitionLayout(partitions))
    else None
  }

  private[smartdatalake] def hadoopPath(implicit session: SparkSession): Path
}
