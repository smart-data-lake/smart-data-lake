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
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.hadoop.fs.{FileSystem, Path}

trait HasHadoopStandardFilestore extends CanHandlePartitions { this: DataObject =>

  /**
   * Creates a cached hadoop [[FileSystem]] with the Hadoop configuration of the context.
   */
  private[smartdatalake] def filesystem(implicit context: ActionPipelineContext): FileSystem = {
    if (filesystemHolder == null) {
      serializableHadoopConfHolder = context.serializableHadoopConf
      filesystemHolder = getFilesystem(hadoopPath)
    }
    filesystemHolder
  }

  // filesystem holder
  @transient private var filesystemHolder: FileSystem = _

  // hadoop configuration holder
  private var serializableHadoopConfHolder: SerializableHadoopConfiguration = _ // we must serialize hadoop config for CustomFileAction running transformation on executors

  /**
   * Creates a hadoop [[FileSystem]] for [[Path]] with a given serializable hadoop configuration.
   */
  private[smartdatalake] def getFilesystem(path: Path): FileSystem = {
    assert(serializableHadoopConfHolder != null, "serializableHadoopConfHolder must be initialized before using getFilesystem")
    getFilesystem(path, serializableHadoopConfHolder)
  }

  /**
   * Creates a hadoop [[FileSystem]] for [[Path]] with a given serializable hadoop configuration.
   */
  private[smartdatalake] def getFilesystem(path: Path, hadoopConf: SerializableHadoopConfiguration): FileSystem = {
    HdfsUtil.getHadoopFsWithConf(path)(hadoopConf.get) // this must use serializable HadoopConfiguration to be distributed.
  }

  /**
   * Return a [[String]] specifying the partition layout.
   * For Hadoop the default partition layout is colname1=<value1>/colname2=<value2>/.../
   */
  def partitionLayout(): Option[String] = {
    if (partitions.nonEmpty) Some(HdfsUtil.getHadoopPartitionLayout(partitions))
    else None
  }

  private[smartdatalake] def hadoopPath(implicit context: ActionPipelineContext): Path
}
