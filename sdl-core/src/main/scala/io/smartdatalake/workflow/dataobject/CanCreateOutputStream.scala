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
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.SparkSession

import java.io.OutputStream

private[smartdatalake] trait CanCreateOutputStream {

  /**
   * This is called before any output stream is created to initialize writing.
   * It is used to apply SaveMode, e.g. deleting existing partitions.
   */
  def startWritingOutputStreams(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext): Unit

  /**
   * Create an OutputStream for a given path, that the Action can use to write data into.
   */
  def createOutputStream(path: String, overwrite: Boolean)(implicit session: SparkSession, context: ActionPipelineContext): OutputStream

  /**
   * This is called after all output streams have been written.
   * It is used for e.g. making sure empty partitions are created as well.
   */
  def endWritingOutputStreams(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext): Unit

}
