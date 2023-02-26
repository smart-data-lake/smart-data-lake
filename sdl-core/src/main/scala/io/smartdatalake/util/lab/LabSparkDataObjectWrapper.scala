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

package io.smartdatalake.util.lab

import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject.{CanCreateSparkDataFrame, CanHandlePartitions, CanWriteSparkDataFrame, DataObject}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

case class LabSparkDataObjectWrapper[T <: DataObject with CanCreateSparkDataFrame](dataObject: T, context: ActionPipelineContext) {
  def get(): DataFrame = {
    dataObject.getSparkDataFrame()(context)
  }
  def get(topLevelPartitionValues: Seq[String]): DataFrame = {
    assert(partitionColumns.nonEmpty, s"($dataObject.id) partition columns are empty but called get(...) with topLevelPartitionValues ${topLevelPartitionValues.mkString(",")}")
    val topLevelPartitionColumn = partitionColumns.head
    dataObject.getSparkDataFrame(topLevelPartitionValues.map(p => PartitionValues(Map(topLevelPartitionColumn -> p))))(context)
  }
  def getWithPartitionValues(partitionValues: Seq[Map[String,String]]): DataFrame = {
    dataObject.getSparkDataFrame(partitionValues.map(PartitionValues(_)))(context)
  }
  def write(dataFrame: DataFrame): Unit = {
    dataObject match {
      case o: CanWriteSparkDataFrame =>
        o.writeSparkDataFrame(dataFrame)(context)
      case _ => throw NotSupportedException(dataObject.id, "can not write Spark DataFrames")
    }
  }
  def partitionColumns: Seq[String] = dataObject match {
    case o: CanHandlePartitions => o.partitions
    case _ => Seq()
  }
  def partitions: Seq[Map[String,String]] = dataObject match {
    case o: CanHandlePartitions => o.listPartitions(context).map(_.elements.mapValues(_.toString))
    case _ => throw NotSupportedException(dataObject.id, "is not partitioned")
  }
  def schema: StructType = dataObject.getSparkDataFrame()(context).schema
}

case class NotSupportedException(id: ConfigObjectId, msg: String) extends Exception(s"$id} $msg")