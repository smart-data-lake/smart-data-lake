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

package io.smartdatalake.lab

import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject.{CanCreateSparkDataFrame, CanHandlePartitions, CanWriteSparkDataFrame, DataObject, FileRefDataObject, HadoopFileDataObject, HasHadoopStandardFilestore, HiveTableDataObject, TableDataObject}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.TimeZone

/**
 * A wrapper around a Spark DataObject simplifying the interface for interactive use.
 */
case class LabSparkDataObjectWrapper[T <: DataObject with CanCreateSparkDataFrame](dataObject: T, context: ActionPipelineContext) {
  def get(): DataFrame = {
    dataObject.getSparkDataFrame()(context)
  }
  def get(topLevelPartitions: Seq[String]): DataFrame = {
    if(partitionColumns.isEmpty) throw NotSupportedException(dataObject.id, s"DataObject is not partitioned but called get(...) with topLevelPartitions ${topLevelPartitions.mkString(",")}")
    val topLevelPartitionColumn = partitionColumns.head
    dataObject.getSparkDataFrame(topLevelPartitions.map(p => PartitionValues(Map(topLevelPartitionColumn -> p))))(context)
  }
  def getWithPartitions(partitions: Seq[Map[String,String]]): DataFrame = {
    if(partitionColumns.isEmpty) throw NotSupportedException(dataObject.id, s"DataObject is not partitioned but called getWithPartitions(...) with partitions ${partitions.mkString(",")}")
    dataObject.getSparkDataFrame(partitions.map(PartitionValues(_)))(context)
  }
  def write(dataFrame: DataFrame, topLevelPartitions: Seq[String] = Seq()): Unit = {
    writeWithPartitions(dataFrame, topLevelPartitions.map(p => Map(partitionColumns.head -> p)))
  }
  def writeWithPartitions(dataFrame: DataFrame, partitions: Seq[Map[String,String]]): Unit = {
    if(!SmartDataLakeBuilderLab.enableWritingDataObjects) throw new IllegalAccessException("Writing into DataObjects using SmartDataLakeBuilderLab is disabled by default because it is not seen as best practice. Set SmartDataLakeBuilderLab.enableWritingDataObjects=true to remove this limitation if you know what you do.")
    if(partitions.nonEmpty && partitionColumns.isEmpty) throw NotSupportedException(dataObject.id, s"DataObject is not partitioned but called getWithPartitions(...) with partitions ${partitions.mkString(",")}")
    dataObject match {
      case o: CanWriteSparkDataFrame =>
        o.writeSparkDataFrame(dataFrame, partitions.map(pv => PartitionValues(pv)))(context)
      case _ => throw NotSupportedException(dataObject.id, "can not write Spark DataFrames")
    }
  }

  def dropPartitions(partitions: Seq[Map[String,String]]): Unit = dataObject match {
    case o: CanHandlePartitions => o.deletePartitions(partitions.map(pv => PartitionValues(pv)))(context)
    case _ => throw NotSupportedException(dataObject.id, "is not partitioned")
  }

  def infos: Map[String,String] = {
    Seq(
      Some(dataObject).collect{case o: TableDataObject => ("table", o.table.fullName)},
      Some(dataObject).collect{case o: FileRefDataObject => ("path", o.getPath(context))}
    ).flatten.toMap
  }

  def partitionColumns: Seq[String] = dataObject match {
    case o: CanHandlePartitions => o.partitions
    case _ => Seq()
  }
  def partitions: Seq[Map[String,String]] = dataObject match {
    case o: CanHandlePartitions => o.listPartitions(context).map(_.elements.mapValues(_.toString))
    case _ => throw NotSupportedException(dataObject.id, "is not partitioned")
  }
  def topLevelPartitions: Seq[String] = partitions.map(_(partitionColumns.head))

  /**
   * lists modification date of partition folders
   */
  def partitionModDates(timezoneId: ZoneId = TimeZone.getDefault.toZoneId): Seq[(PartitionValues,LocalDateTime)] = dataObject match {
    case o: HadoopFileDataObject with CanHandlePartitions =>
      o.getPartitionPathsStatus(context)
        .map( s => (o.extractPartitionValuesFromDirPath(s.getPath.toString)(context), LocalDateTime.ofInstant(Instant.ofEpochMilli(s.getModificationTime), timezoneId)))
    case _ => throw NotSupportedException(dataObject.id, "is not partitioned or has no hadoop directory layout")
  }
  def schema: StructType = dataObject.getSparkDataFrame()(context).schema
  def printSchema(): Unit = dataObject.getSparkDataFrame()(context).printSchema()
}

case class NotSupportedException(id: ConfigObjectId, msg: String) extends Exception(s"$id} $msg")
