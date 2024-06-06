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

import io.smartdatalake.definitions.SDLSaveMode._
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.{ActionPipelineContext, GenericMetrics}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

import java.time.ZoneOffset
import scala.reflect.runtime.universe.{Type, typeOf}

trait CanWriteSparkDataFrame extends CanWriteDataFrame { this: DataObject =>

  /**
   * Configured options for the Spark [[DataFrameReader]]/[[DataFrameWriter]].
   *
   * @see [[DataFrameReader]]
   * @see [[DataFrameWriter]]
   */
  def options: Map[String, String] = Map()

  def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = ()

  def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): MetricsMap

  private[smartdatalake] def writeSparkDataFrameToPath(df: DataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): MetricsMap = throw new RuntimeException("writeDataFrameToPath not implemented")

  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    df match {
      case sparkDf: SparkDataFrame => writeSparkDataFrame(sparkDf.inner, partitionValues, isRecursiveInput, saveModeOptions)
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method writeDataFrame")
    }
  }

  override def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    df match {
      case sparkDf: SparkDataFrame => initSparkDataFrame(sparkDf.inner, partitionValues, saveModeOptions)
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method init")
    }
  }

  override private[smartdatalake] def writeSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[SparkSubFeed])

  override private[smartdatalake] def writeDataFrameToPath(df: GenericDataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): Unit = {
    df match {
      case sparkDataFrame: SparkDataFrame => writeSparkDataFrameToPath(sparkDataFrame.inner, path, finalSaveMode)
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method writeDataFrameToPath")
    }
  }

  override def writeStreamingDataFrame(df: GenericDataFrame, trigger: Trigger, options: Map[String,String], checkpointLocation: String, queryName: String, outputMode: OutputMode = OutputMode.Append, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): StreamingQuery = {
    df match {
      case sparkDataFrame: SparkDataFrame =>
        // lambda function is ambiguous with foreachBatch in scala 2.12... we need to create a real function...
        // Note: no partition values supported when writing streaming target
        def microBatchWriter(dfMicrobatch: Dataset[Row], batchid: Long): Unit = {
          val metrics = writeSparkDataFrame(dfMicrobatch, Seq(), saveModeOptions = saveModeOptions)
          val actionMetrics = GenericMetrics(s"streaming-microBatchWriter", System.currentTimeMillis()/1000, metrics)
          context.currentAction.get.addAsyncMetrics(None, Some(id), actionMetrics)
        }
        sparkDataFrame.inner
          .writeStream
          .trigger(trigger)
          .queryName(queryName)
          .outputMode(outputMode)
          .option("checkpointLocation", checkpointLocation)
          .options(streamingOptions ++ options) // options override streamingOptions
          .foreachBatch(microBatchWriter _)
          .start()
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method writeStreamingDataFrame")
    }
  }

}

/**
 * Mapping to Spark SaveMode
 * This is one-to-one except custom modes as OverwritePreserveDirectories
 */
object SparkSaveMode {
  def from(mode: SDLSaveMode): SaveMode = mode match {
    case Overwrite => SaveMode.Overwrite
    case Append => SaveMode.Append
    case ErrorIfExists => SaveMode.ErrorIfExists
    case Ignore => SaveMode.Ignore
    case OverwritePreserveDirectories => SaveMode.Append // Append with spark, but delete files before with hadoop
    case OverwriteOptimized => SaveMode.Append // Append with spark, but delete partitions before with hadoop
  }
}