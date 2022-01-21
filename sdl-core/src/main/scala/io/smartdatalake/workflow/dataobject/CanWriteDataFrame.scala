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

import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

import scala.reflect.runtime.universe.Type

private[smartdatalake] trait CanWriteDataFrame {

  // additional streaming options which can be overridden by the DataObject, e.g. Kafka topic to write to
  def streamingOptions: Map[String, String] = Map()

  /**
   * Called during init phase for checks and initialization.
   * If possible dont change the system until execution phase.
   */
  def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = Unit

  /**
   * Write a DataFrame to the DataObject
   */
  def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit

  /**
   * Write a GenericDataFrameSubFeed to the DataObject.
   * See writeSubFeedSupportedTypes for supported languages of the GenericDataFrameSubFeed.
   */
  private[smartdatalake] def writeSubFeed(subFeed: DataFrameSubFeed, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit

  /**
   * Declare supported Language for writing DataFrame.
   */
  private[smartdatalake] def writeSubFeedSupportedTypes: Seq[Type]

  /**
   * Write DataFrame to specific Path with properties of this DataObject.
   * This is needed for compacting partitions by housekeeping.
   * Note: this is optional to implement.
   */
  private[smartdatalake] def writeDataFrameToPath(df: GenericDataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): Unit = throw new RuntimeException("writeDataFrameToPath not implemented")

  /**
   * Write Spark structured streaming DataFrame
   * The default implementation uses foreachBatch and this traits writeDataFrame method to write the DataFrame.
   * Some DataObjects will override this with specific implementations (Kafka).
   *
   * @param df      The Streaming DataFrame to write
   * @param trigger Trigger frequency for stream
   * @param checkpointLocation location for checkpoints of streaming query
   */
  // TODO: this interface is still very spark specific!
  def writeStreamingDataFrame(df: GenericDataFrame, trigger: Trigger, options: Map[String,String], checkpointLocation: String, queryName: String, outputMode: OutputMode = OutputMode.Append, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): StreamingQuery = throw new RuntimeException("writeDataFrameToPath not implemented")

}
