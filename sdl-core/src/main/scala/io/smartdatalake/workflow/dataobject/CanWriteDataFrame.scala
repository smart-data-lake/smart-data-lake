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

import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

private[smartdatalake] trait CanWriteDataFrame {

  // additional streaming options which can be overridden by the DataObject, e.g. Kafka topic to write to
  def streamingOptions: Map[String, String] = Map()

  /**
   * Called during init phase for checks and initialization.
   * If possible dont change the system until execution phase.
   */
  def init(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit session: SparkSession, context: ActionPipelineContext): Unit = Unit

  /**
   * Write DataFrame to DataObject
   * @param df the DataFrame to write
   * @param partitionValues partition values included in DataFrames data
   * @param isRecursiveInput if DataFrame needs this DataObject as input - special treatment might be needed in this case.
   */
  def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)(implicit session: SparkSession, context: ActionPipelineContext): Unit

  /**
   * Write DataFrame to specific Path with properties of this DataObject.
   * This is needed for compacting partitions by housekeeping.
   * Note: this is optional to implement.
   */
  private[smartdatalake] def writeDataFrameToPath(df: DataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit session: SparkSession): Unit = throw new RuntimeException("writeDataFrameToPath not implemented")

  /**
   * Write Spark structured streaming DataFrame
   * The default implementation uses foreachBatch and this traits writeDataFrame method to write the DataFrame.
   * Some DataObjects will override this with specific implementations (Kafka).
   *
   * @param df      The Streaming DataFrame to write
   * @param trigger Trigger frequency for stream
   * @param checkpointLocation location for checkpoints of streaming query
   */
  def writeStreamingDataFrame(df: DataFrame, trigger: Trigger, options: Map[String,String], checkpointLocation: String, queryName: String, outputMode: OutputMode = OutputMode.Append, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit session: SparkSession, context: ActionPipelineContext): StreamingQuery = {

    // lambda function is ambiguous with foreachBatch in scala 2.12... we need to create a real function...
    // Note: no partition values supported when writing streaming target
    def microBatchWriter(dfMicrobatch: Dataset[Row], batchid: Long): Unit = writeDataFrame(dfMicrobatch, Seq(), saveModeOptions = saveModeOptions)

    df
      .writeStream
      .trigger(trigger)
      .queryName(queryName)
      .outputMode(outputMode)
      .option("checkpointLocation", checkpointLocation)
      .options(streamingOptions ++ options) // options override streamingOptions
      .foreachBatch(microBatchWriter _)
      .start()
  }

}
