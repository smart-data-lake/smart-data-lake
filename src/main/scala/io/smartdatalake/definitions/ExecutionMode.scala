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
package io.smartdatalake.definitions

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
 * Execution mode defines how data is selected when running a data pipeline.
 * You need to select one of the subclasses by defining type, i.e.
 *
 * {{{
 * executionMode = {
 *   type = SparkIncrementalMode
 *   compareCol = "id"
 * }
 * }}}
 */
sealed trait ExecutionMode

trait ExecutionModeWithMainInputOutput {
  def mainInputId: Option[String]
  def mainOutputId: Option[String]
}

/**
 * Partition difference execution mode lists partitions on mainInput & mainOutput DataObject and starts loading all missing partitions.
 * Partition columns to be used for comparision need to be a common 'init' of input and output partition columns.
 * This mode needs mainInput/Output DataObjects which CanHandlePartitions to list partitions.
 * Partition values are passed to following actions, if for partition columns which they have in common.
 * @param partitionColNb optional number of partition columns to use as a common 'init'.
 * @param mainInputId optional selection of inputId to be used for partition comparision. Only needed if there are multiple input DataObject's.
 * @param mainOutputId optional selection of outputId to be used for partition comparision. Only needed if there are multiple output DataObject's.
 * @param nbOfPartitionValuesPerRun optional restriction of the number of partition values per run.
 */
case class PartitionDiffMode(partitionColNb: Option[Int] = None, override val mainInputId: Option[String] = None, override val mainOutputId: Option[String] = None, nbOfPartitionValuesPerRun: Option[Int] = None) extends ExecutionMode with ExecutionModeWithMainInputOutput

/**
 * Spark streaming execution mode uses Spark Structured Streaming to incrementally execute data loads (trigger=Trigger.Once) and keep track of processed data.
 * This mode needs a DataObject implementing CanCreateStreamingDataFrame and works only with SparkSubFeeds.
 * @param checkpointLocation location for checkpoints of streaming query to keep state
 * @param inputOptions additional option to apply when reading streaming source. This overwrites options set by the DataObjects.
 * @param outputOptions additional option to apply when writing to streaming sink. This overwrites options set by the DataObjects.
 */
case class SparkStreamingOnceMode(checkpointLocation: String, inputOptions: Map[String,String] = Map(), outputOptions: Map[String,String] = Map(), outputMode: OutputMode = OutputMode.Append) extends ExecutionMode

/**
 * Compares max entry in "compare column" between mainOutput and mainInput and incrementally loads the delta.
 * This mode works only with SparkSubFeeds. The filter is not propagated to following actions.
 * @param compareCol a comparable column name existing in mainInput and mainOutput used to identify the delta. Column content should be bigger for newer records.
 * @param mainInputId optional selection of inputId to be used for comparision. Only needed if there are multiple input DataObject's.
 * @param mainOutputId optional selection of outputId to be used for comparision. Only needed if there are multiple output DataObject's.
 */
case class SparkIncrementalMode(compareCol: String, override val mainInputId: Option[String] = None, override val mainOutputId: Option[String] = None) extends ExecutionMode with ExecutionModeWithMainInputOutput
object SparkIncrementalMode {
  private[smartdatalake] val allowedDataTypes = Seq(StringType, LongType, IntegerType, ShortType, FloatType, DoubleType, TimestampType)
}
