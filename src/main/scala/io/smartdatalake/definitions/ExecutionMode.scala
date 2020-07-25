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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionHelper.{getOptionalDataFrame, searchCommonInits}
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanHandlePartitions, DataObject}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}
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
sealed trait ExecutionMode extends SmartDataLakeLogger {
  def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = Unit
  def init(implicit session: SparkSession, context: ActionPipelineContext): Unit = Unit
  def apply(actionId: ActionObjectId, mainInput: DataObject, mainOutput: DataObject)(implicit session: SparkSession, context: ActionPipelineContext): Option[(Seq[PartitionValues], Option[String])]
  def mainInputOutputNeeded: Boolean = false
}

private[smartdatalake] trait ExecutionModeWithMainInputOutput {
  def alternativeOutputId: Option[DataObjectId] = None
  def alternativeOutput(implicit context: ActionPipelineContext): Option[DataObject] = {
    alternativeOutputId.map(context.instanceRegistry.get[DataObject](_))
  }
}

/**
 * Partition difference execution mode lists partitions on mainInput & mainOutput DataObject and starts loading all missing partitions.
 * Partition columns to be used for comparision need to be a common 'init' of input and output partition columns.
 * This mode needs mainInput/Output DataObjects which CanHandlePartitions to list partitions.
 * Partition values are passed to following actions, if for partition columns which they have in common.
 * @param partitionColNb optional number of partition columns to use as a common 'init'.
 * @param alternativeOutputId optional alternative outputId of DataObject later in the DAG. This replaces the mainOutputId.
 *                            It can be used to ensure processing all partitions over multiple actions in case of errors.
 * @param nbOfPartitionValuesPerRun optional restriction of the number of partition values per run.
 */
case class PartitionDiffMode(partitionColNb: Option[Int] = None, override val alternativeOutputId: Option[DataObjectId] = None, nbOfPartitionValuesPerRun: Option[Int] = None) extends ExecutionMode with ExecutionModeWithMainInputOutput {
  override def mainInputOutputNeeded: Boolean = alternativeOutputId.isEmpty
  override def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    // check alternativeOutput exists
    alternativeOutput
  }
  override def apply(actionId: ActionObjectId, mainInput: DataObject, mainOutput: DataObject)(implicit session: SparkSession, context: ActionPipelineContext): Option[(Seq[PartitionValues], Option[String])] = {
    val input = mainInput
    val output = alternativeOutput.getOrElse(mainOutput)
    (input, output) match {
      case (partitionInput: CanHandlePartitions, partitionOutput: CanHandlePartitions)  =>
        if (partitionInput.partitions.nonEmpty) {
          if (partitionOutput.partitions.nonEmpty) {
            // prepare common partition columns
            val commonInits = searchCommonInits(partitionInput.partitions, partitionOutput.partitions)
            require(commonInits.nonEmpty, s"$actionId has set initExecutionMode = 'PartitionDiffMode' but no common init was found in partition columns for $input and $output")
            val commonPartitions = if (partitionColNb.isDefined) {
              commonInits.find(_.size==partitionColNb.get).getOrElse(throw ConfigurationException(s"$actionId has set initExecutionMode = 'PartitionDiffMode' but no common init with ${partitionColNb.get} was found in partition columns of $input and $output from $commonInits!"))
            } else {
              commonInits.maxBy(_.size)
            }
            // calculate missing partition values
            val partitionValuesToBeProcessed = partitionInput.listPartitions.map(_.filterKeys(commonPartitions)).toSet
              .diff(partitionOutput.listPartitions.map(_.filterKeys(commonPartitions)).toSet).toSeq
            // stop processing if no new data
            if (partitionValuesToBeProcessed.isEmpty) throw NoDataToProcessWarning(actionId.id, s"($actionId) No partitions to process found for ${input.id}")
            // sort and limit number of partitions processed
            val ordering = PartitionValues.getOrdering(commonPartitions)
            val selectedPartitionValues = nbOfPartitionValuesPerRun match {
              case Some(n) => partitionValuesToBeProcessed.sorted(ordering).take(n)
              case None => partitionValuesToBeProcessed.sorted(ordering)
            }
            logger.info(s"($actionId) PartitionDiffMode selected partition values ${selectedPartitionValues.mkString(", ")} to process")
            //return
            Some((selectedPartitionValues, None))
          } else throw ConfigurationException(s"$actionId has set initExecutionMode = PartitionDiffMode but $output has no partition columns defined!")
        } else throw ConfigurationException(s"$actionId has set initExecutionMode = PartitionDiffMode but $input has no partition columns defined!")
      case (_: CanHandlePartitions, _) => throw ConfigurationException(s"$actionId has set initExecutionMode = PartitionDiffMode but $output does not support partitions!")
      case (_, _) => throw ConfigurationException(s"$actionId has set initExecutionMode = PartitionDiffMode but $input does not support partitions!")
    }
  }
}

/**
 * Spark streaming execution mode uses Spark Structured Streaming to incrementally execute data loads (trigger=Trigger.Once) and keep track of processed data.
 * This mode needs a DataObject implementing CanCreateStreamingDataFrame and works only with SparkSubFeeds.
 * @param checkpointLocation location for checkpoints of streaming query to keep state
 * @param inputOptions additional option to apply when reading streaming source. This overwrites options set by the DataObjects.
 * @param outputOptions additional option to apply when writing to streaming sink. This overwrites options set by the DataObjects.
 */
case class SparkStreamingOnceMode(checkpointLocation: String, inputOptions: Map[String,String] = Map(), outputOptions: Map[String,String] = Map(), outputMode: OutputMode = OutputMode.Append) extends ExecutionMode {
  override def apply(actionId: ActionObjectId, mainInput: DataObject, mainOutput: DataObject)(implicit session: SparkSession, context: ActionPipelineContext): Option[(Seq[PartitionValues], Option[String])] = throw new NotImplementedError
}

/**
 * Compares max entry in "compare column" between mainOutput and mainInput and incrementally loads the delta.
 * This mode works only with SparkSubFeeds. The filter is not propagated to following actions.
 * @param compareCol a comparable column name existing in mainInput and mainOutput used to identify the delta. Column content should be bigger for newer records.
 * @param alternativeOutputId optional alternative outputId of DataObject later in the DAG. This replaces the mainOutputId.
 *                            It can be used to ensure processing all partitions over multiple actions in case of errors.
 */
case class SparkIncrementalMode(compareCol: String, override val alternativeOutputId: Option[DataObjectId] = None) extends ExecutionMode with ExecutionModeWithMainInputOutput {
  override def mainInputOutputNeeded: Boolean = alternativeOutputId.isEmpty
  override def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    // check alternativeOutput exists
    alternativeOutput
  }
  override def apply(actionId: ActionObjectId, mainInput: DataObject, mainOutput: DataObject)(implicit session: SparkSession, context: ActionPipelineContext): Option[(Seq[PartitionValues], Option[String])] = {
    import session.implicits._
    val input = mainInput
    val output = alternativeOutput.getOrElse(mainOutput)
    (input,output) match {
      case (sparkInput: CanCreateDataFrame, sparkOutput: CanCreateDataFrame) =>
        // if data object is new, it might not be able to create a DataFrame
        val dfInputOpt = getOptionalDataFrame(sparkInput)
        val dfOutputOpt = getOptionalDataFrame(sparkOutput)
        (dfInputOpt, dfOutputOpt) match {
          // if both DataFrames exist, compare and create filter
          case (Some(dfInput), Some(dfOutput)) =>
            val inputColType = dfInput.schema(compareCol).dataType
            require(SparkIncrementalMode.allowedDataTypes.contains(inputColType), s"($actionId) Type of compare column ${compareCol} must be one of ${SparkIncrementalMode.allowedDataTypes.mkString(", ")} in ${sparkInput.id}")
            val outputColType = dfOutput.schema(compareCol).dataType
            require(SparkIncrementalMode.allowedDataTypes.contains(outputColType), s"($actionId) Type of compare column ${compareCol} must be one of ${SparkIncrementalMode.allowedDataTypes.mkString(", ")} in ${sparkOutput.id}")
            require(inputColType == outputColType, s"($actionId) Type of compare column ${compareCol} is different between ${sparkInput.id} ($inputColType) and ${sparkOutput.id} ($outputColType)")
            // get latest values
            val inputLatestValue = dfInput.agg(max(col(compareCol)).cast(StringType)).as[String].head
            val outputLatestValue = dfOutput.agg(max(col(compareCol)).cast(StringType)).as[String].head
            // stop processing if no new data
            if (outputLatestValue == inputLatestValue) throw NoDataToProcessWarning(actionId.id, s"($actionId) No increment to process found for ${output.id} column ${compareCol} (lastestValue=$outputLatestValue)")
            logger.info(s"($actionId) SparkIncrementalMode selected increment for writing to ${output.id}: column ${compareCol} from $outputLatestValue to $inputLatestValue to process")
            // prepare filter
            val selectedData = s"${compareCol} > cast('$outputLatestValue' as ${inputColType.sql})"
            Some((Seq(), Some(selectedData)))
          // otherwise don't filter
          case _ =>
            logger.info(s"($actionId) SparkIncrementalMode selected all records for writing to ${output.id}, because input or output DataObject is still empty.")
            Some((Seq(), None))
        }
      case _ => throw ConfigurationException(s"$actionId has set executionMode = $SparkIncrementalMode but $input or $output does not support creating Spark DataFrames!")
    }
  }
}
object SparkIncrementalMode {
  private[smartdatalake] val allowedDataTypes = Seq(StringType, LongType, IntegerType, ShortType, FloatType, DoubleType, TimestampType)
}
