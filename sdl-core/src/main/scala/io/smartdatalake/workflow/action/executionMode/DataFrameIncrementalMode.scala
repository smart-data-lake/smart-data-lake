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

package io.smartdatalake.workflow.action.executionMode

import com.typesafe.config.Config
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.definitions.Condition
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.ActionHelper.getOptionalDataFrame
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, SubFeed}

/**
 * Compares max entry in "compare column" between mainOutput and mainInput and incrementally loads the delta.
 * This mode works only with SparkSubFeeds. The filter is not propagated to following actions.
 *
 * @param compareCol          a comparable column name existing in mainInput and mainOutput used to identify the delta. Column content should be bigger for newer records.
 * @param alternativeOutputId optional alternative outputId of DataObject later in the DAG. This replaces the mainOutputId.
 *                            It can be used to ensure processing all partitions over multiple actions in case of errors.
 * @param applyCondition      Condition to decide if execution mode should be applied or not. Define a spark sql expression working with attributes of [[DefaultExecutionModeExpressionData]] returning a boolean.
 *                            Default is to apply the execution mode.
 */
case class DataFrameIncrementalMode(compareCol: String
                                    , override val alternativeOutputId: Option[DataObjectId] = None
                                    , applyCondition: Option[Condition] = None
                                   ) extends ExecutionMode with ExecutionModeWithMainInputOutput {
  private[smartdatalake] override val applyConditionsDef = applyCondition.toSeq

  private[smartdatalake] override def mainInputOutputNeeded: Boolean = alternativeOutputId.isEmpty

  private[smartdatalake] override def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = {
    super.prepare(actionId)
    // check alternativeOutput exists
    alternativeOutput
  }

  private[smartdatalake] override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues, PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    assert(subFeed.isInstanceOf[DataFrameSubFeed], s"($actionId) DataFrameIncrementalMode needs DataFrameSubFeed to operate but received ${subFeed.getClass.getSimpleName}")
    val doApply = evaluateApplyConditions(actionId, subFeed)
      .getOrElse(true) // default is to apply DataFrameIncrementalMode
    if (doApply) {
      val input = mainInput
      val output = alternativeOutput.getOrElse(mainOutput)
      val dfSubFeed = subFeed.asInstanceOf[DataFrameSubFeed]
      import dfSubFeed.companion._
      (input, output) match {
        case (doDfInput: CanCreateDataFrame, doDfOutput: CanCreateDataFrame) =>
          // if data object is new, it might not be able to create a DataFrame
          val dfInputOpt = getOptionalDataFrame(doDfInput, Seq(), dfSubFeed.tpe)
          val dfOutputOpt = getOptionalDataFrame(doDfOutput, Seq(), dfSubFeed.tpe)
          (dfInputOpt, dfOutputOpt) match {
            // if both DataFrames exist, compare and create filter
            case (Some(dfInput), Some(dfOutput)) =>
              val inputColType = dfInput.schema.getDataType(compareCol)
              require(inputColType.isSortable, s"($actionId) Type of compare column $compareCol must be sortable, but is ${inputColType.typeName} in ${doDfInput.id}")
              val outputColType = dfOutput.schema.getDataType(compareCol)
              require(outputColType.isSortable, s"($actionId) Type of compare column $compareCol must be sortable, but is ${inputColType.typeName} in ${doDfInput.id}")
              require(inputColType == outputColType, s"($actionId) Type of compare column $compareCol is different between ${doDfInput.id} (${inputColType.typeName}) and ${doDfOutput.id} (${outputColType.typeName})")
              // get latest values
              val inputLatestValue = dfInput.agg(Seq(max(col(compareCol)).cast(stringType))).collect.head.getAs[String](0)
              val outputLatestValue = dfOutput.agg(Seq(max(col(compareCol)).cast(stringType))).collect.head.getAs[String](0)
              // skip processing if no new data
              val warnMsg = if (inputLatestValue == null) {
                Some(s"($actionId) No increment to process found for ${output.id}: ${input.id} is empty")
              } else if (outputLatestValue == inputLatestValue) {
                Some(s"($actionId) No increment to process found for ${output.id} column $compareCol (lastestValue=$outputLatestValue)")
              } else None
              warnMsg.foreach(msg => throw NoDataToProcessWarning(actionId.id, msg))
              // prepare filter
              val dataFilter = if (outputLatestValue != null) {
                logger.info(s"($actionId) DataFrameIncrementalMode selected increment for writing to ${output.id}: column $compareCol} from $outputLatestValue to $inputLatestValue to process")
                Some(s"$compareCol > cast('$outputLatestValue' as ${inputColType.sql})")
              } else {
                logger.info(s"($actionId) DataFrameIncrementalMode selected all data for writing to ${output.id}: output table is currently empty")
                None
              }
              Some(ExecutionModeResult(filter = dataFilter))
            // select all if output is empty
            case (Some(_), None) =>
              logger.info(s"($actionId) DataFrameIncrementalMode selected all records for writing to ${output.id}, because output DataObject is still empty.")
              Some(ExecutionModeResult())
            // otherwise no data to process
            case _ =>
              val warnMsg = s"($actionId) No increment to process found for ${output.id}, because ${input.id} is still empty."
              throw NoDataToProcessWarning(actionId.id, warnMsg)
          }
        case _ => throw ConfigurationException(s"$actionId has set executionMode = $DataFrameIncrementalMode but $input or $output does not support creating Spark DataFrames!")
      }
    } else None
  }
  override def factory: FromConfigFactory[ExecutionMode] = DataFrameIncrementalMode
}

object DataFrameIncrementalMode extends FromConfigFactory[ExecutionMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DataFrameIncrementalMode = {
    extract[DataFrameIncrementalMode](config)
  }
}