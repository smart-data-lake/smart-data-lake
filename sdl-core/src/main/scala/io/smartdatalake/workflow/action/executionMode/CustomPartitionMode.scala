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
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.workflow.dataobject.{CanHandlePartitions, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SubFeed}

/**
 * Execution mode to create custom partition execution mode logic.
 *
 * Define a function which receives main input&output DataObject and returns partition values to process as `Seq[Map[String,String]]`
 *
 * @param className           class name implementing trait [[CustomPartitionModeLogic]]
 * @param alternativeOutputId optional alternative outputId of DataObject later in the DAG. This replaces the mainOutputId.
 *                            It can be used to ensure processing all partitions over multiple actions in case of errors.
 * @param options             Options specified in the configuration for this execution mode
 */
case class CustomPartitionMode(className: String, override val alternativeOutputId: Option[DataObjectId] = None, options: Option[Map[String, String]] = None)
  extends ExecutionMode with ExecutionModeWithMainInputOutput {
  override def mainInputOutputNeeded: Boolean = alternativeOutputId.isEmpty

  private val impl = CustomCodeUtil.getClassInstanceByName[CustomPartitionModeLogic](className)

  override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues, PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    val output = alternativeOutput.getOrElse(mainOutput)
    (mainInput, output) match {
      case (input: CanHandlePartitions, output: CanHandlePartitions) =>
        val partitionValuesOpt = impl.apply(options.getOrElse(Map()), actionId, input, output, subFeed.partitionValues.map(_.getMapString), context)
          .map(_.map(pv => PartitionValues(pv)))
        partitionValuesOpt.map(pvs => ExecutionModeResult(inputPartitionValues = pvs, outputPartitionValues = pvs))
      case (_: CanHandlePartitions, _) =>
        throw ConfigurationException(s"$actionId has set executionMode = CustomPartitionMode but ${output.id} does not support partitions!")
      case (_, _) =>
        throw ConfigurationException(s"$actionId has set executionMode = CustomPartitionMode but $mainInput does not support partitions!")
    }
  }
  override def factory: FromConfigFactory[ExecutionMode] = CustomPartitionMode
}

object CustomPartitionMode extends FromConfigFactory[ExecutionMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomPartitionMode = {
    extract[CustomPartitionMode](config)
  }
}

/**
 * A trait to be implemented for custom partition execution mode logic, to be used within execution mode [[CustomPartitionMode]].
 */
trait CustomPartitionModeLogic {
  /**
   * Function to implement to define custom partition execution mode logic.
   *
   * @param options Options specified in the configuration for this execution mode
   * @param actionId Id of the action this execution mode is associated with
   * @param input Input data object. Use input.listPartitions to get current partitions of the input DataObject
   * @param output Output data object. Use output.listPartitions to get current partitions of the input DataObject
   * @param givenPartitionValues Partition values specified with command line (start action) or passed from previous action
   * @param context Current ActionPipelineContext. This includes feed name, SmartDataLakeBuilderConfig, execution phase and much more.
   * @return Partitions selected or none, if the execution mode should not be applied.
   */
  def apply(options: Map[String,String], actionId: ActionId, input: DataObject with CanHandlePartitions, output: DataObject with CanHandlePartitions, givenPartitionValues: Seq[Map[String,String]], context: ActionPipelineContext): Option[Seq[Map[String,String]]]
}
