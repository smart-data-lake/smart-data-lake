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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.{ActionPipelineContext, SubFeed}

/**
 * Execution mode to create custom execution mode logic.
 * Define a function which receives main input&output DataObject and returns execution mode result
 *
 * @param className           class name implementing trait [[CustomModeLogic]]
 * @param alternativeOutputId optional alternative outputId of DataObject later in the DAG. This replaces the mainOutputId.
 *                            It can be used to ensure processing over multiple actions in case of errors.
 * @param options             Options specified in the configuration for this execution mode
 */
case class CustomMode(className: String, override val alternativeOutputId: Option[DataObjectId] = None, options: Option[Map[String, String]] = None)
  extends ExecutionMode with ExecutionModeWithMainInputOutput {
  override def mainInputOutputNeeded: Boolean = alternativeOutputId.isEmpty

  private val impl = CustomCodeUtil.getClassInstanceByName[CustomModeLogic](className)

  override def apply(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, subFeed: SubFeed
                                            , partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues, PartitionValues])
                                           (implicit context: ActionPipelineContext): Option[ExecutionModeResult] = {
    val output = alternativeOutput.getOrElse(mainOutput)
    impl.apply(options.getOrElse(Map()), actionId, mainInput, output, subFeed.partitionValues.map(_.getMapString), context)
  }
  override def factory: FromConfigFactory[ExecutionMode] = CustomMode
}

object CustomMode extends FromConfigFactory[ExecutionMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomMode = {
    extract[CustomMode](config)
  }
}

trait CustomModeLogic {
  def apply(options: Map[String,String], actionId: ActionId, input: DataObject, output: DataObject, givenPartitionValues: Seq[Map[String,String]], context: ActionPipelineContext): Option[ExecutionModeResult]
}