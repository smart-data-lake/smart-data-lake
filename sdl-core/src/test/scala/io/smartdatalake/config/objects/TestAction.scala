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
package io.smartdatalake.config.objects

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Condition
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.GenericDfTransformer
import io.smartdatalake.workflow.action.{Action, ActionMetadata}
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, DataObject, HousekeepingMode, TransactionalTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SubFeed}

/**
 * A dummy [[Action]] for unit tests.
 *
 * @param id the unique identifier of this object.
 * @param inputId  input [[DataObject]] id
 * @param outputId output [[DataObject]] id
 * @param arg1  some (optional) dummy argument
 */
case class TestAction(override val id: ActionId,
                      inputId: DataObjectId,
                      outputId: DataObjectId,
                      arg1: Option[String],
                      executionMode: Option[ExecutionMode] = None,
                      housekeepingMode: Option[HousekeepingMode] = None,
                      transformers: Seq[GenericDfTransformer] = Seq(),
                      override val executionCondition: Option[Condition] = None,
                      override val metricsFailCondition: Option[String] = None,
                      override val metadata: Option[ActionMetadata] = None
                     )(implicit instanceRegistry: InstanceRegistry)
  extends Action {

  override def init(subFeed: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = { /*NOP*/ Seq() }
  override def exec(subFeed: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = { /*NOP*/ Seq() }

  private[config] val input = getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  private[config] val output = getOutputDataObject[TestDataObject](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[TransactionalTableDataObject] = Seq(output)
  override val recursiveInputs:Seq[DataObject with CanCreateDataFrame] = Seq()

  override def factory: FromConfigFactory[Action] = TestAction
}

object TestAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): TestAction = {
    extract[TestAction](config)
  }
}
