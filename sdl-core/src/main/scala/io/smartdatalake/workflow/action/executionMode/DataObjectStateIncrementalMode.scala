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
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.workflow.dataobject.{CanCreateIncrementalOutput, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataObjectState, SubFeed}

/**
 * An execution mode for incremental processing by remembering DataObjects state from last increment.
 */
case class DataObjectStateIncrementalMode() extends ExecutionMode {
  private var inputsWithIncrementalOutput: Seq[DataObject with CanCreateIncrementalOutput] = Seq()

  override def preInit(subFeeds: Seq[SubFeed], dataObjectsState: Seq[DataObjectState])(implicit context: ActionPipelineContext): Unit = {
    // initialize dataObjectsState
    val unrelatedStateDataObjectIds = dataObjectsState.map(_.dataObjectId).diff(subFeeds.map(_.dataObjectId))
    assert(unrelatedStateDataObjectIds.isEmpty, s"Got state for unrelated DataObjects ${unrelatedStateDataObjectIds.mkString(", ")}")
    // assert SDL is started with state
    assert(context.appConfig.statePath.isDefined, s"SmartDataLakeBuilder must be started with state path set. Please specify location of state with parameter '--state-path'.")
    // set DataObjects state
    inputsWithIncrementalOutput = subFeeds.map(s => context.instanceRegistry.get[DataObject](s.dataObjectId)).flatMap {
      case input: DataObject with CanCreateIncrementalOutput =>
        input.setState(dataObjectsState.find(_.dataObjectId == input.id).map(_.state))
        Some(input)
      case _ => None
    }
    assert(inputsWithIncrementalOutput.nonEmpty, s"DataObjectStateIncrementalMode needs at least one input DataObject implementing CanCreateIncrementalOutput")
  }

  override def postExec(actionId: ActionId, mainInput: DataObject, mainOutput: DataObject, mainInputSubFeed: SubFeed, mainOutputSubFeed: SubFeed)(implicit context: ActionPipelineContext): Unit = {
    // update DataObjects incremental state in DataObjectStateIncrementalMode if streaming
    if (context.appConfig.streaming) {
      inputsWithIncrementalOutput.foreach(i => i.setState(i.getState))
    }
  }
  override def factory: FromConfigFactory[ExecutionMode] = ProcessAllMode
}

object DataObjectStateIncrementalMode extends FromConfigFactory[ExecutionMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DataObjectStateIncrementalMode = {
    extract[DataObjectStateIncrementalMode](config)
  }
}