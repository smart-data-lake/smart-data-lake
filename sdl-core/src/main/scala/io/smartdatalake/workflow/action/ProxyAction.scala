/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action

import io.smartdatalake.communication.agent.AgentClient
import io.smartdatalake.config.{FromConfigFactory, SdlConfigObject}
import io.smartdatalake.definitions.{Condition, ExecutionMode}
import io.smartdatalake.util.dag.DAGHelper.NodeId
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.{ActionPipelineContext, SubFeed}

case class ProxyAction(wrappedAction: Action, override val id: SdlConfigObject.ActionId) extends Action {

  override def exec(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = {

    val agentClient = AgentClient(wrappedAction.remoteActionConfig.get)
    //TODO change when refactoring config file to include agents sections

    val hoconInstructions = AgentClient.prepareHoconInstructions(wrappedAction, context.instanceRegistry.getConnections)
    agentClient.sendSDLMessage(hoconInstructions)

    while (agentClient.socket.actionStillRunning) {
      Thread.sleep(1000)
      println("waiting...")
    }

    wrappedAction.exec(subFeeds)
  }

  /**
   * A unique identifier for this instance.
   */
  //override def id: SdlConfigObject.ConfigObjectId = ProxyActionId(action.id.id, action.remoteActionConfig.get.remoteAgentURL)

  override def factory: FromConfigFactory[Action] = wrappedAction.factory

  override def nodeId: NodeId = wrappedAction.nodeId

  override def atlasName: String = wrappedAction.atlasName

  override def metadata: Option[ActionMetadata] = wrappedAction.metadata

  override def inputs: Seq[DataObject] = wrappedAction.inputs

  override def outputs: Seq[DataObject] = wrappedAction.outputs

  override def executionCondition: Option[Condition] = wrappedAction.executionCondition

  override def executionMode: Option[ExecutionMode] = wrappedAction.executionMode

  override def metricsFailCondition: Option[String] = wrappedAction.metricsFailCondition

  override def init(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = wrappedAction.init(subFeeds)
}
