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

package io.smartdatalake.communication.agent

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigSyntax}
import io.smartdatalake.app.{GlobalConfig, SmartDataLakeBuilder}
import io.smartdatalake.communication.message.{SDLMessage, SDLMessageType, StatusUpdate}
import io.smartdatalake.config.ConfigParser.{getActionConfigMap, getConnectionConfigMap, getDataObjectConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{Action, SDLExecutionId}
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.{ExecutionPhase, SubFeed}

import java.time.LocalDateTime

case class AgentController(
                            instanceRegistry: InstanceRegistry,
                            sdlb: SmartDataLakeBuilder
                          ) {
  def handle(message: SDLMessage, agentServerConfig: AgentServerConfig): SDLMessage = {
    message match {
      case SDLMessage(SDLMessageType.AgentInstruction, None, None, agentInstructionOpt) => agentInstructionOpt match {
        case Some(agentInstruction) => {
          implicit val instanceRegistryImplicit: InstanceRegistry = instanceRegistry
          val agentConnectionIds = instanceRegistryImplicit.getConnections.map(_.id.id)

          val configFromString = ConfigFactory.parseString(agentInstruction.hoconConfig, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

          val connectionsToRegister: Map[ConnectionId, Connection] = getConnectionConfigMap(configFromString)
            //Connections defined by the agent should not get overwritten by the connections in the instructions
            .filterNot { case (id, _) => agentConnectionIds.contains(id) }
            .map { case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config)) }

          instanceRegistryImplicit.register(connectionsToRegister)

          val dataObjects: Map[DataObjectId, DataObject] = getDataObjectConfigMap(configFromString)
            .map { case (id, config) => (DataObjectId(id), parseConfigObjectWithId[DataObject](id, config)) }
          instanceRegistryImplicit.register(dataObjects)

          val actions: Map[ActionId, Action] = getActionConfigMap(configFromString)
            .map { case (id, config) => (ActionId(id), parseConfigObjectWithId[Action](id, config)) }

          instanceRegistryImplicit.register(actions)

          val isSimulation = agentInstruction.phase != ExecutionPhase.Exec

          val (x: Seq[SubFeed], y: Map[RuntimeEventState, Int]) = sdlb.exec(agentServerConfig.sdlConfig, SDLExecutionId.executionId1, LocalDateTime.now(), LocalDateTime.now(), Map(), Seq(), Seq(), None, Seq(), simulation = isSimulation, globalConfig = GlobalConfig())(instanceRegistryImplicit)


          SDLMessage(SDLMessageType.StatusUpdate, Some(StatusUpdate(actionId = Some(agentInstruction.actionId), runtimeInfo = None, phase = agentInstruction.phase, finalState = None)))
        }
      }
    }
  }


}
