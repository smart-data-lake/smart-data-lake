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
import io.smartdatalake.communication.message.{AgentResult, SDLMessage, SDLMessageType}
import io.smartdatalake.config.ConfigParser.{getActionConfigMap, getConnectionConfigMap, getDataObjectConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.workflow.action.{Action, SDLExecutionId}
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.DataObject

import java.time.LocalDateTime

case class AgentController(
                            instanceRegistry: InstanceRegistry,
                            sdlb: SmartDataLakeBuilder
                          ) {
  def handle(message: SDLMessage, agentServerConfig: AgentServerConfig): SDLMessage = {
    message match {
      case SDLMessage(SDLMessageType.AgentInstruction, None, None, agentInstructionOpt, None) => agentInstructionOpt match {
        case Some(agentInstruction) => {
          implicit val instanceRegistryImplicit: InstanceRegistry = instanceRegistry
          val agentConnectionIds = instanceRegistryImplicit.getConnections.map(_.id.id)

          val configFromString = ConfigFactory.parseString(agentInstruction.hoconConfig, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

          val connectionsToRegister: Map[ConnectionId, Connection] = getConnectionConfigMap(configFromString)
            //Connections defined by the agent should not get overwritten by the connections in the instructions
            //       .filterNot { case (id, _) => agentConnectionIds.contains(id) }
            .map { case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config)) }

          instanceRegistryImplicit.register(connectionsToRegister)

          val dataObjects: Map[DataObjectId, DataObject] = getDataObjectConfigMap(configFromString)
            .map { case (id, config) => (DataObjectId(id), parseConfigObjectWithId[DataObject](id, config)) }
          instanceRegistryImplicit.register(dataObjects)

          val actions: Map[ActionId, Action] = getActionConfigMap(configFromString)
            .map { case (id, config) => (ActionId(id), parseConfigObjectWithId[Action](id, config)) }

          instanceRegistryImplicit.register(actions)

          val resultingSubfeeds = sdlb.agentExec(agentServerConfig.sdlConfig, agentInstruction.phase, SDLExecutionId.executionId1, LocalDateTime.now(), LocalDateTime.now(), Seq(), Seq(), None, Seq(), simulation = false, globalConfig = GlobalConfig())(instanceRegistryImplicit)

          val resultingDataObjectIdToSchema = resultingSubfeeds.map(subfeed => subfeed.dataObjectId.id -> subfeed.asInstanceOf[SparkSubFeed].dataFrame.get.inner.schema.json).toMap

          SDLMessage(SDLMessageType.AgentResult, agentResult = Some(AgentResult(actionId = agentInstruction.actionId, phase = agentInstruction.phase, dataObjectIdToSchema = resultingDataObjectIdToSchema)))
        }
      }
    }
  }


}
