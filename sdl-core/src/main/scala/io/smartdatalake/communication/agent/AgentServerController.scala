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
import io.smartdatalake.app.{CanBuildAgentSmartDataLakeBuilderConfig, SmartDataLakeBuilder}
import io.smartdatalake.communication.message.{AgentResult, SDLMessage, SDLMessageType}
import io.smartdatalake.config.ConfigParser.{getActionConfigMap, getConnectionConfigMap, getDataObjectConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataobject.DataObject

case class AgentServerController(
                                  sdlb: SmartDataLakeBuilder,
                                  localConnections: Map[ConnectionId, Connection]
                                ) extends SmartDataLakeLogger {

  def handle(message: SDLMessage, sdlbConfig: CanBuildAgentSmartDataLakeBuilderConfig[_]): Option[SDLMessage] = {
    message match {
      case SDLMessage(SDLMessageType.AgentInstruction, None, None, None, agentInstructionOpt, None) => agentInstructionOpt match {
        case Some(agentInstruction) =>
          try {
            // reset instance registry to avoid side effects from previous runs
            sdlb.instanceRegistry.clear()
            implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry

            val receivedConfig = ConfigFactory.parseString(agentInstruction.hoconConfig, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

            val connectionsToRegister: Map[ConnectionId, Connection] = if (sdlbConfig.useOnlyLocalConnectionConfig) {
              assert(sdlbConfig.configuration.nonEmpty, "No local configuration provided, set useOnlyLocalConnectionConfig=false or specify hocon configuration to use when starting the agent server.")
              localConnections
            } else {
              localConnections ++
                getConnectionConfigMap(receivedConfig)
                  .map { case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config)) }
            }
            instanceRegistry.register(connectionsToRegister)

            val dataObjectConfigs = getDataObjectConfigMap(receivedConfig)
            dataObjectConfigs.foreach { case (id, config) =>
              require(config.hasPath("connectionId") || config.hasPath("connection-id"), s"$id is configured without connection. DataObjects without connectionId are not allowed for security reasons.")
            }
            val dataObjects = dataObjectConfigs
              .map { case (id, config) => (DataObjectId(id), parseConfigObjectWithId[DataObject](id, config)) }
            instanceRegistry.register(dataObjects)

            val actions = getActionConfigMap(receivedConfig)
              .map { case (id, config) => (ActionId(id), parseConfigObjectWithId[Action](id, config)) }
            instanceRegistry.register(actions)

            val resultingSubfeeds = sdlb.agentExec(appConfig = sdlbConfig, phase = agentInstruction.phase)
            val resultingDataObjectIdToSchema = resultingSubfeeds.flatMap {
              case subFeed: DataFrameSubFeed => subFeed.schema.map(schema => DataObjectId(subFeed.dataObjectId.id) -> schema.sql)
              case _ => None
            }.toMap

            Some(SDLMessage(SDLMessageType.AgentResult, agentResult = Some(AgentResult(instructionId = agentInstruction.instructionId, phase = agentInstruction.phase, dataObjectIdToSchema = resultingDataObjectIdToSchema))))
          } catch {
            case e: Exception => logger.error("Run failed, sending error message to AgentClient.")
              Some(SDLMessage(SDLMessageType.AgentResult, agentResult = Some(AgentResult(instructionId = agentInstruction.instructionId, phase = agentInstruction.phase, dataObjectIdToSchema = Map(), exception = Some(e)))))
          }
      }
      case _ =>
        logger.warn(s"Cannot process message of type ${message.msgType}")
        None
    }
  }
}
