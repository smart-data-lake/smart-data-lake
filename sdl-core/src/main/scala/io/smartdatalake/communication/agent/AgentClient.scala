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

import com.typesafe.config.{ConfigObject, ConfigRenderOptions, ConfigValueFactory}
import io.smartdatalake.communication.message.{AgentInstruction, SDLMessage, SDLMessageType}
import io.smartdatalake.config.ConfigParser.{CONFIG_SECTION_ACTIONS, CONFIG_SECTION_CONNECTIONS, CONFIG_SECTION_DATAOBJECTS}
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.agent.Agent
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.{ActionDAGRunState, ExecutionPhase}
import org.eclipse.jetty.websocket.client.WebSocketClient
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.writePretty

import java.net.URI
import scala.collection.JavaConverters._

object AgentClient {
  def prepareHoconInstructions(actionToSerialize: Action, connectionsToSerialize: Seq[Connection], agent: Agent, executionPhase: ExecutionPhase): SDLMessage = {
    val allConnectedDataObjects = actionToSerialize.inputs ++ actionToSerialize.outputs
    val allConnectionIds = allConnectedDataObjects.map(_._config.get).filter(_.hasPath("connectionId")).map(_.getValue("connectionId").render(ConfigRenderOptions.concise().setJson(false)))
    val relevantTopLevelConnections: Seq[Connection] =
      connectionsToSerialize.filter(connectionId => allConnectionIds.contains(connectionId.id.id) && !agent.connections.contains(connectionId.id.id))

    val relevantConnections = relevantTopLevelConnections ++ agent.connections.values

    val hoconConfigToSend: ConfigObject = ConfigValueFactory.fromMap(
      Map(CONFIG_SECTION_ACTIONS ->
        ConfigValueFactory.fromMap(
          Map(actionToSerialize.id.id ->
            ConfigValueFactory.fromAnyRef(actionToSerialize._config.get.root())).asJava)
        ,
        CONFIG_SECTION_DATAOBJECTS ->
          ConfigValueFactory.fromMap(
            allConnectedDataObjects.map(dataObject =>
              dataObject.id.id -> ConfigValueFactory.fromAnyRef(dataObject._config.get.root())).toMap
              .asJava),

        CONFIG_SECTION_CONNECTIONS ->
          ConfigValueFactory.fromMap(
            relevantConnections.map(connection =>
              connection.id.id -> ConfigValueFactory.fromAnyRef(connection._config.get.root())).toMap
              .asJava)).asJava
    )
    val hoconString = hoconConfigToSend.render(ConfigRenderOptions.concise().setJson(false))
    SDLMessage(msgType = SDLMessageType.AgentInstruction, agentInstruction = Some(AgentInstruction(actionToSerialize.id.id, executionPhase, hoconString)))
  }
}

case class AgentClient(agent: Agent) {
  val socket = new AgentClientSocket()

  def sendSDLMessage(message: SDLMessage): Unit = {
    val uri = URI.create(agent.url)
    val client = new WebSocketClient

    client.start()

    val fut = client.connect(socket, uri)
    // Wait for Connect
    val session = fut.get

    session.getRemote.sendString(writePretty(message)(ActionDAGRunState.formats + new EnumNameSerializer(SDLMessageType) + new EnumNameSerializer(ExecutionPhase)))

  }
}
