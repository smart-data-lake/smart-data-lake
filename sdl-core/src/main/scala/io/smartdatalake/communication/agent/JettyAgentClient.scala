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

case class JettyAgentClient(agent: Agent) extends AgentClient {
  val socket = new JettyAgentClientSocket()
  val client = new WebSocketClient
  def sendSDLMessage(message: SDLMessage): Option[SDLMessage] = {
    val uri = URI.create(agent.url)
    client.start()

    val fut = client.connect(socket, uri)
    // Wait for Connect
    val session = fut.get
    session.getRemote.sendString(writePretty(message)(ActionDAGRunState.formats + new EnumNameSerializer(SDLMessageType) + new EnumNameSerializer(ExecutionPhase)))
    val instructionId = message.agentInstruction.get.instructionId
    while (socket.isConnected && !socket.pendingResults.contains(instructionId)) {
      Thread.sleep(1000)
      println(s"Waiting for ${agent.id.id} to finish $instructionId...")
    }
    if (!socket.isConnected) {
      throw new RuntimeException(s"Lost connection to ${agent.id.id}!")
    }
    val response = socket.pendingResults.get(instructionId)
    socket.pendingResults.remove(instructionId)
    closeConnection()
    response
  }

  def closeConnection(): Unit = {
    client.stop()
  }
}
