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

import io.smartdatalake.communication.message.{SDLMessage, SDLMessageType}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.agent.Agent
import io.smartdatalake.workflow.{ActionDAGRunState, ExecutionPhase}
import org.eclipse.jetty.websocket.client.WebSocketClient
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.writePretty

import java.net.URI

case class JettyAgentClient() extends AgentClient with SmartDataLakeLogger {
  def sendSDLMessage(message: SDLMessage, agent: Agent): Option[SDLMessage] = {
    val socket = new JettyAgentClientSocket()
    val client = new WebSocketClient
    val uri = URI.create(agent.url)
    client.start()

    val session = client.connect(socket, uri).get
    val messageStr = writePretty(message)(ActionDAGRunState.formats + new EnumNameSerializer(SDLMessageType) + new EnumNameSerializer(ExecutionPhase))
    logger.info("Sending " + messageStr)
    session.getRemote.sendString(messageStr)
    val instructionId = message.agentInstruction.get.instructionId
    while (socket.isConnected && socket.agentServerResponse.isEmpty) {
      Thread.sleep(1000)
      logger.info(s"Waiting for ${agent.id.id} to finish $instructionId...")
    }
    if (!socket.isConnected) {
      throw new RuntimeException(s"Lost connection to ${agent.id.id}!")
    }
    client.stop()
    socket.agentServerResponse
  }
}
