/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.agent

import com.typesafe.config.Config
import io.smartdatalake.communication.agent.{AgentClient, JettyAgentClientSocket}
import io.smartdatalake.communication.message.{AgentResult, SDLMessage}
import io.smartdatalake.config.SdlConfigObject.AgentId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.Connection
import org.eclipse.jetty.websocket.client.WebSocketClient

import java.net.URI

/**
 * Simple, unsecured SDLB Remote [[Agent]] for development use that communicates via a plain Jetty Websocket.
 * See the class SmartDataLakeBuilderAgentTest for an example.
 *
 * @param url Connection URL on how the agent can be reached, example: "ws://localhost:4441/ws/"
 */
case class JettyAgent(override val id: AgentId, url: String, override val connections: Map[String, Connection] = Map())
  extends Agent with AgentClient with SmartDataLakeLogger {

  override def factory: FromConfigFactory[Agent] = JettyAgent

  private val uri = URI.create(url)

  override def getClient: AgentClient = this

  override def sendSDLMessage(message: SDLMessage)(implicit context: ActionPipelineContext): AgentResult = {
    assert(message.agentInstruction.isDefined, s"($id) Message must contain an agent instruction")
    val socket = new JettyAgentClientSocket()
    val client = new WebSocketClient
    client.start()
    val session = client.connect(socket, uri).get
    val messageStr = message.toJson
    logger.info(s"($id) Sending " + messageStr)
    session.getRemote.sendString(messageStr)
    val instructionId = message.agentInstruction.get.instructionId
    while (socket.isConnected && socket.agentServerResponse.isEmpty) {
      Thread.sleep(1000)
      logger.info(s"($id) Waiting to finish $instructionId...")
    }
    if (!socket.isConnected) {
      throw new RuntimeException(s"($id) Lost connection!")
    }
    client.stop()
    val response = socket.agentServerResponse.get
    assert(response.agentResult.isDefined, s"($id) Agent response must be a message of type AgentResult, but received ${response} for instruction $instructionId")
    response.agentResult.get
  }
}

object JettyAgent extends FromConfigFactory[Agent] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JettyAgent = {
    extract[JettyAgent](config)
  }
}