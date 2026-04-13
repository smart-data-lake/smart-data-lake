/*
 * sdl-azure - Build your data lake the smart way.
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

import com.microsoft.azure.relay.{HybridConnectionClient, RelayConnectionStringBuilder, TokenProvider}
import com.typesafe.config.Config
import io.smartdatalake.communication.agent.AgentClient
import io.smartdatalake.communication.message.{AgentResult, SDLMessage, SDLMessageType}
import io.smartdatalake.config.SdlConfigObject.AgentId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.Connection

import java.net.URI
import java.nio.ByteBuffer
/**
 *  [[Agent]] that communicates via a Azure Relay Service.
 * See the class SmartDataLakeBuilderAzureRelayAgentIT for an example.
 *
 * @param url         Connection URL on how the agent can be reached. See io.smartdatalake.app.SmartDataLakeBuilderAzureRelayAgentIT#azureRelayUrl for an example.
 */
case class AzureRelayAgent(override val id: AgentId, url: String, override val connections: Map[String, Connection] = Map())
  extends Agent with AgentClient with SmartDataLakeLogger {

  override def factory: FromConfigFactory[Agent] = AzureRelayAgent

  override def getClient: AgentClient = this

  override def sendSDLMessage(message: SDLMessage)(implicit context: ActionPipelineContext): AgentResult = {
    val connectionParams = new RelayConnectionStringBuilder(url)
    val tokenProvider: TokenProvider = TokenProvider.createSharedAccessSignatureTokenProvider(connectionParams.getSharedAccessKeyName, connectionParams.getSharedAccessKey)
    val client = new HybridConnectionClient(new URI(connectionParams.getEndpoint.toString + connectionParams.getEntityPath), tokenProvider)

    val connection = client.createConnectionAsync.get
    val messageStr = message.toJson
    logger.info("Sending " + messageStr)
    connection.writeAsync(ByteBuffer.wrap(messageStr.getBytes)).join()
    val byteBuffer = connection.readAsync.get
    // If the read operation is still pending when connection closes, the read result returns null.
    val response = if (byteBuffer != null) {
      val response = new String(byteBuffer.array, byteBuffer.arrayOffset, byteBuffer.remaining)
      logger.info("Received " + response)
      try {
        val sdlMessage = SDLMessage.fromJson(response)
        require(sdlMessage.msgType == SDLMessageType.AgentResult, "AgentServer must respond with AgentResult")
        sdlMessage
      } catch {
        case e: Exception =>
          throw new RuntimeException("Response from AgentServer is not parseable. It probably died. Response=" + response)
      }
    } else {
      throw new RuntimeException("Response from AgentServer was empty. Maybe the read operation was still pending when the connection closed?")
    }
    connection.closeAsync.join
    assert(response.agentResult.isDefined, s"($id) Agent response must be a message of type AgentResult, but received ${response}")
    response.agentResult.get
  }
}

object AzureRelayAgent extends FromConfigFactory[Agent] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AzureRelayAgent = {
    extract[AzureRelayAgent](config)
  }
}


