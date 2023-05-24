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

package io.smartdatalake.communication.agent

import com.microsoft.azure.relay.{HybridConnectionClient, RelayConnectionStringBuilder, TokenProvider}
import io.smartdatalake.communication.message.{SDLMessage, SDLMessageType}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.agent.Agent
import org.json4s.Formats
import org.json4s.jackson.Serialization.{read, writePretty}

import java.net.URI
import java.nio.ByteBuffer

case class AzureRelayAgentClient() extends AgentClient with SmartDataLakeLogger {
  implicit val format: Formats = AgentClient.messageFormat

  override def sendSDLMessage(message: SDLMessage, agent: Agent): Option[SDLMessage] = {
    val connectionParams = new RelayConnectionStringBuilder(agent.url)
    val tokenProvider: TokenProvider = TokenProvider.createSharedAccessSignatureTokenProvider(connectionParams.getSharedAccessKeyName, connectionParams.getSharedAccessKey)
    val client = new HybridConnectionClient(new URI(connectionParams.getEndpoint.toString + connectionParams.getEntityPath), tokenProvider)

    val connection = client.createConnectionAsync.get
    val messageStr = writePretty(message)(AgentClient.messageFormat)
    logger.info("Sending " + messageStr)
    connection.writeAsync(ByteBuffer.wrap(messageStr.getBytes)).join()
    val byteBuffer = connection.readAsync.get
    // If the read operation is still pending when connection closes, the read result returns null.
    val response = if (byteBuffer != null) {
      val response = new String(byteBuffer.array, byteBuffer.arrayOffset, byteBuffer.remaining)
      logger.info("Received " + response)
      try {
        val sdlMessage = read[SDLMessage](response)
        require(sdlMessage.msgType == SDLMessageType.AgentResult, "AgentServer must respond with AgentResult")
        Some(sdlMessage)
      } catch {
        case e: Exception =>
          throw new RuntimeException("Response from AgentServer is not parseable. It probably died. Response=" +  response)
      }
    }
    else {
      Option.empty[SDLMessage]
    }
    connection.closeAsync.join
    response
  }
}
