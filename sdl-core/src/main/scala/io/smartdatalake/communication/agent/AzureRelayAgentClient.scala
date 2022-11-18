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

import com.microsoft.azure.relay.{HybridConnectionChannel, HybridConnectionClient, RelayConnectionStringBuilder, TokenProvider}
import io.smartdatalake.communication.message.{SDLMessage, SDLMessageType}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.{ActionDAGRunState, ExecutionPhase}
import org.json4s.Formats
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.{read, writePretty}

import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import scala.collection.mutable

case class AzureRelayAgentClient() extends AgentClient with SmartDataLakeLogger {
  implicit val format: Formats = ActionDAGRunState.formats + new EnumNameSerializer(SDLMessageType) + new EnumNameSerializer(ExecutionPhase)
  val connectionParams =
    new RelayConnectionStringBuilder(
      "Endpoint=sb://relay-tbb-test.servicebus.windows.net/;EntityPath=relay-tbb-test-connection;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=" + System.getenv("SharedAccessKey"))

  val tokenProvider: TokenProvider = TokenProvider.createSharedAccessSignatureTokenProvider(connectionParams.getSharedAccessKeyName, connectionParams.getSharedAccessKey)
  val client = new HybridConnectionClient(new URI(connectionParams.getEndpoint.toString + connectionParams.getEntityPath), tokenProvider)

  override def sendSDLMessage(message: SDLMessage): Option[SDLMessage] = {
      val connection = client.createConnectionAsync.get
      val messageStr = writePretty(message)(ActionDAGRunState.formats + new EnumNameSerializer(SDLMessageType) + new EnumNameSerializer(ExecutionPhase))

      connection.writeAsync(ByteBuffer.wrap(messageStr.getBytes)).join()
      val byteBuffer = connection.readAsync.get
      // If the read operation is still pending when connection closes, the read result returns null.
      val response =
        if (byteBuffer != null) {
          val response = new String(byteBuffer.array, byteBuffer.arrayOffset, byteBuffer.remaining)
          logger.info("Received TEXT message: " + response)
          val sdlMessage = read[SDLMessage](response)
          require(sdlMessage.msgType == SDLMessageType.AgentResult) //TODO treat other cases

         /* sdlMessage.msgType match {
            case SDLMessageType.AgentResult =>
              pendingResults.put(sdlMessage.agentResult.get.instructionId, sdlMessage)
            case SDLMessageType.EndConnection =>
              logger.info(this + ": received EndConnection request, closing connection")
              connection.close()
            case _ =>*/
          Some(sdlMessage)
          }
        else {
          Option.empty[SDLMessage]
        }
      connection.closeAsync.join
      response
  }
}
