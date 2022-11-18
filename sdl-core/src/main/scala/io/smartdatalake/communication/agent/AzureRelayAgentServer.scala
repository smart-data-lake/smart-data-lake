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

import io.smartdatalake.util.misc.SmartDataLakeLogger

import java.net.URI
import java.net.URISyntaxException
import java.nio.ByteBuffer
import java.util.Scanner
import java.util.concurrent.CompletableFuture
import com.microsoft.azure.relay.{HybridConnectionChannel, HybridConnectionListener, RelayConnectionStringBuilder, TokenProvider}
import io.smartdatalake.communication.message.{SDLMessage, SDLMessageType}
import io.smartdatalake.workflow.{ActionDAGRunState, ExecutionPhase}
import org.json4s.Formats
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.{read, writePretty}

object AzureRelayAgentServer extends AgentServer with SmartDataLakeLogger {
  implicit val format: Formats = ActionDAGRunState.formats + new EnumNameSerializer(SDLMessageType) + new EnumNameSerializer(ExecutionPhase)

  val connectionParams =
    new RelayConnectionStringBuilder (
      "Endpoint=sb://relay-tbb-test.servicebus.windows.net/;EntityPath=relay-tbb-test-connection;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=" + System.getenv("SharedAccessKey"))

  override def start(config: JettyAgentServerConfig, agentController: AgentServerController): Unit = {
    val tokenProvider = TokenProvider.createSharedAccessSignatureTokenProvider(connectionParams.getSharedAccessKeyName, connectionParams.getSharedAccessKey)
    val listener = new HybridConnectionListener(new URI(connectionParams.getEndpoint.toString + connectionParams.getEntityPath), tokenProvider)

    listener.openAsync.join
    System.out.println("Listener is online. Press ENTER to terminate this program.")


    def doStuff(connection: HybridConnectionChannel): Unit =
    {
      // connection may be null if the listener is closed before receiving a connection
      if (connection != null) {
        System.out.println("New session connected.")
        while ( {
          connection.isOpen
        }) {
          val bytesReceived = connection.readAsync.join
          // If the read operation is still pending when connection closes, the read result as null.
          if (bytesReceived.remaining > 0) {
            val message = new String(bytesReceived.array, bytesReceived.arrayOffset, bytesReceived.remaining)
            logger.info("Received TEXT message: " + message)
            val sdlMessage = read[SDLMessage](message)
            val responseMessageOpt = agentController.handle(sdlMessage, config)
            if (responseMessageOpt.isDefined) sendSDLMessage(responseMessageOpt.get, connection)
            else closeConnection(connection)
          }
        }
        System.out.println("Session disconnected.")
      } else {
        logger.info("Connection is null!")
      }
    }
/*    while ( {
      listener.isOnline
    }) */{ // If listener closes, then listener.acceptConnectionAsync() will complete with null after closing down
      listener.acceptConnectionAsync().thenAccept(doStuff).join

      logger.info("Reached end of AzureRelayAgentServer.start")
    }
  }

  def sendSDLMessage(sdlMessage: SDLMessage, connection: HybridConnectionChannel): Unit = {
    val outputString = writePretty(sdlMessage)
    val msgToSend = ByteBuffer.wrap(outputString.getBytes)
    connection.writeAsync(msgToSend)
  }

  def closeConnection(connection: HybridConnectionChannel): Unit = {
    logger.info(this + ": received EndConnection request, closing connection")
    connection.close()
  }
}
