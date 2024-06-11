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

import com.microsoft.azure.relay.{HybridConnectionChannel, HybridConnectionListener, RelayConnectionStringBuilder, TokenProvider}
import io.smartdatalake.app.{LocalAzureRelayAgentSmartDataLakeBuilderConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.communication.message.SDLMessage
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.json4s.Formats
import org.json4s.jackson.Serialization.{read, writePretty}

import java.net.URI
import java.nio.ByteBuffer

object AzureRelayAgentServer extends SmartDataLakeLogger {
  implicit val format: Formats = AgentClient.messageFormat

  def start(localAzureRelayAgentConfig: LocalAzureRelayAgentSmartDataLakeBuilderConfig, agentController: AgentServerController): Unit = {
    val connectionParams = new RelayConnectionStringBuilder(localAzureRelayAgentConfig.azureRelayURL.get + System.getenv("SharedAccessKey"))

    val tokenProvider = TokenProvider.createSharedAccessSignatureTokenProvider(connectionParams.getSharedAccessKeyName, connectionParams.getSharedAccessKey)
    val listener = new HybridConnectionListener(new URI(connectionParams.getEndpoint.toString + connectionParams.getEntityPath), tokenProvider)

    listener.openAsync.join
    logger.info("Listener is online.")

    def handleConnection(connection: HybridConnectionChannel): Unit = {
      // connection may be null if the listener is closed before receiving a connection
      if (connection != null) {
        logger.info("New session connected.")
        while ( {
          connection.isOpen
        }) {
          val bytesReceived = connection.readAsync.join
          // If the read operation is still pending when connection closes, the read result as null.
          if (bytesReceived.remaining > 0) {
            val message = new String(bytesReceived.array, bytesReceived.arrayOffset, bytesReceived.remaining)
            logger.info("Received " + message)
            val sdlMessage = read[SDLMessage](message)
            val sdlConfig = SmartDataLakeBuilderConfig(localAzureRelayAgentConfig.feedSel, applicationName = localAzureRelayAgentConfig.applicationName, configuration = localAzureRelayAgentConfig.configuration,
              partitionValues = localAzureRelayAgentConfig.partitionValues, multiPartitionValues = localAzureRelayAgentConfig.multiPartitionValues,
              parallelism = localAzureRelayAgentConfig.parallelism, statePath = localAzureRelayAgentConfig.statePath, overrideJars = localAzureRelayAgentConfig.overrideJars
              , test = localAzureRelayAgentConfig.test, streaming = localAzureRelayAgentConfig.streaming)
            val responseMessageOpt = agentController.handle(sdlMessage, sdlConfig)
            if (responseMessageOpt.isDefined) {
              sendSDLMessage(responseMessageOpt.get, connection)
              if(responseMessageOpt.get.agentResult.get.exception.isDefined){
                throw(responseMessageOpt.get.agentResult.get.exception.get)
              }
            }
            else closeConnection(connection)
          }
        }
        logger.info("Session disconnected.")
      } else {
        logger.info("Connection is null!")
      }
    }
    while ( {
      listener.isOnline
    }) { // If listener closes, then listener.acceptConnectionAsync() will complete with null after closing down
      listener.acceptConnectionAsync().thenAccept(handleConnection).join
    }
  }

  private def sendSDLMessage(sdlMessage: SDLMessage, connection: HybridConnectionChannel): Unit = {
    val outputString = writePretty(sdlMessage)
    logger.info("Sending " + outputString)
    val msgToSend = ByteBuffer.wrap(outputString.getBytes)
    connection.writeAsync(msgToSend)
  }

  private def closeConnection(connection: HybridConnectionChannel): Unit = {
    logger.info(this.toString + ": received EndConnection request, closing connection")
    connection.close()
  }
}
