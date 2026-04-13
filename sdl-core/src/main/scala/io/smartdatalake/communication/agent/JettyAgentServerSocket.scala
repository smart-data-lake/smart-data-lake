/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.communication.agent

import io.smartdatalake.app.LocalJettyAgentSmartDataLakeBuilderConfig
import io.smartdatalake.communication.message.{SDLMessage, SDLMessageMetadata, SDLMessageType}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.eclipse.jetty.websocket.api.{Session, StatusCode, WebSocketAdapter}


class JettyAgentServerSocket(agentConfig: LocalJettyAgentSmartDataLakeBuilderConfig, agentController: AgentServerController) extends WebSocketAdapter with SmartDataLakeLogger {

  override def onWebSocketConnect(sess: Session): Unit = {

    super.onWebSocketConnect(sess)
    logger.info(s"Socket $this Connected")
    val message = SDLMessage(
      msgType = SDLMessageType.StartConnection,
      messageMetadata = Some(SDLMessageMetadata(this.toString, sess.getRemoteAddress.toString))
    )
    sess.getRemote.sendString(message.toJson)
    sess.getPolicy.setMaxTextMessageBufferSize(1000000)
  }


  override def onWebSocketText(message: String): Unit = {
    super.onWebSocketText(message)
    logger.info("Received " + message)
    val sdlMessage = SDLMessage.fromJson(message)
    val responseMessageOpt = agentController.handle(sdlMessage, agentConfig)
    if(responseMessageOpt.isDefined) sendSDLMessage(responseMessageOpt.get)
    else closeConnection()
  }

  def sendSDLMessage(sdlMessage: SDLMessage): Unit = {
    val outputString = sdlMessage.toJson
    logger.info("Sending" + outputString)
    getSession.getRemote.sendString(outputString)
  }

  def closeConnection(): Unit = {
    logger.info(this.toString + ": received EndConnection request, closing connection")
    getSession.close(StatusCode.NORMAL, "Connection closed by " + this)
  }

  override def onWebSocketClose(statusCode: Int, reason: String): Unit = {
    super.onWebSocketClose(statusCode, reason)
    logger.info("Server says: Socket Closed: [" + statusCode + "] " + reason)

  }

  override def onWebSocketError(cause: Throwable): Unit = {
    super.onWebSocketError(cause)
    logger.error(s"Socket $this was closed with error ${cause.printStackTrace(System.err)}")
  }

  override def onWebSocketBinary(payload: Array[Byte], offset: Int, len: Int): Unit = {

  }
}
