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

import io.smartdatalake.communication.message.{SDLMessage, SDLMessageType}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.eclipse.jetty.websocket.api.{Session, WebSocketAdapter}
import org.json4s.Formats
import org.json4s.jackson.Serialization.read

class JettyAgentClientSocket() extends WebSocketAdapter with SmartDataLakeLogger {

  var agentServerResponse: Option[SDLMessage] = Option.empty[SDLMessage]

  override def onWebSocketConnect(sess: Session): Unit = {
    super.onWebSocketConnect(sess)
  }

  override def onWebSocketText(message: String): Unit = {
    logger.info("Received " + message)
    super.onWebSocketText(message)
    implicit val format: Formats = AgentClient.messageFormat
    val sdlMessage = read[SDLMessage](message)
    sdlMessage.msgType match {
      case SDLMessageType.AgentResult =>
        agentServerResponse = Some(sdlMessage)
      case _ =>
    }
  }

  override def onWebSocketClose(statusCode: Int, reason: String): Unit = {
    super.onWebSocketClose(statusCode, reason)
    logger.info(this.toString + ": Socket Closed: [" + statusCode + "] " + reason)

  }

  override def onWebSocketError(cause: Throwable): Unit = {
    super.onWebSocketError(cause)
    logger.error(s"Socket $this was closed with error ${cause.printStackTrace(System.err)}")
  }


}
