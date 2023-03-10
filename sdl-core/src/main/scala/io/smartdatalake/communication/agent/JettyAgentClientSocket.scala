//
// ========================================================================
// Copyright (c) Mort Bay Consulting Pty Ltd and others.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License v. 2.0 which is available at
// https://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
// ========================================================================
//

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
    logger.info(this + ": Socket Closed: [" + statusCode + "] " + reason)

  }

  override def onWebSocketError(cause: Throwable): Unit = {
    super.onWebSocketError(cause)
    logger.error(s"Socket $this was closed with error ${cause.printStackTrace(System.err)}")
  }


}
