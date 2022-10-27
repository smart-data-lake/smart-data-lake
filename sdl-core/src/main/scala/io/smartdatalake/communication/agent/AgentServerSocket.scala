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

import com.sun.xml.internal.ws.api.message.MessageMetadata
import io.smartdatalake.communication.message.SDLMessageType.EndConnection
import io.smartdatalake.communication.message.{SDLMessage, SDLMessageType, SDLMessageMetadata}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.{ActionDAGRunState, ExecutionPhase}
import org.eclipse.jetty.websocket.api.{Session, StatusCode, WebSocketAdapter}
import org.json4s.Formats
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.{read, writePretty}


class AgentServerSocket(config: AgentServerConfig, agentController: AgentServerController) extends WebSocketAdapter with SmartDataLakeLogger {
  implicit val format: Formats = ActionDAGRunState.formats + new EnumNameSerializer(SDLMessageType) + new EnumNameSerializer(ExecutionPhase)

  override def onWebSocketConnect(sess: Session): Unit = {

    super.onWebSocketConnect(sess)
    logger.info(s"Socket $this Connected")
    val outputString = writePretty {
      SDLMessage(
        msgType = SDLMessageType.StartConnection,
        messageMetadata = Some(SDLMessageMetadata(this.toString, sess.getRemoteAddress.toString))
      )
    }
    sess.getRemote.sendString(outputString)
    sess.getPolicy.setMaxTextMessageBufferSize(1000000)
  }


  override def onWebSocketText(message: String): Unit = {
    super.onWebSocketText(message)
    logger.info("Received TEXT message: " + message)
    val sdlMessage = read[SDLMessage](message)

    val responseMessage = agentController.handle(sdlMessage, config)

    val outputString = writePretty(responseMessage)
   // getSession.getRemote.sendString(EndConnection.toString)
      getSession.getRemote.sendString(outputString)

    //TODO TIMO cleanup end connection
    if (message.contains(EndConnection)) {
      logger.info(this + ": received EndConnection request, closing connection")
      getSession.close(StatusCode.NORMAL, "Connection closed by " + this)
    }
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
