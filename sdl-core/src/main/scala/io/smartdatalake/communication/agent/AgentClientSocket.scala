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

import io.smartdatalake.communication.statusinfo.websocket.SDLMessageType.EndConnection
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.eclipse.jetty.websocket.api.{Session, StatusCode, WebSocketAdapter}

class AgentClientSocket() extends WebSocketAdapter with SmartDataLakeLogger {

  var actionStillRunning = true

  override def onWebSocketConnect(sess: Session): Unit = {
    super.onWebSocketConnect(sess)
  }

  override def onWebSocketText(message: String): Unit = {
    logger.info("Received TEXT message: " + message)
    super.onWebSocketText(message)

    if (message.contains(EndConnection.toString)) {
      logger.info(this + ": received EndConnection request, closing connection")
      actionStillRunning = false
      getSession.close(StatusCode.NORMAL, "Connection closed by " + this)
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
