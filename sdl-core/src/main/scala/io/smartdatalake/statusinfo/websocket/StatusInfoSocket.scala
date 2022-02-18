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

package io.smartdatalake.statusinfo.websocket

import io.smartdatalake.statusinfo.IncrementalStatusInfoListener
import io.smartdatalake.statusinfo.websocket.SDLMessageType.EndConnection
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.eclipse.jetty.websocket.api.{Session, StatusCode, WebSocketAdapter}

import java.util.Locale

class StatusInfoSocket(stateListener: IncrementalStatusInfoListener) extends WebSocketAdapter with SmartDataLakeLogger {

    override def onWebSocketConnect(sess: Session): Unit = {
        super.onWebSocketConnect(sess)
        stateListener.activeSockets.+=(this)
        logger.info(s"Socket $this Connected")
        sess.getRemote.sendString("Hello from " + this)
    }

    override def onWebSocketText(message: String): Unit = {
        super.onWebSocketText(message)
        logger.info("Received TEXT message: " + message)

        if (message.toLowerCase(Locale.US).contains(EndConnection)) {
            getSession.close(StatusCode.NORMAL, "Connection closed by client")
        }
    }

    override def onWebSocketClose(statusCode: Int, reason: String): Unit = {
        super.onWebSocketClose(statusCode, reason)
        stateListener.activeSockets -= this
        logger.info("Socket Closed: [" + statusCode + "] " + reason)

    }

    override def onWebSocketError(cause: Throwable): Unit = {
        super.onWebSocketError(cause)
        stateListener.activeSockets -= this
        logger.error(s"Socket $this was closed with error ${cause.printStackTrace(System.err)}")
    }
}
