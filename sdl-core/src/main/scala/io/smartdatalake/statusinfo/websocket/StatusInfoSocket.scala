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
import org.eclipse.jetty.websocket.api.{Session, StatusCode, WebSocketAdapter}

import java.util.Locale

class StatusInfoSocket(stateListener: IncrementalStatusInfoListener) extends WebSocketAdapter {

    override def onWebSocketConnect(sess: Session): Unit = {
        super.onWebSocketConnect(sess)
        stateListener.activeSockets.+=(this)
        System.out.println("Socket Connected: " + sess)
        sess.getRemote.sendString("Hello from " + this)

    }

    override def onWebSocketText(message: String): Unit = {
        super.onWebSocketText(message)
        System.out.println("Received TEXT message: " + message)

        if (message.toLowerCase(Locale.US).contains("bye")) {
            getSession.close(StatusCode.NORMAL, "Thanks")
        }
    }

    override def onWebSocketClose(statusCode: Int, reason: String): Unit = {
        super.onWebSocketClose(statusCode, reason)
        stateListener.activeSockets -= this
        System.out.println("Socket Closed: [" + statusCode + "] " + reason)

    }

    override def onWebSocketError(cause: Throwable): Unit = {
        super.onWebSocketError(cause)
        cause.printStackTrace(System.err)
    }
}
