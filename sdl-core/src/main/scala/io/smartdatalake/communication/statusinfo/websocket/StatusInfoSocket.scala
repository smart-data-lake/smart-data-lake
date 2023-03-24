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

package io.smartdatalake.communication.statusinfo.websocket

import io.smartdatalake.communication.message.SDLMessageType.EndConnection
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
        logger.info("Received " + message)

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
