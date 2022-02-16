/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.statusinfo

import io.smartdatalake.app.StatusInfoRestApiConfig
import io.smartdatalake.statusinfo.websocket.EventSocket
import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler.{ContextHandler, ContextHandlerCollection}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.eclipse.jetty.websocket.server.WebSocketHandler
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

/**
 * Methods for starting and stopping the Status Info Server
 */
object StatusInfoServer {

  private val pool = new QueuedThreadPool(200)
  private val server = new Server(pool)

  def start(stateListener: StatusInfoListener, config: StatusInfoRestApiConfig): Unit = {
    val contextHandler = getServletContextHandler(stateListener)
    PortUtils.startOnPort(startServer(contextHandler), "StatusInfoServer", config.port, config.maxPortRetries)
  }

  def stop(): Unit = {
    server.stop()
  }

  private def getServletContextHandler(stateListener: StatusInfoListener): ContextHandlerCollection = {
    val handlers: ContextHandlerCollection = new ContextHandlerCollection();
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/api")
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "io.smartdatalake.statusinfo")
    StatusInfoServletContext.setStateListener(jerseyContext, stateListener)
    jerseyContext.addServlet(holder, "/*")
    handlers.addHandler(jerseyContext)

    val wsh = new WebSocketHandler.Simple(classOf[EventSocket])
    handlers.addHandler(createContextHandler("/ws", wsh));

    handlers
  }

  // Convenience method to create and configure a ContextHandler.
  private def createContextHandler(contextPath: String, wrappedHandler: Handler): ContextHandler = {
    val ch = new ContextHandler(contextPath)
    ch.setHandler(wrappedHandler)
    ch.clearAliasChecks()
    ch.setAllowNullPathInfo(true)
    ch
  }

  private def startServer(handlers: ContextHandlerCollection)(port: Int): Int = {

    val connector = new ServerConnector(server)
    connector.setPort(port)
    server.setConnectors(Array(connector))
    server.setHandler(handlers)
    server.start()
    port
  }

}
