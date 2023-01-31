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
package io.smartdatalake.communication.agent

import io.smartdatalake.app.LocalJettyAgentSmartDataLakeBuilderConfig
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.util.PortUtils
import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler.{ContextHandler, ContextHandlerCollection}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.eclipse.jetty.websocket.server.WebSocketHandler
import org.eclipse.jetty.websocket.servlet.{ServletUpgradeRequest, ServletUpgradeResponse, WebSocketCreator, WebSocketServletFactory}

/**
 * Methods for starting and stopping the JettyAgentServer
 */
object JettyAgentServer extends SmartDataLakeLogger {

  private val pool = new QueuedThreadPool(200)
  private val server = new Server(pool)

  def start(config: LocalJettyAgentSmartDataLakeBuilderConfig, serverController: AgentServerController): Unit = {
    val contextHandler = getServletContextHandler(config, serverController)
    PortUtils.startOnPort(startServer(contextHandler), "AgentServer", config.port, config.maxPortRetries, logger)
  }

  def stop(): Unit = {
    server.stop()
  }

  private def getServletContextHandler(config: LocalJettyAgentSmartDataLakeBuilderConfig, serverController: AgentServerController): ContextHandlerCollection = {
    val handlers: ContextHandlerCollection = new ContextHandlerCollection()

    val socketHandler = createWebsocketHandler(config, serverController)
    handlers.addHandler(socketHandler)
    handlers
  }

  private def createWebsocketHandler(config: LocalJettyAgentSmartDataLakeBuilderConfig, serverController: AgentServerController): ContextHandler = {
    val contextHandler = new ContextHandler("/ws")
    val webSocketcreator: WebSocketCreator = new WebSocketCreator() {
      override def createWebSocket(request: ServletUpgradeRequest, response: ServletUpgradeResponse) = new JettyAgentServerSocket(config, serverController)
    }
    val webSocketHandler = new WebSocketHandler() {
      override def configure(factory: WebSocketServletFactory): Unit = {
        factory.setCreator(webSocketcreator)
      }
    }

    contextHandler.setHandler(webSocketHandler)
    contextHandler.getMaxFormContentSize
    contextHandler
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
