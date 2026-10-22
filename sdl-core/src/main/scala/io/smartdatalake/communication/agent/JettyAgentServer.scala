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

import io.smartdatalake.app.{GlobalConfig, LocalJettyAgentSmartDataLakeBuilderConfig, SmartDataLakeBuilder}
import io.smartdatalake.config.ConfigParser.{getConnectionConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.Connection
import org.apache.hadoop.conf.Configuration
import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler.{ContextHandler, ContextHandlerCollection}
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.eclipse.jetty.websocket.server.{JettyServerUpgradeRequest, JettyServerUpgradeResponse, JettyWebSocketCreator}
import org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer

/**
 * Methods for starting and stopping the JettyAgentServer
 */
case class JettyAgentServer(sdlb: SmartDataLakeBuilder, config: LocalJettyAgentSmartDataLakeBuilderConfig) extends SmartDataLakeLogger {

  private val pool = new QueuedThreadPool(200)
  private val server = new Server(pool)

  private implicit val dummyInstanceRegistry: InstanceRegistry = new InstanceRegistry()
  private val localConfig = config.getHoconConfig(validateCompletness = false)(new Configuration())
  private val localConnections = getConnectionConfigMap(localConfig)
    .map { case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config)) }
  private val sdlbGlobalConfig = GlobalConfig.from(localConfig)
  implicit val hadoopConfiguration: Configuration = sdlbGlobalConfig.getHadoopConfiguration

  private val agentController = AgentServerController(sdlb, localConnections)

  def start(): Unit = {
    val contextHandler = getServletContextHandler(config, agentController)
    startOnPort(startServer(contextHandler), "AgentServer", config.port, config.maxPortRetries)
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
    val contextHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    contextHandler.setContextPath("/ws")
    val webSocketCreator: JettyWebSocketCreator = new JettyWebSocketCreator {
      override def createWebSocket(request: JettyServerUpgradeRequest, response: JettyServerUpgradeResponse): Object =
        new JettyAgentServerSocket(config, serverController)
    }
    JettyWebSocketServletContainerInitializer.configure(contextHandler, (_, container) => {
      container.addMapping("/", webSocketCreator)
    })
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

  private def startOnPort(startService: Int => Int, serviceName: String, startPort: Int, maxRetries: Int): Int = {
    for (offset <- 0 to maxRetries) {
      val tryPort = startPort + offset
      try {
        return startService(tryPort)
      } catch {
        case e: Exception if offset < maxRetries =>
          logger.warn(s"$serviceName: Failed to start on port $tryPort, trying ${tryPort + 1}")
      }
    }
    throw new RuntimeException(s"$serviceName: Failed to bind on any port from $startPort to ${startPort + maxRetries}")
  }

}
