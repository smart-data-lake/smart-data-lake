package io.smartdatalake.statusinfo

import io.smartdatalake.app.StatusInfoRestApiConfig
import org.eclipse.jetty.server._
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer


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

  private def getServletContextHandler(stateListener: StatusInfoListener): ServletContextHandler = {
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/api")
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "io.smartdatalake.jetty")
    StatusInfoServletContext.setStatusInfoProvider(jerseyContext, StatusInfoProvider(stateListener))
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }

  private def startServer(context: ServletContextHandler)(port: Int): Int = {
    val connector = new ServerConnector(server)
    connector.setPort(port)
    server.setConnectors(Array(connector))
    server.setHandler(context)
    server.start()
    port
  }

}
