package io.smartdatalake.jetty

import io.smartdatalake.app.StatusInfoRestApiConfig
import org.eclipse.jetty.server._
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer


object JettyServer {

  private val pool = new QueuedThreadPool(200)
  private val server = new Server(pool)

  def start(stateListener: CustomListener, config: StatusInfoRestApiConfig): Unit = {

    val context = getServletHandler(stateListener)

    def startService(context: ServletContextHandler)(port: Int): Int = {
      val connector = new ServerConnector(server)
      connector.setPort(port)
      server.setConnectors(Array(connector))
      server.setHandler(context);
      server.start()
      port
    }
    PortUtils.startOnPort(startService(context), "Test", config.port, config.maxPortRetries)

  }
  def stop(): Unit = {
    server.stop()
  }

  def getServletHandler(stateListener: CustomListener): ServletContextHandler = {
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    jerseyContext.setContextPath("/api")
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "io.smartdatalake.jetty")
    ApiRequestServletContext.setApiRoot(jerseyContext, APIRoot(stateListener))
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
}
