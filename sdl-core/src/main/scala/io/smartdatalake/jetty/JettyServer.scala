package io.smartdatalake.jetty

import org.eclipse.jetty.server._
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer


object JettyServer {

  def start(stateListener: CustomListener): Unit = {

    val context = getServletHandler(stateListener)
    val pool = new QueuedThreadPool(200)
    val server = new Server(pool)
    val connector = new ServerConnector(server)
    //TODO find first avaialbe port like spark UI
    connector.setPort(8090)
    server.setConnectors(Array(connector))
    server.setHandler(context);
    server.start()

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
