package io.smartdatalake.jetty

import org.eclipse.jetty.server._
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool

object JettyServer {

  @throws[Exception]
  def start(): Unit = {
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    val pool = new QueuedThreadPool(200)
    val server = new Server(pool)
    val connector = new ServerConnector(server)
    connector.setPort(8090)
    server.setConnectors(Array(connector))
    server.setHandler(context);
    val holder: ServletHolder = new ServletHolder(classOf[EntryPoint])
    context.addServlet(holder, "/test" )
    server.start()
  }
}
