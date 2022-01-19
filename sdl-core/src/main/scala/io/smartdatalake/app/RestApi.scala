package io.smartdatalake.app

import io.smartdatalake.jetty.{APIRoot, ApiRequestServletContext, CustomListener}
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

/**
 *
 * @param port: port with which the first connection attempt is made
 * @param maxPortRetries: If port is already in use, RestAPI will increment port by one and try with that new port.
 * maxPortRetries describes how many times this should be attempted. If set to 0 it will not be attempted.
 * @param stopOnEnd: Set to false if the RestAPI should remain online even after SDL has finished its execution.
 * In that case, the Application needs to be stopped manually. Useful for debugging.
 */
case class RestApi(port: Int = 4140, maxPortRetries: Int = 10, stopOnEnd: Boolean = true)