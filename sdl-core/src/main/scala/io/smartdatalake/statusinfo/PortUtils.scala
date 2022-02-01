package io.smartdatalake.statusinfo

import io.netty.channel.unix.Errors.NativeIoException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.eclipse.jetty.util.MultiException

import java.net.BindException
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

object PortUtils extends SmartDataLakeLogger {

  /**
   * Returns the user port to try when trying to bind a service. Handles wrapping and skipping
   * privileged ports.
   */
  def userPort(base: Int, offset: Int): Int = {
    (base + offset - 1024) % (65536 - 1024) + 1024
  }

  /**
   * Attempt to start a service on the given port, or fail after a number of attempts.
   * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
   *
   * @param serviceName  Name of the service.
   * @param startService Function to start service on a given port.
   *                     This is expected to throw java.net.BindException on port collision.
   * @param startPort    The initial port to start the service on.
   * @param maxRetries   : If startPort is already in use,  we will increment port by one and try with that new port.
   *                     maxPortRetries describes how many times this should be attempted. If set to 0 it will not be attempted.
   * @return (service: T, port: Int)
   */
  def startOnPort(startService: Int => Int,
                  serviceName: String,
                  startPort: Int, maxRetries: Int): Int = {

    require(1024 <= startPort && startPort < 65536, "startPort should be between 1024 and 65535 (inclusive)")

    for (offset <- 0 to maxRetries + 1) {
      val tryPort = userPort(startPort, offset)
      try {
        val port = startService(tryPort)
        logger.info(s"Successfully started service$serviceName on port $port.")
        return port
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage =
              s"${e.getMessage}: Service$serviceName failed after " +
                s"$maxRetries retries (starting from $startPort)! Consider explicitly setting " +
                s"the appropriate port for the service $serviceName " +
                s"or increasing maxRetries."

            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          logger.warn(s"Service$serviceName could not bind on port $tryPort. " +
            s"Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new IllegalStateException(s"Failed to start service$serviceName on port $startPort")
  }

  /**
   * Return whether the exception is caused by an address-port collision when binding.
   */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: MultiException =>
        e.getThrowables.asScala.exists(isBindCollision)
      case e: NativeIoException =>
        (e.getMessage != null && e.getMessage.startsWith("bind() failed: ")) ||
          isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }
}
