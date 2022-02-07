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

import io.netty.channel.unix.Errors.NativeIoException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.eclipse.jetty.util.MultiException

import java.net.BindException
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

/**
 * Utility-Methods for finding out the next available port to bind to.
 *
 * Derived from org.apache.spark.util.Utils#startServiceOnPort and surrounding methods.
 */
object PortUtils extends SmartDataLakeLogger {

  /**
   * Returns the user port to try based upon base and offset.
   * Privileged ports >= 65536 are skipped by rolling over to 1024.
   */
  def userPort(base: Int, offset: Int): Int = {
    (base + offset - 1024) % (65536 - 1024) + 1024
  }

  /**
   * Attempts to start a server on the given port, or fails after a number of attempts.
   * Each subsequent attempt uses 1 + the port used in the previous attempt.
   *
   * @param serverName  Name of the service.
   * @param startServer Function to start server on a given port.
   *                    This is expected to throw java.net.BindException on port collision.
   * @param startPort   The initial port to start the server on.
   * @param maxRetries  : If startPort is already in use,  we will increment port by one and try with that new port.
   *                    maxPortRetries describes how many times this should be attempted. If set to 0 it will not be attempted.
   * @return The port on which the Server was started
   */
  def startOnPort(startServer: Int => Int,
                  serverName: String,
                  startPort: Int, maxRetries: Int): Int = {

    require(1024 <= startPort && startPort < 65536, "startPort should be between 1024 and 65535 (inclusive)")
    require(maxRetries >= 0, "maxRetries has to be >= 0")

    for (offset <- 0 to maxRetries + 1) {
      val tryPort = userPort(startPort, offset)
      try {
        val port = startServer(tryPort)
        logger.info(s"Successfully started server $serverName on port $port.")
        return port
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage =
              s"${e.getMessage}: Server $serverName failed after $maxRetries retries (starting from $startPort)!"
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          logger.warn(s"Server $serverName could not bind on port $tryPort. Attempting port ${tryPort + 1}.")
      }
    }
    // Should never happen
    throw new IllegalStateException(s"Failed to start server $serverName on port $startPort")
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
