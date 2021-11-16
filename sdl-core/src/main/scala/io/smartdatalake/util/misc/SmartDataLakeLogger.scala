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
package io.smartdatalake.util.misc

import org.apache.spark.annotation.DeveloperApi
import org.slf4j.event.Level
import org.slf4j.{Logger, LoggerFactory}

@DeveloperApi
trait SmartDataLakeLogger {
  @transient
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  /* private to hide it in scaladoc, otherwise you have this method in almost all members */
  private[smartdatalake] def logAndThrowException(msg: String, e: Exception): Unit = {
    logger.error( s"$msg: ${e.getClass.getSimpleName} - ${e.getMessage}" )
    throw e
  }

  private[smartdatalake] def logException(e: Exception): Exception = {
    logger.error( s"${e.getClass.getSimpleName} - ${e.getMessage}" )
    e
  }

  private[smartdatalake] def logWithSeverity(severity: Level, msg: String): Unit = {
    severity match {
      case Level.ERROR => logger.error(msg)
      case Level.WARN => logger.warn(msg)
      case Level.INFO => logger.info(msg)
      case Level.DEBUG => logger.debug(msg)
      case Level.TRACE => logger.trace(msg)
    }
  }
}