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

import org.apache.spark.SparkContext

/**
 * Provides utility functions for logging with Spark.
 */
private[smartdatalake] object LogUtil {

  /**
   * Overrides the Spark log level with system property "loglevel" if it is set.
   * @param sparkContext [[SparkContext]] on which the loglevel should bet set
   */
  def setLogLevel(sparkContext: SparkContext): Unit = {
    if (System.getProperty("logLevel") != null) {
      sparkContext.setLogLevel(System.getProperty("logLevel"))
    }
  }

  /**
   * Simplify stack trace by removing unneeded entries
   * Works recursively through all causes
   * - truncate stacktrace starting from "monix.*" entries
   */
  def simplifyStackTrace(ex: Throwable): Throwable = {
    ex.setStackTrace(ex.getStackTrace.takeWhile(!_.getClassName.startsWith("monix")))
    Option(ex.getCause).foreach(simplifyStackTrace)
    ex
  }

  /**
   * Split string into lines, removing empty lines
   */
  def splitLines(s: String): Seq[String] = {
    if (s == null) return Seq()
    s.split("(\r)?\n").toSeq.filter(_.nonEmpty)
  }

  /**
   * Get Exception class simple name and message, and the same for the root cause.
   */
  def getExceptionSummary(ex: Exception): String = {
    val rootCause = Option(ex.getCause).map(getRootCause)
    val rootCauseString = rootCause.map(c => s", RootCause: ${getSimpleExceptionStr(c)}")
    s"Exception: ${getSimpleExceptionStr(ex)}${rootCauseString.getOrElse("")}"
  }

  /**
   * Get Exception class simple name and message, e.g. 'ConfigurationException: test msg'
   */
  def getSimpleExceptionStr(ex: Throwable) = s"${ex.getClass.getSimpleName}: ${ex.getMessage}"

  /**
   * recursively get root cause of exception
   */
  def getRootCause(cause: Throwable): Throwable = {
    Option(cause.getCause).map(getRootCause).getOrElse(cause)
  }
}
