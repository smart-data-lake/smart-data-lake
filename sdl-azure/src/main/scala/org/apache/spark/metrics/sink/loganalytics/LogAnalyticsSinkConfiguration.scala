/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package org.apache.spark.metrics.sink.loganalytics

import com.microsoft.pnp.{LogAnalyticsConfiguration, LogAnalyticsEnvironment}

import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * This code originates from https://github.com/mspnp/spark-monitoring and is protected by its corresponding MIT license
 */
private[spark] object LogAnalyticsSinkConfiguration {
  private[spark] val LOGANALYTICS_KEY_WORKSPACEID = "workspaceId"
  private[spark] val LOGANALYTICS_KEY_SECRET = "secret"
  private[spark] val LOGANALYTICS_KEY_LOGTYPE = "logType"
  private[spark] val LOGANALYTICS_KEY_TIMESTAMPFIELD = "timestampField"
  private[spark] val LOGANALYTICS_KEY_PERIOD = "period"
  private[spark] val LOGANALYTICS_KEY_UNIT = "unit"

  private[spark] val LOGANALYTICS_DEFAULT_LOGTYPE = "SparkMetrics"
  private[spark] val LOGANALYTICS_DEFAULT_PERIOD = "10"
  private[spark] val LOGANALYTICS_DEFAULT_UNIT = "SECONDS"
}

/**
 * This code originates from https://github.com/mspnp/spark-monitoring and is protected by its corresponding MIT license
 */
private[spark] class LogAnalyticsSinkConfiguration(properties: Properties)
  extends LogAnalyticsConfiguration {

  import LogAnalyticsSinkConfiguration._

  override def getWorkspaceId: Option[String] = {
    Option(properties.getProperty(LOGANALYTICS_KEY_WORKSPACEID, LogAnalyticsEnvironment.getWorkspaceId))
  }

  override def getSecret: Option[String] = {
    Option(properties.getProperty(LOGANALYTICS_KEY_SECRET, LogAnalyticsEnvironment.getWorkspaceKey))
  }

  override protected def getLogType: String =
    properties.getProperty(LOGANALYTICS_KEY_LOGTYPE, LOGANALYTICS_DEFAULT_LOGTYPE)

  override protected def getTimestampFieldName: Option[String] =
    Option(properties.getProperty(LOGANALYTICS_KEY_TIMESTAMPFIELD, null))

  val pollPeriod: Int = {
    val value = properties.getProperty(LOGANALYTICS_KEY_PERIOD, LOGANALYTICS_DEFAULT_PERIOD).toInt
    logInfo(s"Setting polling period to $value")
    value
  }

  val pollUnit: TimeUnit = {
    val value = TimeUnit.valueOf(
      properties.getProperty(LOGANALYTICS_KEY_UNIT, LOGANALYTICS_DEFAULT_UNIT).toUpperCase)
    logInfo(s"Setting polling unit to $value")
    value
  }
}
