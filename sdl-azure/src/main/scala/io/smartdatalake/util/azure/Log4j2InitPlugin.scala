/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.azure

import com.azure.identity.ClientSecretCredentialBuilder
import io.smartdatalake.app.SDLPlugin
import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import org.apache.logging.log4j.core.config.{Configuration, LoggerConfig, NullConfiguration}
import org.apache.logging.log4j.core.filter.AbstractFilter
import org.apache.logging.log4j.core.{Filter, LogEvent, LoggerContext}
import org.apache.logging.log4j.layout.template.json.JsonTemplateLayout
import org.apache.logging.log4j.{Level, LogManager}

/**
 * This Plugin programmatically configures Log4j2 to write logs to LogAnalytics.
 * This is needed if Log4j2 configuration file is managed by the environment, e.g. Databricks Cluster.
 * The logger configuration is:
 * - io.smartdatalake -> INFO
 * - RootLogger -> ERROR
 *
 * Enable by setting java property -Dsdl.pluginClassName=io.smartdatalake.util.azure.Log4j2InitPlugin
 * and add the following section to global config:
 * global {
 *   pluginOptions {
 *     endpoint: "https://....ingest.monitor.azure.com"
 *     ruleId: "dcr-..."
 *     streamName: "Custom-sdlb-log"
 *     tenantId: ...
 *     clientId: ...
 *     clientSecret: "..."
 *   }
 * }
 */
class Log4j2InitPlugin extends SDLPlugin with SmartDataLakeLogger {

  private val jsonLayout = JsonTemplateLayout.newBuilder().setConfiguration(new NullConfiguration()).setEventTemplateUri("classpath:LogAnalyticsSdlbLayout.json").build

  // backend is configured later, but we want to start collecting logs from the beginning
  private val appender = new LogAnalyticsAppender("AzureLogAnalytics", backend = None, layout = jsonLayout, filter = None, maxDelayMillis = Some(1000))

  override def startup(): Unit = {

    // add appender to Log4j2
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config = context.getConfiguration
    appender.start()
    config.addAppender(appender)

    // configure loggers
    configureLogger(config, "io.smartdatalake", Level.INFO, appender)

    // configure root logger
    config.getRootLogger.addAppender(appender, Level.ERROR, null)

    // finalize
    context.updateLoggers()
    logger.info("SDLPlugin startup finished")
  }

  override def configure(options: Map[String, StringOrSecret]): Unit = {

    // try Azure AD authentication using clientId/Secret
    val tokenCredential = if (options.isDefinedAt("clientSecret")) {
      val clientId = options("clientId").resolve()
      val clientSecret = options("clientSecret").resolve()
      val tenantId = options("tenantId").resolve()
      Some(new ClientSecretCredentialBuilder()
        .tenantId(tenantId)
        .clientId(clientId)
        .clientSecret(clientSecret)
        .build())
    } else None

    // configure backend
    val endpoint = options.getOrElse("endpoint", throw ConfigurationException("Value for option endpoint missing in GlobalConfig.pluginConfig")).resolve()
    val ruleId = options.getOrElse("ruleId", throw ConfigurationException("Value for option ruleId missing in GlobalConfig.pluginConfig")).resolve()
    val streamName = options.getOrElse("streamName", throw ConfigurationException("Value for option streamName missing in GlobalConfig.pluginConfig")).resolve()
    val backend = new LogAnalyticsIngestionBackend[LogEvent](endpoint, ruleId, streamName, batchSize = 100, jsonLayout.toSerializable, tokenCredential)
    appender.updateBackend(backend)

    // update optional filter
    val loggerNamesToIgnore = options.get("loggerNamesToIgnore").map(_.resolve().split(','))
    loggerNamesToIgnore.foreach(names => appender.addFilter(LoggerNameFilter(names.toSet)))

    logger.info("SDLPlugin configure finished")
  }

  private def configureLogger(config: Configuration, name: String, level: Level, appender: LogAnalyticsAppender): LoggerConfig = {
    val loggerConfig = Option(config.getLoggers.get(name))
      .getOrElse {
        val newLoggerConfig = new LoggerConfig(name, level, true)
        config.addLogger(name, newLoggerConfig)
        newLoggerConfig
      }
    loggerConfig.addAppender(appender, Level.INFO, null)
    // return
    loggerConfig
  }
}

/**
 * A Log4j2 filter implementation to ignore a given list of logger names.
 */
case class LoggerNameFilter(names: Set[String]) extends AbstractFilter {
  override def filter(event: LogEvent): Filter.Result = {
    if (names.contains(event.getLoggerName)) Filter.Result.DENY
    else Filter.Result.NEUTRAL
  }
}