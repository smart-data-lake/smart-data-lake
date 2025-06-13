package io.smartdatalake.util.misc

import io.smartdatalake.app.SDLPlugin
import io.smartdatalake.util.secrets.StringOrSecret
import org.apache.logging.log4j.core.appender.{AbstractAppender, ConsoleAppender}
import org.apache.logging.log4j.core.config.{Configuration, LoggerConfig}
import org.apache.logging.log4j.core.filter.AbstractFilter
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.logging.log4j.core.{Filter, LogEvent, LoggerContext}
import org.apache.logging.log4j.{Level, LogManager}

import scala.jdk.CollectionConverters._

/**
 * This Plugin programmatically configures Log4j2 to write logs to Console.
 * This is needed if Log4j2 configuration file is managed by the environment, e.g. Databricks Cluster.
 * The Plugin creates an additional ConsoleAppender. Default logger configuration for the new appender is:
 * - io.smartdatalake -> INFO
 * - RootLogger -> ERROR
 *
 * Enable the plugin by setting java property -Dsdl.pluginClassNames=io.smartdatalake.util.misc.Log4j2ConsoleInitPlugin
 *
 * Configure options by adding the following section to global config:
 * global {
 * pluginOptions {
 * loggerNames = "io.smartdatalake,org.example"
 * loggerNamesToIgnore: "abc,def"
 * patternLayout: "log4j2 pattern"
 * }
 * }
 */
class Log4j2ConsoleInitPlugin extends SDLPlugin with SmartDataLakeLogger {

  override def startup(): Unit = {
    logger.info("Log4j2ConsoleInitPlugin startup finished")
  }

  override def configure(options: Map[String, StringOrSecret]): Unit = {

    val loggerNames = options.get("loggerNames").map(_.resolve().split(',').toSeq).getOrElse(Seq("io.smartdatalake", "ch.sbb"))
    val pattern = options.get("patternLayout").map(_.resolve()).getOrElse("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1} - %m [%t]%n")
    // By default some irrelevant Error messages on (Azure) Databricks are excluded. This can be overridden by setting loggerNamesToIgnore.
    val loggerNamesToIgnore = options.get("loggerNamesToIgnore").map(_.resolve().split(',').toSeq)
      .getOrElse(Seq("azurebfs.services.AbfsClient", "azurebfs.AzureBlobFileSystemStore", "azurebfs.org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation"))

    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config = context.getConfiguration

    // check if already initialized
    val hasDefaultConsoleAppender = config.getAppenders.keySet.asScala.exists(_.startsWith("DefaultConsole-"))
    if (!hasDefaultConsoleAppender) {

      // add appender to Log4j2
      val layout = PatternLayout.newBuilder().withPattern(pattern).build()
      val appender = ConsoleAppender.createDefaultAppenderForLayout(layout)
      appender.start()
      config.addAppender(appender)

      // configure loggers
      loggerNames.foreach(loggerName => configureLogger(config, loggerName, Level.INFO, appender))

      // add appender to root logger
      config.getRootLogger.addAppender(appender, Level.ERROR, null)

      // finalize
      context.updateLoggers()

      // update optional filter
      if (loggerNamesToIgnore.nonEmpty) {
        appender.addFilter(LoggerNameFilter(loggerNamesToIgnore.toSet))
      }

      logger.info("Log4j2ConsoleInitPlugin configure finished: added DefaultConsole-Appender to Log4j configuration")
    } else {
      logger.info("Log4j2ConsoleInitPlugin configure finished: DefaultConsole-Appender was already existing")
    }
  }

  private def configureLogger(config: Configuration, name: String, level: Level, appender: AbstractAppender): LoggerConfig = {
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
    // use "endsWith" to simplify filtering shaded classes (Databricks...)
    if (names.exists(event.getLoggerName.endsWith)) Filter.Result.DENY
    else Filter.Result.NEUTRAL
  }
}