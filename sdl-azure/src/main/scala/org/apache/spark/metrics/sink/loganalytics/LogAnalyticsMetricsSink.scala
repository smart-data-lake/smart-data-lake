package org.apache.spark.metrics.sink.loganalytics

import com.codahale.metrics.MetricRegistry
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.metrics.sink.Sink

import java.util.Properties


/**
 * This code originates from https://github.com/mspnp/spark-monitoring and is protected by its corresponding MIT license
 */
private class LogAnalyticsMetricsSink(
                                val property: Properties,
                                val registry: MetricRegistry,
                                securityMgr: SecurityManager)
  extends Sink with SmartDataLakeLogger {

  private val config = new LogAnalyticsSinkConfiguration(property)

  org.apache.spark.metrics.MetricsSystem.checkMinimalPollingPeriod(config.pollUnit, config.pollPeriod)

  var reporter = LogAnalyticsReporter.forRegistry(registry)
    .withWorkspaceId(config.workspaceId)
    .withWorkspaceKey(config.secret)
    .withLogType(config.logType)
    .build()

  override def start(): Unit = {
    reporter.start(config.pollPeriod, config.pollUnit)
    logger.info(s"LogAnalyticsMetricsSink started")
  }

  override def stop(): Unit = {
    reporter.stop()
    logger.info("LogAnalyticsMetricsSink stopped.")
  }

  override def report(): Unit = {
    reporter.report()
  }
}
