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

import com.codahale.metrics.json.MetricsModule
import com.codahale.metrics._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.smartdatalake.util.azure.client.loganalytics.{LogAnalyticsClient, LogAnalyticsSendBufferClient}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.metrics.sink.SparkInformation
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * This code originates from https://github.com/mspnp/spark-monitoring and is protected by its corresponding MIT license
 */
object LogAnalyticsReporter {
  /**
   * Returns a new {@link Builder} for {@link LogAnalyticsReporter}.
   *
   * @param registry the registry to report
   * @return a { @link Builder} instance for a { @link LogAnalyticsReporter}
   */
  def forRegistry(registry: MetricRegistry) = new LogAnalyticsReporter.Builder(registry)
  /**
   * A builder for {@link LogAnalyticsReporter} instances. Defaults to not using a prefix, using the default clock, converting rates to
   * events/second, converting durations to milliseconds, and not filtering metrics. The default
   * Log Analytics log type is DropWizard
   */
  class Builder(val registry: MetricRegistry) extends SmartDataLakeLogger {
    private var clock = Clock.defaultClock
    private var prefix: String = null
    private var rateUnit = TimeUnit.SECONDS
    private var durationUnit = TimeUnit.MILLISECONDS
    private var filter = MetricFilter.ALL
    private var filterRegex = sys.env.getOrElse("LA_SPARKMETRIC_REGEX", "")
    if(filterRegex != "") {
      filter = new MetricFilter() {
        override def matches(name: String, metric: Metric): Boolean = {
          name.matches(filterRegex)
        }
      }
    }
    private var logType = "SparkMetrics"
    private var workspaceId: String = null
    private var workspaceKey: String = null
    /**
     * Use the given {@link Clock} instance for the time. Usually the default clock is sufficient.
     *
     * @param clock clock
     * @return { @code this}
     */
    def withClock(clock: Clock): LogAnalyticsReporter.Builder = {
      this.clock = clock
      this
    }
    /**
     * Configure a prefix for each metric name. Optional, but useful to identify originator of metric.
     *
     * @param prefix prefix for metric name
     * @return { @code this}
     */
    def prefixedWith(prefix: String): LogAnalyticsReporter.Builder = {
      this.prefix = prefix
      this
    }
    /**
     * Convert all the rates to a certain TimeUnit, defaults to TimeUnit.SECONDS.
     *
     * @param rateUnit unit of rate
     * @return { @code this}
     */
    def convertRatesTo(rateUnit: TimeUnit): LogAnalyticsReporter.Builder = {
      this.rateUnit = rateUnit
      this
    }
    /**
     * Convert all the durations to a certain TimeUnit, defaults to TimeUnit.MILLISECONDS
     *
     * @param durationUnit unit of duration
     * @return { @code this}
     */
    def convertDurationsTo(durationUnit: TimeUnit): LogAnalyticsReporter.Builder = {
      this.durationUnit = durationUnit
      this
    }
    /**
     * Allows to configure a special MetricFilter, which defines what metrics are reported
     *
     * @param filter metrics filter
     * @return { @code this}
     */
    def filter(filter: MetricFilter): LogAnalyticsReporter.Builder = {
      this.filter = filter
      this
    }
    /**
     * The log type to send to Log Analytics. Defaults to 'SparkMetrics'.
     *
     * @param logType Log Analytics log type
     * @return { @code this}
     */
    def withLogType(logType: String): LogAnalyticsReporter.Builder = {
      logger.info(s"Setting logType to '${logType}'")
      this.logType = logType
      this
    }
    /**
     * The workspace id of the Log Analytics workspace
     *
     * @param workspaceId Log Analytics workspace id
     * @return { @code this}
     */
    def withWorkspaceId(workspaceId: String): LogAnalyticsReporter.Builder = {
      logger.info(s"Setting workspaceId to '${workspaceId}'")
      this.workspaceId = workspaceId
      this
    }
    /**
     * The workspace key of the Log Analytics workspace
     *
     * @param workspaceKey Log Analytics workspace key
     * @return { @code this}
     */
    def withWorkspaceKey(workspaceKey: String): LogAnalyticsReporter.Builder = {
      this.workspaceKey = workspaceKey
      this
    }
    /**
     * Builds a {@link LogAnalyticsReporter} with the given properties.
     *
     * @return a { @link LogAnalyticsReporter}
     */
    def build(): LogAnalyticsReporter = {
      logger.info("Creating LogAnalyticsReporter")
      new LogAnalyticsReporter(
        registry,
        workspaceId,
        workspaceKey,
        logType,
        clock,
        prefix,
        rateUnit,
        durationUnit,
        filter
      )
    }
  }
}

/**
 * This code originates from https://github.com/mspnp/spark-monitoring and is protected by its corresponding MIT license
 */
class LogAnalyticsReporter(val registry: MetricRegistry, val workspaceId: String, val workspaceKey: String, val logType: String, val clock: Clock, val prefix: String, val rateUnit: TimeUnit, val durationUnit: TimeUnit, val filter: MetricFilter)//, var additionalFields: util.Map[String, AnyRef]) //this.logType);
  extends ScheduledReporter(registry, "loganalytics-reporter", filter, rateUnit, durationUnit)
    with SmartDataLakeLogger {
  private val mapper = new ObjectMapper()
    .registerModules(
      DefaultScalaModule,
      new MetricsModule(
        rateUnit,
        durationUnit,
        true,
        filter
      )
    )
  private val logAnalyticsBufferedClient = new LogAnalyticsSendBufferClient(
    new LogAnalyticsClient(this.workspaceId, this.workspaceKey),
    "SparkMetric"
  )
  override def report(
                       gauges: java.util.SortedMap[String, Gauge[_]],
                       counters: java.util.SortedMap[String, Counter],
                       histograms: java.util.SortedMap[String, Histogram],
                       meters: java.util.SortedMap[String, Meter],
                       timers: java.util.SortedMap[String, Timer]): Unit = {
    logger.debug("Reporting metrics")
    // nothing to do if we don't have any metrics to report
    if (gauges.isEmpty && counters.isEmpty && histograms.isEmpty && meters.isEmpty && timers.isEmpty) {
      logger.info("All metrics empty, nothing to report")
      return
    }
    val now = Instant.now
    val ambientProperties = SparkInformation.get() + ("SparkEventTime" -> now.toString)
    val metrics = gauges.asScala.retain((_, v) => v.getValue != null).toSeq ++
      counters.asScala.toSeq ++ histograms.asScala.toSeq ++ meters.asScala.toSeq ++ timers.asScala.toSeq
    for ((name, metric) <- metrics) {
      try {
        this.logAnalyticsBufferedClient.sendMessage(
          compact(this.addProperties(name, metric, ambientProperties)),
          "SparkMetricTime"
        )
      } catch {
        case NonFatal(e) =>
          logger.error(s"Error serializing metric to JSON", e)
          None
      }
    }
  }
  //private def addProperties(name: String, metric: Metric, timestamp: Instant): JValue = {
  private def addProperties(name: String, metric: Metric, properties: Map[String, String]): JValue = {
    val metricType: String = metric match {
      case _: Counter => classOf[Counter].getSimpleName
      case _: Gauge[_] => classOf[Gauge[_]].getSimpleName
      case _: Histogram => classOf[Histogram].getSimpleName
      case _: Meter => classOf[Meter].getSimpleName
      case _: Timer => classOf[Timer].getSimpleName
      case m: Metric => m.getClass.getSimpleName
    }
    parse(this.mapper.writeValueAsString(metric))
      .merge(render(
        ("metric_type" -> metricType) ~
          ("name" -> name) ~
          properties
      ))
  }
}