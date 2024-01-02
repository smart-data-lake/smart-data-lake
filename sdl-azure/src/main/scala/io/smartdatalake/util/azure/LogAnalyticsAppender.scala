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

import io.smartdatalake.app.AppUtil
import org.apache.logging.log4j.core.Filter.Result
import org.apache.logging.log4j.core._
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.impl.ThrowableProxy
import org.apache.logging.log4j.core.time.Instant
import org.apache.logging.log4j.layout.template.json.JsonTemplateLayout
import org.apache.logging.log4j.message.Message
import org.apache.logging.log4j.spi.DefaultThreadContextMap
import org.apache.logging.log4j.util.ReadOnlyStringMap
import org.apache.logging.log4j.{Level, Marker, ThreadContext}
import org.apache.spark.TaskContext
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
 * Custom Log4j2 Appender to write log messages to LogAnalytics.
 * This Appender sends batches of max 100 log messages to a LogAnalytics workspace.
 *
 * @param name           name of the appender
 * @param backend        LogAnalytics backend to send logs to. Can be empty and set later, see also [[updateBackend]].
 * @param maxDelayMillis The maximum number of milliseconds to wait for a complete batch.
 * @param filter         optional log event filter definition.
 * @param layout         custom Log4j2 layout using [[JsonTemplateLayout]]. See also https://logging.apache.org/log4j/2.x/manual/json-template-layout.html.
 * */
class LogAnalyticsAppender(name: String, var backend: Option[LogAnalyticsBackend[LogEvent]], layout: JsonTemplateLayout, filter: Option[Filter] = None, maxDelayMillis: Option[Int] = None)
  extends AbstractAppender(name, filter.orNull, layout, /*ignoreExceptions*/ false, Array()) {

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private var msgBuffer = collection.mutable.Buffer[LogEvent]()

  /**
   * Method to set backend later. This is useful if backend configuration is not available from the start, but logging should already collect events.
   */
  def updateBackend(backend: LogAnalyticsBackend[LogEvent]): Unit = {
    if (this.backend.nonEmpty) logger.warn("LogAnalyticsBackend was already initialized, it will be overwritten.")
    this.backend = Some(backend)
  }

  override def append(event: LogEvent): Unit = {
    assert(isStarted)
    // ignore logs from org.apache.http to avoid infinite loop on error in LogAnalyticsClient
    if (event.getLoggerName.startsWith("org.apache.http")) return
    // apply filter
    if (filter.map(_.filter(event)).contains(Result.DENY)) return
    // process event
    synchronized {
      msgBuffer.append(enrichSparkLocalProperties(event.toImmutable))
    }
    if (backend.isDefined && msgBuffer.size >= backend.get.batchSize) flush()
  }

  private def flush(): Unit = {
    if (msgBuffer.nonEmpty && backend.isDefined) {
      val msgBufferToSend = synchronized {
        val tmp = msgBuffer
        msgBuffer = collection.mutable.Buffer[LogEvent]()
        tmp
      }.toSeq
      // don't include sending in synchronized block as it might take a longer time (or even hang...)
      backend.get.send(msgBufferToSend)
    }
  }

  override def start(): Unit = {
    super.start()
    import monix.execution.Scheduler.{global => scheduler}
    val duration = FiniteDuration(maxDelayMillis.getOrElse(1000).toLong, TimeUnit.MILLISECONDS)
    scheduler.scheduleAtFixedRate(duration, duration)(flush())
    sys.addShutdownHook(flush())
    println("LogAnalyticsAppender started")
  }

  override def stop(): Unit = {
    flush()
    super.stop()
    println("LogAnalyticsAppender stopped")
  }

  private def enrichSparkLocalProperties(event: LogEvent) = {
    // If we are on an executor, we have a TaskContext
    // On executors we need to enrich LogEvent with SDLB information from spark local properties
    if (TaskContext.get() != null) new LogEventWithSparkLocalProperties(event)
    else event
  }
}

object LogAnalyticsAppender {
  def createHttpCollectorAppender(name: String, workspaceId: String, workspaceKey: String, maxDelayMillis: Integer, logType: String, layout: Layout[_], filter: Filter): LogAnalyticsAppender = {
    assert(name != null, s"name is not defined for ${getClass.getSimpleName}")
    assert(workspaceId != null, s"workspaceId is not defined for ${getClass.getSimpleName}")
    assert(workspaceKey != null, s"workspaceKey is not defined for ${getClass.getSimpleName}")
    assert(logType != null, s"logType is not defined for ${getClass.getSimpleName}")
    assert(layout != null && layout.isInstanceOf[JsonTemplateLayout], "layout must be an instance of JsonTemplateLayout")
    val jsonLayout = layout.asInstanceOf[JsonTemplateLayout]
    val backend = new LogAnalyticsHttpCollectorBackend[LogEvent](workspaceId, workspaceKey, logType, jsonLayout.toSerializable)
    new LogAnalyticsAppender(name, Some(backend), layout.asInstanceOf[JsonTemplateLayout], Option(filter), Option(maxDelayMillis).map(_.toInt))
  }

  def createIngestionAppender(name: String, endpoint: String, ruleId: String, streamName: String, maxDelayMillis: Integer, batchSize: Integer, layout: Layout[_], filter: Filter): LogAnalyticsAppender = {
    assert(name != null, s"name is not defined for ${getClass.getSimpleName}")
    assert(endpoint != null, s"endpoint is not defined for ${getClass.getSimpleName}")
    assert(ruleId != null, s"ruleId is not defined for ${getClass.getSimpleName}")
    assert(streamName != null, s"streamName is not defined for ${getClass.getSimpleName}")
    assert(batchSize != null, s"batchSize is not defined for ${getClass.getSimpleName}")
    assert(layout != null && layout.isInstanceOf[JsonTemplateLayout], "layout must be an instance of JsonTemplateLayout")
    val jsonLayout = layout.asInstanceOf[JsonTemplateLayout]
    val backend = new LogAnalyticsIngestionBackend[LogEvent](endpoint, ruleId, streamName, batchSize, jsonLayout.toSerializable)
    new LogAnalyticsAppender(name, Some(backend), jsonLayout, Option(filter), Option(maxDelayMillis).map(_.toInt))
  }
}

/**
 * Wrapper around a LogEvent to add SDLB context data on executors.
 */
class LogEventWithSparkLocalProperties(event: LogEvent) extends LogEvent {
  override def toImmutable: LogEvent = new LogEventWithSparkLocalProperties(event.toImmutable)
  override def getContextMap: util.Map[String, String] = event.getContextData.toMap
  override def getContextData: ReadOnlyStringMap = {
    val data = event.getContextData
    if (data.containsKey(AppUtil.MDC_SDLB_PROPERTIES.head)) data
    else LogEventWithSparkLocalProperties.getContextData
  }
  override def getContextStack: ThreadContext.ContextStack = event.getContextStack
  override def getLoggerFqcn: String = event.getLoggerFqcn
  override def getLevel: Level = event.getLevel
  override def getLoggerName: String = event.getLoggerName
  override def getMarker: Marker = event.getMarker
  override def getMessage: Message = event.getMessage
  override def getTimeMillis: Long = event.getTimeMillis
  override def getInstant: Instant = event.getInstant
  override def getSource: StackTraceElement = event.getSource
  override def getThreadName: String = event.getThreadName
  override def getThreadId: Long = event.getThreadId
  override def getThreadPriority: Int = event.getThreadPriority
  override def getThrown: Throwable = event.getThrown
  override def getThrownProxy: ThrowableProxy = event.getThrownProxy
  override def isEndOfBatch: Boolean = event.isEndOfBatch
  override def isIncludeLocation: Boolean = event.isIncludeLocation
  override def setEndOfBatch(endOfBatch: Boolean): Unit = event.setEndOfBatch(endOfBatch)
  override def setIncludeLocation(locationRequired: Boolean): Unit = event.setIncludeLocation(locationRequired)
  override def getNanoTime: Long = event.getNanoTime
}
object LogEventWithSparkLocalProperties {

  // cache spark local properties context data per stage id, as it should not change in between
  @transient private val cachedContextData = mutable.Map[Int,ReadOnlyStringMap]()
  def getContextData: ReadOnlyStringMap = {
    val tc = TaskContext.get()
    if (tc != null) {
      cachedContextData.getOrElseUpdate(tc.stageId(), {
        val data = new DefaultThreadContextMap()
        AppUtil.MDC_SDLB_PROPERTIES
          .flatMap(k => Option((k, tc.getLocalProperty(k))))
          .foreach { case (k, v) => data.put(k, v) }
        AppUtil.getMachineContext
          .foreach { case (k, v) => data.put(k, v) }
        data
      })
    } else emptyContextMap
  }
  private final val emptyContextMap = new DefaultThreadContextMap()
}