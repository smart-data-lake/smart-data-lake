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

package io.smartdatalake.metrics

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.{DataFrameActionImpl, RuntimeEventState, SparkStreamingExecutionId}
import io.smartdatalake.workflow.{ActionMetrics, ActionPipelineContext, ExecutionPhase, InitSubFeed}
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, LocalDateTime, ZoneId}
import java.util.UUID
import java.util.concurrent.Semaphore
import scala.collection.mutable

/**
 * Collect metrics for Spark streaming queries
 * This listener registers and unregisters itself in the spark session.
 */
class SparkStreamingQueryListener(action: DataFrameActionImpl, dataObjectId: DataObjectId, queryName: String, firstProgressWaitLock: Option[Semaphore] = None)(implicit context: ActionPipelineContext) extends StreamingQueryListener with SmartDataLakeLogger {
  private var id: UUID = _
  private var isFirstProgress = true
  context.sparkSession.streams.addListener(this) // self-register
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    if (queryName == event.name) {
      logger.info(s"(${event.name}) streaming query started")
      id = event.id
    }
  }
  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    if (event.progress.id == id) {
      val executionId = SparkStreamingExecutionId(event.progress.batchId)
      val endTstmp = LocalDateTime.ofInstant(Instant.parse(event.progress.timestamp), ZoneId.systemDefault) // String is in UTC. It must be converted to local timezone.
      val startTstmp = endTstmp.minus(event.progress.batchDuration, ChronoUnit.MILLIS) // start time is not stored in event...
      action.addRuntimeEvent(executionId, ExecutionPhase.Exec, RuntimeEventState.STARTED, tstmp = startTstmp)
      val streamingMetrics = SparkStreamingMetrics(event.progress)
      logger.info(s"(${event.progress.name}) streaming query ${if (streamingMetrics.noData) "had no data" else "made progress"}: batchId=${event.progress.batchId} ${streamingMetrics.getAsText}")
      if (!streamingMetrics.noData) {
        action.addAsyncMetrics(Some(SparkStreamingExecutionId(event.progress.batchId)), Some(dataObjectId), streamingMetrics)
        action.addRuntimeEvent(executionId, ExecutionPhase.Exec, RuntimeEventState.SUCCEEDED, tstmp = endTstmp, results = Seq(InitSubFeed(dataObjectId, partitionValues = Seq()))) // dummy results provided for runtime info
      }
      releaseFirstProgressWaitLock()
    }
  }
  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    if (event.id == id) {
      logger.info(s"($queryName) streaming query terminated ${event.exception.map(e => s"exception=$e").getOrElse("normally")}")
      context.sparkSession.streams.removeListener(this) // self-unregister
      action.notifySparkStreamingQueryTerminated
      releaseFirstProgressWaitLock()
    }
    Environment.stopStreamingGracefully = true // stop synchronous actions
  }
  private def releaseFirstProgressWaitLock(): Unit = {
    if (isFirstProgress) {
      firstProgressWaitLock.foreach(_.release())
      isFirstProgress = false
    }
  }
}

case class SparkStreamingMetrics(progress: StreamingQueryProgress) extends ActionMetrics {

  val noData: Boolean = (progress.durationMs.size <= 2) // if only 2 phases have run, there was no data...

  override val getId: String = s"streaming-${progress.batchId}"

  override val getOrder: Long = Instant.parse(progress.timestamp).getEpochSecond

  override def getMainInfos: Map[String, Any] = {
    val metrics = mutable.Map[String, Any]("batch_duration" -> progress.batchDuration, "no_data" -> noData)
    if (progress.sink.numOutputRows >= 0) { // -1 if reporting metrics is not supported by sink
      metrics += ("records_written" -> progress.sink.numOutputRows)
    }
    metrics.toMap
  }
}