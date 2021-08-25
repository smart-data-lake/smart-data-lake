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

package io.smartdatalake.workflow.action

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.{ActionMetrics, DataObjectState, InitSubFeed, SubFeed}
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState

import java.time.{Duration, LocalDateTime}
import scala.collection.mutable

/**
 * Collect runtime data (events and metrics) for repeated execution of actions and asynchronous execution
 */
private[smartdatalake] trait RuntimeData {
  // collect execution data
  protected val executions: mutable.Buffer[ExecutionData[ExecutionId]] = mutable.Buffer()
  protected var currentExecution: Option[ExecutionData[ExecutionId]] = None
  // the number of executions to keep to implement housekeeping
  def numberOfExecutionToKeep: Int
  protected def doHousekeeping(): Unit = {
    if (executions.size > numberOfExecutionToKeep) executions.remove(0) // remove first element
  }
  def addEvent(executionId: ExecutionId, event: RuntimeEvent): Unit
  def addMetric(executionId: Option[ExecutionId], dataObjectId: DataObjectId, metric: ActionMetrics): Unit
  def currentExecutionId: Option[ExecutionId] = currentExecution.map(_.id)
  def getLatestEventState: Option[RuntimeEventState] = currentExecution.flatMap(_.getLatestEvent.map(_.state))
  def isLatestExecutionFinal: Option[Boolean] = getLatestEventState.map(RuntimeEventState.isFinal)
  def clear(): Unit = {
    executions.clear
    currentExecution = None
  }

  /**
   * Get the latest metrics for a DataObject and a given ExecutionId.
   * If ExecutionId is empty, metrics for the current existing ExecutionId is returned.
   */
  def getEvents(executionIdOpt: Option[ExecutionId] = None): Seq[RuntimeEvent] = {
    if (executionIdOpt.isEmpty) currentExecution.map(_.getEvents).getOrElse(Seq())
    else executions.find(_.id == executionIdOpt.get).map(_.getEvents).getOrElse(Seq())
  }

  /**
   * Get the latest metrics for a DataObject and a given ExecutionId.
   * If ExecutionId is empty, metrics for the current existing ExecutionId is returned.
   */
  def getMetrics(dataObjectId: DataObjectId, executionIdOpt: Option[ExecutionId] = None): Option[ActionMetrics] = {
    if (executionIdOpt.isEmpty) currentExecution.flatMap(_.getLatestMetric(dataObjectId))
    else executions.find(_.id == executionIdOpt.get).flatMap(_.getLatestMetric(dataObjectId))
  }

  /**
   * Get the latest metrics for a specific DataObject and current ExecutionId and mark it as final.
   * The method remembers the call per ExecutionId and DataObject to warn about late arriving metrics.
   * This method is for internal use only!
   */
  def getFinalMetrics(dataObjectId: DataObjectId): Option[ActionMetrics] = {
    currentExecution.flatMap(_.getFinalMetric(dataObjectId))
  }

  /**
   * Get the latest metrics for all DataObjects and a given ExecutionId.
   * If ExecutionId is empty, metrics for the current existing ExecutionId is returned.
   */
  def getRuntimeInfo(outputIds: Seq[DataObjectId], dataObjectsState: Seq[DataObjectState], executionIdOpt: Option[ExecutionId] = None): Option[RuntimeInfo] = {
    if(executions.isEmpty) None
    else {
      if (executionIdOpt.isEmpty) currentExecution.flatMap(_.getRuntimeInfo(outputIds, dataObjectsState))
      else executions.find(_.id == executionIdOpt.get).flatMap(_.getRuntimeInfo(outputIds, dataObjectsState))
    }
  }
}

/**
 * Easy implementation of RuntimeData for synchronous Actions
 */
private[smartdatalake] case class SynchronousRuntimeData(override val numberOfExecutionToKeep: Int) extends RuntimeData {
  override def addEvent(executionId: ExecutionId, event: RuntimeEvent): Unit = {
    assert(currentExecutionId.forall(_ <= executionId), s"ExecutionId $executionId smaller than currentExecutionId $currentExecutionId")
    if (currentExecutionId.forall(_ < executionId)) {
      currentExecution = Some(ExecutionData(executionId))
      executions.append(currentExecution.get)
    }
    currentExecution.get.addEvent(event)
    doHousekeeping()
  }
  override def addMetric(executionId: Option[ExecutionId], dataObjectId: DataObjectId, metric: ActionMetrics): Unit = {
    assert(executionId.nonEmpty, "ExecutionId must be defined")
    assert(currentExecutionId.forall(_ == executionId.get), s"New metric's ExecutionId $executionId is different than current ExecutionId $currentExecutionId")
    if (currentExecutionId.forall(_ < executionId.get)) {
      currentExecution = Some(ExecutionData(executionId.get))
      executions.append(currentExecution.get)
    }
    currentExecution.get.addMetric(dataObjectId, metric)
  }
}
/**
 * Asynchronous Actions might receive Events & Metrics with synchronous ExecutionId.
 * The synchronous events are created by initial synchronous Execution. There are asynchronous Events for the same Execution.
 * The implementation keeps both, but currentExecution will always be the asynchronous Execution.
 * The synchronous metrics are created by Spark Streaming with foreachBatch and must be assigned to the corresponding asynchronous Execution.
 */
private[smartdatalake] case class AsynchronousRuntimeData(override val numberOfExecutionToKeep: Int) extends RuntimeData {
  // temporarily hold metrics with SDLExecutionId or without executionId
  private val unassignedMetrics: mutable.Buffer[(DataObjectId,ActionMetrics)] = mutable.Buffer()
  // keep track of first SDLExecutionId for logic in getRuntimeInfo
  private var firstSDLExecutionId: Option[SDLExecutionId] = None
  override def addEvent(executionId: ExecutionId, event: RuntimeEvent): Unit = {
    executionId match {
      case sdlExecutionId: SDLExecutionId =>
        var execution = executions.find(_.id == executionId)
        // only collect the first synchronous execution and ignore the following, as they have no relevant information for asynchronous actions.
        if (execution.isEmpty && executions.isEmpty) {
          execution = Some(ExecutionData(executionId))
          executions.append(execution.get)
          firstSDLExecutionId = Some(sdlExecutionId)
        }
        if (execution.nonEmpty) execution.get.addEvent(event)
      case _ =>
        assert(currentExecutionId.forall(_ <= executionId), "ExecutionId smaller than currentExecutionId")
        if (currentExecutionId.forall(_ < executionId)) {
          currentExecution = Some(ExecutionData(executionId))
          executions.append(currentExecution.get)
        }
        currentExecution.get.addEvent(event)
        // add unassigned synchronous metrics
        unassignedMetrics.foreach { case (d, m) => currentExecution.get.addMetric(d, m) }
        unassignedMetrics.clear
        doHousekeeping()
    }
  }
  override def addMetric(executionId: Option[ExecutionId], dataObjectId: DataObjectId, metric: ActionMetrics): Unit = {
    // special handling of synchronous metrics: add to corresponding asynchronous execution
    if (executionId.isEmpty || executionId.get.isInstanceOf[SDLExecutionId]) {
      if (currentExecution.isEmpty || isLatestExecutionFinal.getOrElse(true)) unassignedMetrics.append((dataObjectId, metric))
      else currentExecution.get.addMetric(dataObjectId, metric) // add to current asynchronous execution data
    } else {
      assert(currentExecutionId.forall(_ == executionId.get), s"New metric received for other than current ExecutionId: currentExecutionId=$currentExecutionId executionId=$executionId")
      currentExecution.get.addMetric(dataObjectId, metric)
    }
  }
  override def clear(): Unit = {
    super.clear()
    unassignedMetrics.clear
    firstSDLExecutionId = None
  }
  override def getRuntimeInfo(outputIds: Seq[DataObjectId], dataObjectsState: Seq[DataObjectState], executionIdOpt: Option[ExecutionId]): Option[RuntimeInfo] = {
    if (executionIdOpt.exists(_.isInstanceOf[SDLExecutionId])) {
      // report state as STREAMING after first SDLexecutionId only. This allows to skip first execution in recovery if SUCCEEDED.
      if (executionIdOpt == firstSDLExecutionId) super.getRuntimeInfo(outputIds, dataObjectsState, executionIdOpt)
      else super.getRuntimeInfo(outputIds, dataObjectsState, None).map(_.copy(state = RuntimeEventState.STREAMING))
    } else {
      super.getRuntimeInfo(outputIds, dataObjectsState, executionIdOpt)
    }
  }
}

/**
 * A structure to collect events & metrics for one action execution
 */
private[smartdatalake] case class ExecutionData[A <: ExecutionId](id: A) {
  private val events: mutable.Buffer[RuntimeEvent] = mutable.Buffer()
  private val metrics: mutable.Map[DataObjectId,mutable.Buffer[ActionMetrics]] = mutable.Map()
  private val metricsDelivered = mutable.Set[DataObjectId]()
  def isAsynchronous: Boolean = !id.isInstanceOf[SDLExecutionId]
  def addEvent(event: RuntimeEvent): Unit = events.append(event)
  def addMetric(dataObjectId: DataObjectId, metric: ActionMetrics): Unit = {
    val dataObjectMetrics = metrics.getOrElseUpdate(dataObjectId, mutable.Buffer[ActionMetrics]())
    dataObjectMetrics.append(metric)
    if (metricsDelivered.contains(dataObjectId))
      throw LateArrivingMetricException(s"Late arriving metrics for $dataObjectId detected. Final metrics have already been delivered.")
  }
  def getEvents: Seq[RuntimeEvent] = events
  def getLatestEvent: Option[RuntimeEvent] = {
    events.lastOption
  }
  def getLatestMetric(dataObjectId: DataObjectId): Option[ActionMetrics] = {
    metrics.get(dataObjectId).map(_.maxBy(_.getOrder))
  }
  def getFinalMetric(dataObjectId: DataObjectId): Option[ActionMetrics] = {
    val latestMetrics = getLatestMetric(dataObjectId)
      .orElse {
        // wait some time and retry, because the metrics might be delivered by another thread...
        Thread.sleep(500)
        getLatestMetric(dataObjectId)
      }
    // remember for which data object final metrics has been delivered, so that we can warn on late arriving metrics!
    metricsDelivered += dataObjectId
    // return
    latestMetrics
  }
  def getRuntimeInfo(outputIds: Seq[DataObjectId], dataObjectsState: Seq[DataObjectState]): Option[RuntimeInfo] = {
    assert(events.nonEmpty, "Cannot getRuntimeInfo if events are empty")
    val lastEvent = events.last
    val lastResults = events.reverseIterator.map(_.results).find(_.nonEmpty) // on failed actions we take the results from initialization to store what partition values have been tried to process
    val startEvent = events.reverseIterator.find(event => event.state == RuntimeEventState.STARTED && event.phase == lastEvent.phase)
    val duration = startEvent.map(start => Duration.between(start.tstmp, lastEvent.tstmp))
    val outputSubFeeds = if (lastEvent.state != RuntimeEventState.SKIPPED) lastResults.toSeq.flatten
    else outputIds.map(outputId => InitSubFeed(outputId, partitionValues = Seq(), isSkipped = true)) // fake results for skipped actions for state information
    val results = outputSubFeeds.map(subFeed => ResultRuntimeInfo(subFeed, getLatestMetric(subFeed.dataObjectId).map(_.getMainInfos).getOrElse(Map())))
    Some(RuntimeInfo(id, lastEvent.state, startTstmp = startEvent.map(_.tstmp), duration = duration, msg = lastEvent.msg, results = results, dataObjectsState = dataObjectsState))
  }
}
private[smartdatalake] case class LateArrivingMetricException(msg: String) extends Exception(msg)

/**
 * A structure to collect runtime event information
 */
private[smartdatalake] case class RuntimeEvent(tstmp: LocalDateTime, phase: ExecutionPhase, state: RuntimeEventState, msg: Option[String], results: Seq[SubFeed])
private[smartdatalake] object RuntimeEventState extends Enumeration {
  type RuntimeEventState = Value
  val STARTED, PREPARED, INITIALIZED, SUCCEEDED, FAILED, CANCELLED, SKIPPED, PENDING, STREAMING = Value
  def isFinal(runtimeEventState: RuntimeEventState): Boolean = Seq(SUCCEEDED, FAILED, CANCELLED, SKIPPED).contains(runtimeEventState)
}

/**
 * Trait to identify action execution.
 * Execution id's must be sortable.
 */
private[smartdatalake] sealed trait ExecutionId extends Ordered[ExecutionId]

/**
 * Standard execution id for actions that are executed synchronous by SDL.
 */
private[smartdatalake] case class SDLExecutionId(runId: Int, attemptId: Int = 1) extends ExecutionId {
  override def toString: String = s"$runId-$attemptId"
  override def compare(that: ExecutionId): Int = that match {
    case that: SDLExecutionId => SDLExecutionId.ordering.compare(this, that)
    case _ => throw new RuntimeException(s"SDLExecutionId cannot be compare with ${that.getClass.getSimpleName}")
  }
  def incrementRunId: SDLExecutionId = this.copy(runId = runId + 1, attemptId = 1)
  def incrementAttemptId: SDLExecutionId = this.copy(attemptId = this.attemptId + 1)
}
private[smartdatalake] object SDLExecutionId {
  val ordering: Ordering[SDLExecutionId] = Ordering.by(x => (x.runId, x.attemptId))
  val executionId1: SDLExecutionId = SDLExecutionId(1)
}

/**
 * Execution id for spark streaming jobs.
 * They need a different execution id as they are executed asynchronous.
 */
private[smartdatalake] case class SparkStreamingExecutionId(batchId: Long) extends ExecutionId {
  override def toString: String = s"$batchId"
  override def compare(that: ExecutionId): Int = that match {
    case that: SparkStreamingExecutionId => SparkStreamingExecutionId.ordering.compare(this, that)
    case _ => throw new RuntimeException(s"SparkStreamingExecutionId cannot be compare with ${that.getClass.getSimpleName}")
  }
}
private[smartdatalake] object SparkStreamingExecutionId {
  val ordering: Ordering[SparkStreamingExecutionId] = Ordering.by(x => x.batchId)
}

/**
 * Summarized runtime information
 */
private[smartdatalake] case class RuntimeInfo(
                                               executionId: ExecutionId,
                                               state: RuntimeEventState,
                                               startTstmp: Option[LocalDateTime] = None,
                                               duration: Option[Duration] = None,
                                               msg: Option[String] = None,
                                               results: Seq[ResultRuntimeInfo] = Seq(),
                                               dataObjectsState: Seq[DataObjectState] = Seq()
                                             ) {
  /**
   * Completed Actions will be ignored in recovery
   */
  def hasCompleted: Boolean = state==RuntimeEventState.SUCCEEDED || state==RuntimeEventState.SKIPPED
  override def toString: String = duration.map(d => s"$state $d").getOrElse(state.toString)
}
private[smartdatalake] case class ResultRuntimeInfo(subFeed: SubFeed, mainMetrics: Map[String, Any])
