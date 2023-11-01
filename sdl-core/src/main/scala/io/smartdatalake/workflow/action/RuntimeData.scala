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
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow._

import java.time.{Duration, LocalDateTime}
import scala.collection.mutable

/**
 * Collect runtime data (events and metrics) for repeated execution of actions and asynchronous execution
 */
private[smartdatalake] trait RuntimeData {
  // collect execution data
  protected val executions: mutable.Buffer[ExecutionData[ExecutionId]] = mutable.Buffer()
  protected var currentExecution: Option[ExecutionData[ExecutionId]] = None
  protected var lastExecution: Option[ExecutionData[ExecutionId]] = None
  // the number of executions to keep to implement housekeeping
  def numberOfExecutionToKeep: Int
  protected def doHousekeeping(): Unit = {
    if (executions.size > numberOfExecutionToKeep) executions.remove(0) // remove first element
  }
  def addEvent(executionId: ExecutionId, event: RuntimeEvent): Unit
  def addMetric(executionId: Option[ExecutionId], dataObjectId: DataObjectId, metric: ActionMetrics): Unit = throw new NotImplementedError()
  def currentExecutionId: Option[ExecutionId] = currentExecution.map(_.id)
  def lastExecutionId: Option[ExecutionId] = lastExecution.map(_.id)
  def getLatestEventState: Option[RuntimeEventState] = lastExecution.flatMap(_.getLatestEvent.map(_.state))
  def clear(): Unit = {
    executions.clear
    currentExecution = None
    lastExecution = None
  }

  /**
   * Get the latest metrics for a DataObject and a given ExecutionId.
   * If ExecutionId is empty, metrics for the current existing ExecutionId is returned.
   */
  def getEvents(executionIdOpt: Option[ExecutionId] = None): Seq[RuntimeEvent] = {
    if (executionIdOpt.isEmpty) lastExecution.map(_.getEvents).getOrElse(Seq())
    else executions.find(_.id == executionIdOpt.get).map(_.getEvents).getOrElse(Seq())
  }

  /**
   * Get the latest metrics for a DataObject and a given ExecutionId.
   * If ExecutionId is empty, metrics for the current existing ExecutionId is returned.
   */
  def getMetrics(dataObjectId: DataObjectId, executionIdOpt: Option[ExecutionId] = None): Option[ActionMetrics] = {
    if (executionIdOpt.isEmpty) lastExecution.flatMap(_.getLatestMetrics(dataObjectId))
    else executions.find(_.id == executionIdOpt.get).flatMap(_.getLatestMetrics(dataObjectId))
  }

  /**
   * Get the latest metrics for all DataObjects and a given ExecutionId.
   * If ExecutionId is empty, metrics for the current existing ExecutionId is returned.
   */
  def getRuntimeInfo(inputIds: Seq[DataObjectId], outputIds: Seq[DataObjectId], dataObjectsState: Seq[DataObjectState], executionIdOpt: Option[ExecutionId] = None): Option[RuntimeInfo] = {
    if(executions.isEmpty) None
    else {
      if (executionIdOpt.isEmpty) lastExecution.flatMap(_.getRuntimeInfo(inputIds, outputIds, dataObjectsState))
      else executions.find(_.id == executionIdOpt.get).flatMap(_.getRuntimeInfo(inputIds, outputIds, dataObjectsState))
    }
  }
}

/**
 * Easy implementation of RuntimeData for synchronous Actions
 * Note that this does not support collecting metrics, as these are passed on directly in the corresponding SubFeed for synchronous Actions.
 */
private[smartdatalake] case class SynchronousRuntimeData(override val numberOfExecutionToKeep: Int) extends RuntimeData {
  override def addEvent(executionId: ExecutionId, event: RuntimeEvent): Unit = {
    assert(lastExecutionId.forall(_ <= executionId), s"ExecutionId $executionId smaller than currentExecutionId $lastExecutionId")
    if (lastExecutionId.forall(_ < executionId)) {
      currentExecution = Some(ExecutionData(executionId))
      lastExecution = currentExecution
      executions.append(currentExecution.get)
    }
    currentExecution.get.addEvent(event)
    doHousekeeping()
  }
}

/**
 * Asynchronous Actions might receive Events with synchronous ExecutionId.
 * The synchronous events are created by initial synchronous Execution. There are asynchronous Events for the same Execution.
 * The implementation keeps both, but currentExecution will always be the asynchronous Execution.
 */
private[smartdatalake] case class AsynchronousRuntimeData(override val numberOfExecutionToKeep: Int) extends RuntimeData {
  // temporarily hold metrics without executionId
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
        assert(lastExecutionId.forall(_ <= executionId), "ExecutionId smaller than currentExecutionId")
        if (lastExecutionId.forall(_ < executionId)) {
          currentExecution = Some(ExecutionData(executionId))
          lastExecution = currentExecution
          executions.append(currentExecution.get)
        }
        currentExecution.get.addEvent(event)
        // add unassigned synchronous metrics
        unassignedMetrics.foreach { case (d, m) => currentExecution.get.addMetrics(d, m) }
        unassignedMetrics.clear
        // remove current execution on final state. This is needed for handling unassigned Metrics.
        if (RuntimeEventState.isFinal(event.state)) currentExecution = None
        doHousekeeping()
    }
  }
  override def addMetric(executionId: Option[ExecutionId], dataObjectId: DataObjectId, metrics: ActionMetrics): Unit = {
    assert(!executionId.exists(_.isInstanceOf[SDLExecutionId]), "Can not handle synchronous metrics")
    assert(currentExecutionId.isEmpty || executionId.isEmpty || currentExecutionId == executionId, s"New metric received for other than current ExecutionId: currentExecutionId=$currentExecutionId executionId=$executionId")
    if (currentExecution.isEmpty) unassignedMetrics.append((dataObjectId, metrics))
    else currentExecution.get.addMetrics(dataObjectId, metrics)
    // also add to firstSDLExecution if not final
    firstSDLExecutionId.flatMap(id => executions.find(_.id == id))
      .filterNot(_.isFinal).foreach(_.addMetrics(dataObjectId, metrics))
  }
  override def clear(): Unit = {
    super.clear()
    firstSDLExecutionId = None
  }
  override def getRuntimeInfo(inputIds: Seq[DataObjectId], outputIds: Seq[DataObjectId], dataObjectsState: Seq[DataObjectState], executionIdOpt: Option[ExecutionId]): Option[RuntimeInfo] = {
    // report state as STREAMING after first SDLexecutionId only. This allows to skip first execution in recovery if SUCCEEDED.
    if (executionIdOpt.exists(_.isInstanceOf[SDLExecutionId])) {
      if (executionIdOpt == firstSDLExecutionId) super.getRuntimeInfo(inputIds, outputIds, dataObjectsState, executionIdOpt)
      else super.getRuntimeInfo(inputIds, outputIds, dataObjectsState, None).map(_.copy(state = RuntimeEventState.STREAMING))
    } else {
      super.getRuntimeInfo(inputIds, outputIds, dataObjectsState, executionIdOpt)
    }
  }
}

/**
 * A structure to collect events & metrics for one action execution
 */
private[smartdatalake] case class ExecutionData[A <: ExecutionId](id: A) {
  private val events: mutable.Buffer[RuntimeEvent] = mutable.Buffer()
  private val metricsMap: mutable.Map[DataObjectId,mutable.Buffer[ActionMetrics]] = mutable.Map()
  def isAsynchronous: Boolean = !id.isInstanceOf[SDLExecutionId]
  def addEvent(event: RuntimeEvent): Unit = events.append(event)
  def addMetrics(dataObjectId: DataObjectId, metrics: ActionMetrics): Unit = {
    val dataObjectMetrics = metricsMap.getOrElseUpdate(dataObjectId, mutable.Buffer[ActionMetrics]())
    dataObjectMetrics.append(metrics)
  }
  def getEvents: Seq[RuntimeEvent] = events
  def getLatestEvent: Option[RuntimeEvent] = {
    events.lastOption
  }
  def isFinal: Boolean = {
    getLatestEvent.exists(e => RuntimeEventState.isFinal(e.state))
  }

  /**
   * Get the latest metrics for this ExecutionData.
   * This should not be used directly, as it does not include metrics included in SubFeeds.
   * Use getRuntimeInfo instead, and extract metrics from results.
   */
  private[action] def getLatestMetrics(dataObjectId: DataObjectId): Option[ActionMetrics] = {
    // combine latest metric for all types
    val metrics = metricsMap.get(dataObjectId).map(_.groupBy(_.getClass).values.map(_.maxBy(_.getOrder).getMainInfos).reduceOption(_ ++ _).getOrElse(Map()))
    metrics.map(GenericMetrics("latest", 1, _))
  }
  def getRuntimeInfo(inputIds: Seq[DataObjectId], outputIds: Seq[DataObjectId], dataObjectsState: Seq[DataObjectState]): Option[RuntimeInfo] = {
    assert(events.nonEmpty, "Cannot getRuntimeInfo if events are empty")
    val lastEvent = events.last
    val lastResults = events.reverseIterator.map(_.results).find(_.nonEmpty) // on failed actions we take the results from initialization to store what partition values have been tried to process

    val prepareEvents = events.filter(p => p.phase == ExecutionPhase.Prepare)
    val initEvents = events.filter(i => i.phase == ExecutionPhase.Init)
    val execEvents = events.filter(e => e.phase == ExecutionPhase.Exec)
    val prepareStartEvent = prepareEvents.headOption
    val prepareEndEvent = if(prepareEvents.nonEmpty && prepareEvents.lastOption!=prepareStartEvent) prepareEvents.lastOption else None
    val initStartEvent = initEvents.headOption
    val initEndEvent = if(initEvents.nonEmpty && initEvents.lastOption!=initStartEvent) initEvents.lastOption else None
    val execStartEvent = execEvents.headOption
    val execEndEvent = if(execEvents.nonEmpty && execEvents.lastOption!=execStartEvent) execEvents.lastOption else None
    val startEventLastPhase = events.reverseIterator.find(event => event.state == RuntimeEventState.STARTED && event.phase == lastEvent.phase)
    // Duration of last successful phase
    val duration = startEventLastPhase.map(start => Duration.between(start.tstmp, lastEvent.tstmp))
    val outputSubFeeds = if (lastEvent.state != RuntimeEventState.SKIPPED) {
      // enrich with potential metrics
      lastResults.toSeq.flatten.map(subFeed => subFeed.appendMetrics(getLatestMetrics(subFeed.dataObjectId).map(_.getMainInfos).getOrElse(Map())))
    }
    // TODO: why fake results? partitionValues should be preserved!
    else outputIds.map(outputId => InitSubFeed(outputId, partitionValues = Seq(), isSkipped = true)) // fake results for skipped actions for state information
    Some(RuntimeInfo(id, lastEvent.state,
      startTstmp = execStartEvent.map(_.tstmp), endTstmp = execEndEvent.map(_.tstmp),
      duration = duration,
      startTstmpPrepare = prepareStartEvent.map(_.tstmp), endTstmpPrepare = prepareEndEvent.map(_.tstmp),
      startTstmpInit = initStartEvent.map(_.tstmp), endTstmpInit = initEndEvent.map(_.tstmp),
      msg = lastEvent.msg, results = outputSubFeeds, dataObjectsState = dataObjectsState, inputIds = inputIds, outputIds = outputIds))
  }
}

/**
 * A structure to collect runtime event information
 */
case class RuntimeEvent(tstmp: LocalDateTime, phase: ExecutionPhase, state: RuntimeEventState, msg: Option[String], results: Seq[SubFeed])
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
case class SDLExecutionId(runId: Int, attemptId: Int = 1) extends ExecutionId {
  override def toString: String = s"$runId-$attemptId"
  override def compare(that: ExecutionId): Int = that match {
    case that: SDLExecutionId => SDLExecutionId.ordering.compare(this, that)
    case _ => throw new RuntimeException(s"SDLExecutionId cannot be compare with ${that.getClass.getSimpleName}")
  }
  private[smartdatalake] def incrementRunId: SDLExecutionId = this.copy(runId = runId + 1, attemptId = 1)
  private[smartdatalake] def incrementAttemptId: SDLExecutionId = this.copy(attemptId = this.attemptId + 1)
}
object SDLExecutionId {
  private[smartdatalake] val ordering: Ordering[SDLExecutionId] = Ordering.by(x => (x.runId, x.attemptId))
  val executionId1: SDLExecutionId = SDLExecutionId(1)
}

/**
 * Execution id for spark streaming jobs.
 * They need a different execution id as they are executed asynchronous.
 */
case class SparkStreamingExecutionId(batchId: Long) extends ExecutionId {
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
case class RuntimeInfo(
                       executionId: ExecutionId = SDLExecutionId(-1, -1), // default value is needed for backward compatibility
                       state: RuntimeEventState,
                       startTstmpPrepare: Option[LocalDateTime] = None,
                       endTstmpPrepare: Option[LocalDateTime] = None,
                       startTstmpInit: Option[LocalDateTime] = None,
                       endTstmpInit: Option[LocalDateTime] = None,
                       startTstmp: Option[LocalDateTime] = None,
                       endTstmp: Option[LocalDateTime] = None,
                       duration: Option[Duration] = None,
                       msg: Option[String] = None,
                       results: Seq[SubFeed] = Seq(),
                       dataObjectsState: Seq[DataObjectState] = Seq(),
                       inputIds: Seq[DataObjectId] = Seq(),
                       outputIds: Seq[DataObjectId] = Seq()
                     ) {
  /**
   * Completed Actions will be ignored in recovery
   */
  def hasCompleted: Boolean = state==RuntimeEventState.SUCCEEDED || state==RuntimeEventState.SKIPPED
  override def toString: String = duration.map(d => s"$state $d").getOrElse(state.toString)
}