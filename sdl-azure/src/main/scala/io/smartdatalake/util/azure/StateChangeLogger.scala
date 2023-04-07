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

package io.smartdatalake.util.azure

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.misc.ProductUtil.attributesWithValuesForCaseClass
import io.smartdatalake.util.misc.ScalaUtil.{optionalizeMap, optionalizeSeq}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.action.RuntimeInfo
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import java.time.LocalDateTime

/**
 * Log state changes to LogAnalytics workspace.
 * Supports LogAnalyticsIngestionBackend and the older LogAnalyticsHttpCollectorBackend by defining corresponding configuration as options.
 *
 * To enable add the state listener as follows to global config section:
 *
 * stateListeners = [{
 * className = "io.smartdatalake.util.azure.StateChangeLogger"
 * options = {
 *   endpoint: "https://sdlb-log-w2po.switzerlandnorth-1.ingest.monitor.azure.com"
 *   ruleId: "dcr-e7e5363647ed4f0ab7926e0e02bb8be5"
 *   tableName: "sdlb_state_CL"
 *   includeMetadata: "true" # optionally disable logging data object metadata
 * }
 * }]
 */
class StateChangeLogger(options: Map[String, StringOrSecret]) extends StateListener with SmartDataLakeLogger {

  val includeMetadata = options.get("includeMetadata").map(_.resolve().toBoolean).getOrElse(false)
  val batchSize = 100 // azure log analytics' limit

  val backend: LogAnalyticsBackend[StateLogEvent] = if (options.isDefinedAt("workspaceId")) {
    // LogAnalyticsHttpCollectorBackend
    val workspaceId =  options.getOrElse("workspaceID", throw new ConfigurationException(s"Option workspaceID needed for ${this.getClass.getSimpleName}")).resolve()
    val workspaceKey = options.getOrElse("workspaceKey", throw new ConfigurationException(s"Option workspaceKey needed for ${this.getClass.getSimpleName}")).resolve()
    val logType = options.get("logType").map(_.resolve()).getOrElse("sdlb_state")
    new LogAnalyticsHttpCollectorBackend[StateLogEvent](workspaceId, workspaceKey, logType, serialize)
  } else if (options.isDefinedAt("endpoint")) {
    // LogAnalyticsIngestionBackend
    val endpoint =  options.getOrElse("endpoint", throw new ConfigurationException(s"Option endpoint needed for ${this.getClass.getSimpleName}")).resolve()
    val ruleId =  options.getOrElse("ruleId", throw new ConfigurationException(s"Option ruleId needed for ${this.getClass.getSimpleName}")).resolve()
    val tableName =  options.getOrElse("tableName", throw new ConfigurationException(s"Option tableName needed for ${this.getClass.getSimpleName}")).resolve()
    val batchSize =  options.get("batchSize").map(_.resolve().toInt).getOrElse(100)
    new LogAnalyticsIngestionBackend[StateLogEvent](endpoint, ruleId, tableName, batchSize, serialize)
  } else throw new ConfigurationException("Configuration options missing for LogAnalyticsHttpCollectorBackend or LogAnalyticsIngestionBackend")

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  override def init(context: ActionPipelineContext): Unit = {
    logger.debug(s"initialized")
  }

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[ActionId]): Unit = {
    val logContext = StateLogEventContext.from(context, state.isFinal)
    if (state.isFinal) {
      val events = state.actionsState.flatMap { case (actionId, runtimeInfo) =>
        extractLogEvents(actionId, runtimeInfo, logContext, context.instanceRegistry)
      }.toSeq
      sendLogEvents(events)
    }
    else if (changedActionId.isDefined) {
      val changedActionState = state.actionsState.getOrElse(changedActionId.get, throw new IllegalStateException(s"changed $changedActionId not found in state!"))
      val logEvents = extractLogEvents(changedActionId.get, changedActionState, logContext, context.instanceRegistry)
      sendLogEvents(logEvents)
    }
  }

  def extractLogEvents(actionId: ActionId, runtimeInfo: RuntimeInfo, logContext: StateLogEventContext, instanceRegistry: InstanceRegistry): Seq[StateLogEvent] = {
    val results = runtimeInfo.results.map {
      result =>
        val metadata = instanceRegistry.get[DataObject](result.subFeed.dataObjectId).metadata
        val metadataMap: Map[String, String] = if (includeMetadata) attributesWithValuesForCaseClass(metadata).toMap.filterKeys(_ != "description").mapValues(_.toString)
        else Map()
        val dataObjectsState = runtimeInfo.dataObjectsState.find(_.dataObjectId == result.subFeed.dataObjectId).map(_.state)
        StateLogEvent(logContext, actionId.id, runtimeInfo.state.toString, runtimeInfo.msg,
          Some(result.subFeed.dataObjectId.id), optionalizeMap(metadataMap), optionalizeMap(result.mainMetrics), optionalizeSeq(result.subFeed.partitionValues.map(_.toString)), dataObjectsState)
    }
    // generate at least one log entry per Action if no results
    if (results.nonEmpty) results
    else Seq(StateLogEvent(logContext, actionId.id, runtimeInfo.state.toString, runtimeInfo.msg))
  }

  private def sendLogEvents(logEvents: Seq[StateLogEvent]): Unit = {
    logEvents.grouped(batchSize).foreach {
      logEvents =>
        backend.send(logEvents)
    }
    logger.debug("sending completed")
  }

  private def serialize(event: StateLogEvent): String = Serialization.write(event)
}

case class StateLogEventContext(application: String, startTime: LocalDateTime, runId: Int, attemptId: Int, phase: String, isFinal: Boolean)

object StateLogEventContext {
  def from(context: ActionPipelineContext, isFinal: Boolean): StateLogEventContext = {
    StateLogEventContext(
      application = context.application,
      startTime = context.runStartTime,
      runId = context.executionId.runId,
      attemptId = context.executionId.attemptId,
      phase = context.phase.toString,
      isFinal = isFinal
    )
  }
}

case class StateLogEvent(context: StateLogEventContext, actionId: String, state: String, msg: Option[String], dataObjectId: Option[String] = None, metadata: Option[Map[String, String]] = None, metrics: Option[Map[String, Any]] = None, partitionValues: Option[Seq[String]] = None, dataObjectsState: Option[String] = None)

