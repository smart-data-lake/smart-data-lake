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

import com.google.gson.Gson
import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.azure.client.loganalytics.LogAnalyticsClient
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.action.{ResultRuntimeInfo, RuntimeInfo}
import io.smartdatalake.workflow.dataobject.{DataObject, DataObjectMetadata}
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}

import java.time.LocalDateTime

/**
 * Log state changes to LogAnalytics workspace
 *
 * To enable add the state listener as follows to global config section:
 *
 * stateListeners = [{
 * className = "io.smartdatalake.util.azure.StateChangeLogger"
 * options = {
 * workspaceID : "xxx",         // workspace ID found under azure LogAnalytics workspace's 'agents management' section
 * logAnalyticsKey : "xxx",     // primary or secondary key found under azure LogAnalytics workspace's 'agents management' section
 * logType : "__yourLogType__"  // optional name of log type for LogAnalytics, default is StateChange.
 * }
 * }]
 */
class StateChangeLogger(options: Map[String, StringOrSecret]) extends StateListener with SmartDataLakeLogger {

  assert(options.contains("workspaceID"))
  assert(options.contains("logAnalyticsKey"))

  val logType: String = options.get("logType").map(_.resolve()).getOrElse("StateChange")
  val maxDocumentsNumber = 100 // azure log analytics' limit

  lazy private val logAnalyticsKey: String = options("logAnalyticsKey").resolve()
  lazy private val azureLogClient = new LogAnalyticsClient(options("workspaceID").resolve(), logAnalyticsKey)

  override def init(context: ActionPipelineContext): Unit = {
    azureLogClient // initialize lazy val
    logger.debug(s"initialized: logType=$logType")
  }

  case class StateLogEventContext(thread: String, notificationTime: String, application: String, executionId: String, phase: String, actionId: String, state: String, message: String)

  case class TargetObjectMetadata(name: String, layer: String, subjectArea: String, description: String)

  case class Result(targetObjectMetadata: TargetObjectMetadata, recordsWritten: String, stageDuration: String)

  case class StateLogEvent(context: StateLogEventContext, result: Result)

  private val gson = new Gson

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[ActionId]): Unit = {

    if (state.isFinal) {
      sendLogEvents(state.actionsState.flatMap { aStateEntry => extractLogEvents(aStateEntry._1, aStateEntry._2, context) }.toSeq)
    }
    else if (changedActionId.isDefined) {
      val changedActionState = state.actionsState.getOrElse(changedActionId.get, throw new IllegalStateException(s"changed $changedActionId not found in state!"))
      val logEvents = extractLogEvents(changedActionId.get, changedActionState, context)
      sendLogEvents(logEvents)
    }
  }

  def extractLogEvents(actionId: ActionId, runtimeInfo: RuntimeInfo, context: ActionPipelineContext): Seq[StateLogEvent] = {

    val notificationTime = LocalDateTime.now

    val logContext = StateLogEventContext(Thread.currentThread().getName,
      notificationTime.toString,
      application = context.application,
      executionId = runtimeInfo.executionId.toString,
      phase = context.phase.toString,
      actionId = actionId.toString,
      state = runtimeInfo.state.toString,
      message = runtimeInfo.msg.getOrElse(" no message"))

    val optResults = { // we generate at least one log entry, and one log entry per result
      if (runtimeInfo.results.nonEmpty) runtimeInfo.results.map({ result: ResultRuntimeInfo => Some(result) })
      else Seq(None)
    }
    optResults.map(optResult => {
      val result = optResult.map(
        { result: ResultRuntimeInfo =>

          val toMetadata = {
            val metadata = context.instanceRegistry.get[DataObject](result.subFeed.dataObjectId).metadata

            def extractString(attribute: DataObjectMetadata => Option[String]) = metadata.map(attribute(_).getOrElse("")).getOrElse("")

            TargetObjectMetadata(name = extractString(_.name),
              layer = extractString(_.layer),
              subjectArea = extractString(_.subjectArea),
              description = extractString(_.description))
          }

          Result(toMetadata, result.mainMetrics.getOrElse("records_written", -1).toString, result.mainMetrics.getOrElse("stage_duration", -1).toString)
        })

      StateLogEvent(logContext, result.orNull)
    })
  }

  private def sendLogEvents(logEvents: Seq[StateLogEvent]): Unit = {
    logEvents.grouped(maxDocumentsNumber).foreach {
      logEvents =>
        val jsonEvents = logEvents.map { le: StateLogEvent => gson.toJson(le) }.mkString(",")
        logger.debug("logType " + logType + " sending: " + jsonEvents.toString())
        azureLogClient.send("[ " + jsonEvents + " ]", logType)
    }
    logger.debug("sending completed")
  }
}