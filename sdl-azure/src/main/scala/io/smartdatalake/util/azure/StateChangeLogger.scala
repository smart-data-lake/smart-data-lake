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

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}
import io.smartdatalake.workflow.action.ResultRuntimeInfo
import io.smartdatalake.workflow.dataobject.{DataObject, DataObjectMetadata}
import io.smartdatalake.app.StateListener
import com.google.gson.Gson
import io.smartdatalake.util.azure.client.loganalytics.LogAnalyticsClient
import io.smartdatalake.util.secrets.SecretsUtil
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.workflow.action.RuntimeInfo

import java.time.LocalDateTime

/**
 * usage:
 * in global config section, integrate the following:
 *
 *        stateListeners = [
 *         { className = "io.smartdatalake.util.azure.StateChangeLogger"
 *           options = { workspaceID : "xxx",    // Workspace ID found under azure log analytics workspace's 'agents management' section
 *                       logAnalyticsKey : "xxx",   // primary or secondary key found under azure log analytics workspace's 'agents management' section
 *                        logType : "__yourLogType__"} }
 *        ]
 */
class StateChangeLogger(options: Map[String,String]) extends StateListener with SmartDataLakeLogger {

  assert(options.contains("workspaceID"))
  assert(options.contains("logAnalyticsKey"))

  val logType : String = options.getOrElse("logType", "StateChange")

  lazy private val logAnalyticsKey: String = SecretsUtil.getSecret(options("logAnalyticsKey"))
  lazy private val azureLogClient = new LogAnalyticsClient(options("workspaceID"), logAnalyticsKey)

  override def init(): Unit = {
    logger.info(s"io.smartdatalake.util.log.StateChangeLogger init done, " +
                 s"logType: $logType, key: _${logAnalyticsKey.substring(0,4)} .. ${logAnalyticsKey.substring(logAnalyticsKey.length-4)}_ ")
  }

  case class StateLogEventContext(thread: String, notificationTime: String, application: String, executionId: String, phase: String, actionId: String, state: String, message: String)
  case class TargetObjectMetadata(name: String, layer: String, subjectArea: String, description: String)
  case class Result(targetObjectMetadata: TargetObjectMetadata, recordsWritten: String, stageDuration: String)
  case class StateLogEvent(context: StateLogEventContext, result: Result)

  private val gson = new Gson

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, oChangedActionId : Option[ActionId]): Unit = {
    if (state.isFinal) return  // final notification is redundant -> skip
    if (oChangedActionId.isEmpty) return  // we log only action specific events -> skip

    val changedActionId = oChangedActionId.get
    assert(state.actionsState.get(changedActionId).isDefined)

    val logEvents = extractLogEvents(changedActionId, state.actionsState(changedActionId), context)
    val jsonEvents = logEvents.map{le:StateLogEvent => gson.toJson(le)}.mkString(",")

    logger.debug("logType " + logType+ " sending: " + jsonEvents.toString())
    azureLogClient.send("[ " + jsonEvents + " ]", logType)
    logger.debug("sending completed")
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

    val optResults = {   // we generate at least one log entry, and one log entry per result
      if (runtimeInfo.results.nonEmpty) runtimeInfo.results.map({result : ResultRuntimeInfo => Some(result)})
      else Seq(None)
    }
    optResults.map(optResult => {
      val result = optResult.map(
        {result : ResultRuntimeInfo =>

          val toMetadata = {
            val metadata = context.instanceRegistry.get[DataObject](result.subFeed.dataObjectId).metadata
            def extractString(attribute: DataObjectMetadata => Option[String]) = metadata.map(attribute(_).getOrElse("")).getOrElse("")

            TargetObjectMetadata(name = extractString(_.name),
              layer = extractString(_.layer),
              subjectArea = extractString(_.subjectArea),
              description = extractString(_.description))
          }

          Result(toMetadata, result.mainMetrics.getOrElse("records_written", -1).toString, result.mainMetrics.getOrElse("stageDuration", -1).toString)
        })

      StateLogEvent(logContext, result.orNull)
    })
  }
}