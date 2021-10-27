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

package io.smartdatalake.util.log

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}
import io.smartdatalake.workflow.action.ResultRuntimeInfo
import io.smartdatalake.workflow.dataobject.{DataObject, DataObjectMetadata}
import io.smartdatalake.app.StateListener

import com.google.gson.Gson
import java.time.LocalDateTime

/**
 * usage:
 * in global config section, integrate the following:

        stateListeners = [
          { className = "io.smartdatalake.util.log.StateChangeLogger"
            options = { primaryKey : "xxx",    // primary key found under azure log analytics workspace's 'agents management' section
                        secondaryKey : "xxx",   // secondary key found under azure log analytics workspace's 'agents management' section
                        logType : "__yourLogType__"} }
        ]
*/

/**
 * This code originates from https://github.com/mspnp/spark-monitoring and is protected the corresponding MIT license
 */
class StateChangeLogger(options: Map[String,String]) extends StateListener with SmartDataLakeLogger {

  assert(options.contains("primaryKey"))
  assert(options.contains("secondaryKey"))
  private val azureLogClient = new AzureLogClient(options("primaryKey"), options("secondaryKey"))

  val logType : String = options.getOrElse("logType", "StateChange")

  override def init(): Unit = {
    logger.debug("io.smartdatalake.util.log.StateChangeLogger init done, logType: " + logType)
  }

  case class StateLogEventContext(thread: String, notification_time: String, executionId: String, phase: String, actionId: String, state: String, message: String)
  case class TargetObjectMetadata(name: String, layer: String, subjectArea: String, description: String)
  case class Result(targetObjectMetadata: TargetObjectMetadata, records_written: Long, stageDuration: String)
  case class StateLogEvent(context: StateLogEventContext, result: Result)

  private val gson = new Gson

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext): Unit = {

    val logEvents = extractLogEvents(state, context)
    val jsonEvents = logEvents.map{le:StateLogEvent => gson.toJson(le)}.mkString(",")

    logger.debug("logType " + logType+ " sending: " + jsonEvents)
    azureLogClient.send("[ " + jsonEvents + " ]", logType)
    logger.debug("sending completed")
  }

  def extractLogEvents(state: ActionDAGRunState, context: ActionPipelineContext): Seq[StateLogEvent] = {
    assert(state.actionsState.nonEmpty)

    val notification_time = LocalDateTime.now

    state.actionsState.map(aState => {
      val log_context = StateLogEventContext(Thread.currentThread().getName,
        notification_time.toString,
        executionId = aState._2.executionId.toString,
        phase = context.phase.toString,
        actionId = aState._1.toString,
        state = aState._2.state.toString,
        message = aState._2.msg.getOrElse(" no message"))

      val optResults = {   // we generate at least one log entry, and one log entry per result
        if (aState._2.results.nonEmpty) aState._2.results.map({result : ResultRuntimeInfo => Some(result)})
        else Seq(None)
      }
      optResults.map(optResult => {
        val result = optResult.map(
          {result : ResultRuntimeInfo =>

            val to_metadata = {
              val metadata = context.instanceRegistry.get[DataObject](result.subFeed.dataObjectId).metadata
              def extractString(attribute: DataObjectMetadata => Option[String]) = metadata.map(attribute(_).getOrElse("")).getOrElse("")

              TargetObjectMetadata(name = extractString(_.name),
                layer = extractString(_.layer),
                subjectArea = extractString(_.subjectArea),
                description = extractString(_.description))
            }

            Result(to_metadata, result.mainMetrics.getOrElse("records_written", null).asInstanceOf[Long], result.mainMetrics.getOrElse("stageDuration",null).toString)
          })

        StateLogEvent(log_context, result.orNull)
      })
    }).toSeq.flatten
  }
}
