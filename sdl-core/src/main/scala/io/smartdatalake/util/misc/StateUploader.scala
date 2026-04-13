/*
 * sdl-core - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.util.misc

import io.smartdatalake.app.{BackendClient, StateListener}
import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.LogUtil.getExceptionSummary
import io.smartdatalake.workflow.action.ExecutionId
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext, HadoopFileActionDAGRunStateStore}
import org.apache.hadoop.fs.FileSystem

import scala.collection.mutable

/**
 * Upload final state to given baseUrl. This is mainly used to upload state to the backend of the UI.
 */
class StateUploader(backend: BackendClient, stagePath: Option[String], processUpdates: Boolean) extends StateListener with SmartDataLakeLogger {

  private[smartdatalake] var stageStateStore: Option[HadoopFileActionDAGRunStateStore] = None
  private val uploadedExecutionIds = mutable.Set[ExecutionId]()

  logger.info(s"instantiated: stagePath=$stagePath processUpdates=$processUpdates")

  override def prepare(context: ActionPipelineContext): Unit = {
    assert(context.appConfig.applicationName.nonEmpty, "Application name must be set on command line (--name) for using StateUploader.")
    // reset state
    uploadedExecutionIds.clear()
    // retry failed uploads from previous runs
    stagePath.foreach { path =>
      stageStateStore = Some(new HadoopFileActionDAGRunStateStore(path, context.application, context.hadoopConf))
      val stagedStates = stageStateStore.get.getFiles()
      if (stagedStates.nonEmpty) {
        logger.info(s"Retry uploading ${stagedStates.size} failed uploads from previous runs")
        implicit val filesystem: FileSystem = stageStateStore.get.filesystem
        try { // stop on first upload error
          stagedStates.foreach { file =>
            val body = HdfsUtil.readHadoopFile(file.path)
            backend.writeState(body)
            filesystem.delete(file.path, false)
          }
        } catch {
          case ex: Exception => logger.error(s"Retry uploading failed uploads from stagePath $path failed again. Will retry next time again. ${getExceptionSummary(ex)}")
        }
      }
    }
  }

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[SdlConfigObject.ActionId]): Unit = {
    val isFirst = !uploadedExecutionIds.contains(context.executionId)
    if (isFirst || state.isFinal) {
      // if first notification for executionId or final notification, upload full state to baseUrl or save to stagePath on failure
      uploadedExecutionIds.add(context.executionId)
      logger.info(s"Uploading ${if (isFirst) "first" else "final"} state for executionId=${context.executionId}")
      try {
        backend.writeState(state.toJson)
      } catch {
        case ex: Exception =>
          stageStateStore match {
            case Some(store) =>
              logger.warn(s"Failed uploading final state for executionId=${context.executionId}. Saved it to stagePath for retrying with next run. ${getExceptionSummary(ex)}")
              store.saveStateToFile(state)
            case None =>
              logger.error(s"Failed uploading final state for executionId=${context.executionId}. To avoid failing SDLB job on state upload configure global.uiBackend.stagePath. ${getExceptionSummary(ex)}")
              throw ex
          }
      }
    } else {
      // if intermediate notification, upload only changed action info to baseUrl
      if (processUpdates && changedActionId.isDefined) {
        // upload changed action info
        val body = ActionDAGRunState.toJson(state.actionsState(changedActionId.get))
        logger.debug(s"Uploading state update for ${changedActionId.get} executionId=${context.executionId}")
        try {
          backend.updateState(body, context.appConfig.applicationName.get, context.executionId, changedActionId.get)
        } catch {
          // just warn if update fails
          case ex: Exception => logger.warn(s"Failed uploading state update for $changedActionId. ${getExceptionSummary(ex)}")
        }
      }
    }
  }
}