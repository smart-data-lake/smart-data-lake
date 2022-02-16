/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.statusinfo

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.statusinfo.websocket.{ActionStatusUpdate, StatusInfoSocket}
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}
import org.json4s.jackson.Serialization.writePretty

import scala.collection.mutable.ListBuffer

/*
 * Pushes Incremental Updates on changes on the actionsState-map of ActionDAGRunState to all activeSockets.
 */
class IncrementalStatusInfoListener extends StateListener {

  val activeSockets: ListBuffer[StatusInfoSocket] = ListBuffer()

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[ActionId]): Unit = {

    if (changedActionId.isDefined) {
      val changedActions = state.actionsState.filter(_._1 == changedActionId.get)
      require(changedActions.size == 1, "Not exactly one changedAction! Got: " + changedActions)
      val changedAction = changedActions.head
      val changedActionJSON = ActionStatusUpdate(changedAction._1, changedAction._2, context.phase.toString)

      activeSockets.foreach(socket => socket.getRemote.sendString(writePretty(changedActionJSON)(ActionDAGRunState.formats)))
    }
  }
}
