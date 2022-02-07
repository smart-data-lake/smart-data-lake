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
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}


/*
 * Listens to the current contents of ActionDAGRunState and ActionPipelineContext and stores them so that
 * the status info server has access to them.
 */
class StatusInfoListener extends StateListener {

  var stateVar: ActionDAGRunState = _
  var contextVar: ActionPipelineContext = _

  //TODO When implementing websocket: if changedAction!=None then look for action with changedActionId  in actionsState and push it into socket
  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[ActionId]): Unit = {
    stateVar = state
    contextVar = context
  }
}
