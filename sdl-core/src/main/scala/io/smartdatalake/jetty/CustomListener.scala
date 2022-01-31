package io.smartdatalake.jetty

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}

class CustomListener extends StateListener {

  var stateVar: ActionDAGRunState =_
  var contextVar: ActionPipelineContext = _

  //TODO When implementing websocket: if changedAction!=None then look for action with changedActionId  in actionsState and push it into socket
  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[ActionId]): Unit = {

    stateVar = state
    contextVar = context
  }
}
