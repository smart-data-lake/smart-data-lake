package io.smartdatalake.jetty

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}

class CustomListener extends StateListener {
  var isOver: Boolean = false

  //Fuer websocket: wenn changedAction!=None ist den Inhalt dieser Action pushen und in actionsState suchen
  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[ActionId]): Unit = {

    if (state.isFinal) {
      isOver = true
    } else {
      isOver = false
    }
  }
}
