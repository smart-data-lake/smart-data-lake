package io.smartdatalake.util.misc

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext, HadoopFileActionDAGRunStateStore}

/**
 * Write final state to given hadoop path to be used as notification for succeeded runs, e.g. by an Azure Function.
 * Needs 'path' as option.
 */
class FinalStateWriter(options: Map[String,String]) extends StateListener {
  var stateStore: Option[HadoopFileActionDAGRunStateStore] = None
  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[SdlConfigObject.ActionId]): Unit = {
    // initialize state store
    if (stateStore.isEmpty) {
      val path = options.get("path").getOrElse(throw new IllegalArgumentException("Option 'path' not defined"))
      stateStore = Some(new HadoopFileActionDAGRunStateStore(path, context.application, context.hadoopConf))
      // check connection
      stateStore.get.getLatestRunId
    }
    // write state file on final notification
    if (state.isFinal) {
      stateStore.get.saveState(state)
    }
  }
}
