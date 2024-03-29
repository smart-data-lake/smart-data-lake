package io.smartdatalake.util.misc

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.webservice.ScalaJWebserviceClient
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext, HadoopFileActionDAGRunStateStore}

/**
 * Write final state to given hadoop path to be used as notification for succeeded runs, e.g. by an Azure Function.
 * Needs 'path' as option.
 */
class FinalStateWriter(options: Map[String,StringOrSecret]) extends StateListener with SmartDataLakeLogger {

  private val path = options.getOrElse("path", throw new IllegalArgumentException("Option 'path' not defined")).resolve()
  private var stateStore: Option[HadoopFileActionDAGRunStateStore] = None

  logger.info(s"instantiated: path=$path")

  override def init(context: ActionPipelineContext): Unit = {
    stateStore = Some(new HadoopFileActionDAGRunStateStore(path, context.application, context.hadoopConf))
    // check connection
    stateStore.get.getLatestRunId
  }

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[SdlConfigObject.ActionId]): Unit = {
    // write state file on final notification
    if (state.isFinal) {
      stateStore.get.saveState(state)
    }
  }
}
