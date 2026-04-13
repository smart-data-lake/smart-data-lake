/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.webservice.SttpWebserviceClient
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext, HadoopFileActionDAGRunStateStore}

/**
 * Write final state to given hadoop path to be used as notification for succeeded runs, e.g. by an
 * Azure Function. Needs 'path' as option.
 */
class FinalStateWriter(options: Map[String, StringOrSecret]) extends StateListener with SmartDataLakeLogger {

  private val path = options.getOrElse("path", throw new IllegalArgumentException("Option 'path' not defined")).resolve()
  private var stateStore: Option[HadoopFileActionDAGRunStateStore] = None

  logger.info(s"instantiated: path=$path")

  override def init(context: ActionPipelineContext): Unit = {
    stateStore = Some(new HadoopFileActionDAGRunStateStore(path, context.application, context.hadoopConf))
    // check connection
    stateStore.get.getLatestRunId
  }

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[SdlConfigObject.ActionId]): Unit =
    // write state file on final notification
    if (state.isFinal) {
      stateStore.get.saveState(state)
    }
}
