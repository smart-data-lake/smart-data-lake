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
package io.smartdatalake.workflow

import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.{Action, RuntimeData}

import scala.collection.mutable

/**
 * Keeps the runtime state (events & metrics) of all Actions of an SDLB execution, indexed by [[ActionId]].
 *
 * The registry is held by [[ActionPipelineContext]]. As `copy` of a case class is shallow, every derived
 * context - the per phase copies, the per Action copy created by `withAction`, and the copy created by
 * `incrementRunId` for the next streaming iteration - shares the same registry instance. Runtime information
 * written by an Action is therefore visible to every Action executed afterwards, without having to merge
 * anything for Actions with multiple predecessors.
 *
 * Note that this state used to live on the [[Action]] instances themselves. Actions are configuration objects
 * held by the InstanceRegistry for the lifetime of the JVM, so per run state on them had to be cleaned up
 * explicitly. Keeping it here scopes it to the execution instead.
 */
private[smartdatalake] class ActionsRuntimeRegistry extends SmartDataLakeLogger {

  private val runtimeData = mutable.Map[ActionId, RuntimeData]()

  /**
   * Get the [[RuntimeData]] of an Action, creating it on first access.
   */
  def apply(action: Action): RuntimeData = synchronized {
    runtimeData.getOrElseUpdate(action.id, action.createRuntimeData)
  }

  /**
   * Get the [[RuntimeData]] of an Action if it already exists.
   */
  def get(actionId: ActionId): Option[RuntimeData] = synchronized {
    runtimeData.get(actionId)
  }

  /**
   * Drop all runtime state of an Action, so that a subsequent execution starts from scratch.
   */
  def reset(actionId: ActionId): Unit = synchronized {
    runtimeData.remove(actionId)
  }
}
