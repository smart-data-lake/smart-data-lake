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
package io.smartdatalake.testutils

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.action.RuntimeData
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}

/**
 * Mix into a test class to get hold of the [[ActionPipelineContext]] of the last SDLB run.
 *
 * The runtime information of a run (events & metrics) is kept in the ActionsRuntimeRegistry of the
 * ActionPipelineContext which SmartDataLakeBuilder creates for that run, and not on the Action instances.
 * A test which wants to inspect events or metrics after `sdlb.run(...)` returned therefore needs the context of
 * that run. This trait captures it by registering a [[StateListener]], which is notified with the context on
 * every state change of the run.
 *
 * Note that a test has to set additional StateListeners through [[setAdditionalStateListeners]] instead of
 * assigning `Environment._additionalStateListeners` directly, as the latter removes the capturing listener.
 *
 * Example:
 * {{{
 *   class MyTest extends AnyFunSuite with CapturesRunContext {
 *     test("...") {
 *       sdlb.run(sdlConfig)
 *       val runtimeInfo = action1.getRuntimeInfo()(lastRunContext)
 *     }
 *   }
 * }}}
 */
trait CaptureRunContext {

  private val runContextListener = new RunContextCaptureListener

  // register on construction of the test class, so it is active for every run of the test class
  Environment._additionalStateListeners = Environment._additionalStateListeners
    .filterNot(_.eq(runContextListener)) :+ runContextListener

  /**
   * Set additional StateListeners for the following runs, keeping the capturing listener registered.
   * Call without arguments to remove all additional StateListeners again.
   */
  def setAdditionalStateListeners(listeners: StateListener*): Unit = {
    Environment._additionalStateListeners = listeners :+ runContextListener
  }

  /**
   * Forget the context captured so far, e.g. to assert that a following run really captured a new one.
   */
  def resetRunContext(): Unit = runContextListener.reset()

  /**
   * The [[ActionPipelineContext]] of the last run.
   * Note that in streaming mode the context is copied per iteration, but all copies share the same registries.
   */
  def lastRunContext: ActionPipelineContext = lastRunContextOpt.getOrElse(
    throw new IllegalStateException("No ActionPipelineContext captured yet." +
      " Make sure a run was executed and that the SDLB configuration of the test does not replace" +
      " Environment._additionalStateListeners, see CapturesRunContext.setAdditionalStateListeners.")
  )

  def lastRunContextOpt: Option[ActionPipelineContext] = runContextListener.context

  /**
   * The [[RuntimeData]] collected for an Action in the last run, if it was executed.
   */
  def lastRunRuntimeData(actionId: ActionId): Option[RuntimeData] = lastRunContext.runtimeRegistry.get(actionId)
}

/**
 * [[StateListener]] which remembers the [[ActionPipelineContext]] it was last notified with.
 */
private[testutils] class RunContextCaptureListener extends StateListener {

  // notifyState is called from the threads executing the DAG
  @volatile private var lastContext: Option[ActionPipelineContext] = None

  def context: Option[ActionPipelineContext] = lastContext

  def reset(): Unit = lastContext = None

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[ActionId]): Unit = {
    lastContext = Some(context)
  }
}
