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

import io.smartdatalake.app.{SDLPlugin, StateListener}
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}

/**
 * StateListener for testing, registered in test/resources/application.conf.
 */
class TestStateListener(options: Map[String, StringOrSecret]) extends StateListener {
  var firstState: Option[ActionDAGRunState] = None
  var finalState: Option[ActionDAGRunState] = None

  override def notifyState(state: ActionDAGRunState,
                           context: ActionPipelineContext,
                           changedActionId: Option[ActionId]): Unit = {
    if (TestStateListener.context.isEmpty) TestStateListener.context = Some(context)
    if (firstState.isEmpty) firstState = Some(state)
    finalState = Some(state)
  }
}

object TestStateListener {
  var context: Option[ActionPipelineContext] = None
}

/**
 * SDLPlugin for testing, remembering if its methods have been called.
 */
class TestSDLPlugin extends SDLPlugin {
  override def startup(): Unit = {
    TestSDLPlugin.startupCalled = true
  }

  override def configure(options: Map[String, StringOrSecret]): Unit = {
    TestSDLPlugin.configureCalled = true
  }

  override def shutdown(): Unit = {
    TestSDLPlugin.shutdownCalled = true
  }
}

object TestSDLPlugin {
  var startupCalled = false
  var configureCalled = false
  var shutdownCalled = false
}
