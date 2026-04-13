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

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.workflow.{ActionDAGRunState, ActionDAGRunStateStore, StateId}

import scala.collection.mutable

/**
 * In-memory implementation of ActionDAGRunStateStore for testing purposes.
 * It stores the states in a mutable buffer and allows saving and recovering states based on runId and attemptId.
 *
 * Note: It can store only states for one pipeline. Every testcase needs it own instance of MemoryDagRunStateStore.
 */
case class MemoryDagRunStateStore() extends ActionDAGRunStateStore[MemoryStateId] {

  private val states: mutable.Buffer[ActionDAGRunState] = mutable.Buffer()
  private var app: Option[SmartDataLakeBuilderConfig] = None

  override def saveState(state: ActionDAGRunState): Unit = {
    assert(app.isEmpty || app.contains(state.appConfig), "State from different app config cannot be saved in the same memory store.")
    app = Some(state.appConfig)
    states.append(state)
  }

  override def recoverRunState(stateId: MemoryStateId): ActionDAGRunState = {
    states.find(s => s.runId == stateId.runId && s.attemptId == stateId.attemptId)
      .getOrElse(throw new IllegalStateException(s"State with runId=${stateId.runId} and attemptId=${stateId.attemptId} not found in memory store."))
  }

  override def getLatestStateId(runId: Option[Int]): Option[MemoryStateId] = {
    val filteredStates = runId match {
      case Some(id) => states.filter(_.runId == id)
      case None => states
    }
    // assuming that states are saved in order of attemptId, the last one with the highest attemptId is the latest state for the run
    filteredStates.lastOption.map(s => MemoryStateId(s.runId, s.attemptId))
  }

  override def getLatestRunId: Option[Int] = {
    states.lastOption.map(_.runId)
  }
}

case class MemoryStateId(runId: Int, attemptId: Int) extends StateId
