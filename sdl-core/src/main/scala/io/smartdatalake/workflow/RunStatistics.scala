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

import io.smartdatalake.workflow.action.RuntimeEventState
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState

/**
 * Result statistics of an SDLB run, e.g. the number of Actions per [[RuntimeEventState]].
 *
 * @param currentAttempt Action count per state of the attempt just executed.
 * @param previousAttempts Action count per state of the Actions which already completed in previous attempts of the
 *                         same run and were therefore not executed again. Note that this only contains SUCCEEDED and
 *                         SKIPPED Actions: an Action which failed in a previous attempt is executed again and
 *                         accounted for in `currentAttempt`.
 */
case class RunStatistics(currentAttempt: Map[RuntimeEventState, Int], previousAttempts: Map[RuntimeEventState, Int] = Map()) {

  /**
   * Format as one line for logging, e.g. `SKIPPED=1; previous attempts: SUCCEEDED=5 SKIPPED=2`.
   * The previous attempts part is omitted if there were none.
   */
  def toLogString: String = {
    val currentStr = RunStatistics.format(currentAttempt)
    if (previousAttempts.isEmpty) currentStr
    else s"$currentStr; previous attempts: ${RunStatistics.format(previousAttempts)}"
  }
}

object RunStatistics {

  val empty: RunStatistics = RunStatistics(Map())

  /**
   * Count Actions per state, e.g. to summarize an attempt.
   */
  def countStates(states: Iterable[RuntimeEventState]): Map[RuntimeEventState, Int] =
    states.groupBy(identity).view.mapValues(_.size).toMap

  private def format(stats: Map[RuntimeEventState, Int]): String =
    stats.toSeq.sortBy(_._1).map(x => x._1.toString + "=" + x._2.toString).mkString(" ")
}
