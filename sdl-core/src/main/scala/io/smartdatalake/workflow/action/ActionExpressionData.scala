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
package io.smartdatalake.workflow.action

import io.smartdatalake.workflow.ActionPipelineContext

import java.sql.Timestamp

/**
 * Runtime information of an Action executed earlier in the same run, presented for evaluation by expressions
 * in various places of the configuration, e.g. the `runtimeOptions` of a transformer or an `executionCondition`.
 *
 * @param state state of the Action, e.g. SUCCEEDED, SKIPPED, FAILED, CANCELLED or STREAMING.
 * @param partitionValues partition values of the results of the Action.
 * @param metrics metrics of the Action per output DataObject id, e.g. `metrics['tgt1']['records_written']`.
 * @param executionModeOptions options returned by the ExecutionMode of the Action.
 * @param inputIds ids of the input DataObjects of the Action.
 * @param outputIds ids of the output DataObjects of the Action.
 * @param startTstmp start of the exec phase of the Action.
 * @param endTstmp end of the exec phase of the Action.
 * @param durationMillis duration of the last successful phase of the Action in milliseconds.
 * @param runId runId of the execution which produced this information.
 * @param attemptId attemptId of the execution which produced this information. This is smaller than the attemptId of
 *                  the current run if the Action already completed in a previous attempt and was not executed again.
 */
case class ActionExpressionData(
    state: String,
    partitionValues: Seq[Map[String, String]],
    metrics: Map[String, Map[String, String]],
    executionModeOptions: Map[String, String],
    inputIds: Seq[String],
    outputIds: Seq[String],
    startTstmp: Option[Timestamp],
    endTstmp: Option[Timestamp],
    durationMillis: Option[Long],
    runId: Option[Int],
    attemptId: Option[Int]
)

object ActionExpressionData {

  /**
   * Collect runtime information of all Actions the current Action transitively depends on, indexed by ActionId.
   *
   * Only predecessors in the DAG are included, and not simply every Action which happens to have finished:
   * predecessors are guaranteed to be complete when the current Action runs, so the result does not depend on
   * the scheduling order of parallel branches.
   *
   * Note that metrics are only available in ExecutionPhase.Exec. In earlier phases the corresponding entries
   * are missing, so expressions referring to them evaluate to null.
   */
  def predecessorsFrom(context: ActionPipelineContext): Map[String, ActionExpressionData] = {
    context.predecessorActions
      .flatMap(action => from(action)(context).map(action.id.id -> _))
      .toMap
  }

  private def from(action: Action)(implicit context: ActionPipelineContext): Option[ActionExpressionData] = {
    // An Action which already completed in a previous attempt has no runtime information in the registry of this
    // attempt, as it is not executed again. Its RuntimeInfo is restored from the state file on recovery instead,
    // see ActionPipelineContext.actionsSkipped.
    action.getRuntimeInfo(Some(context.executionId))
      .orElse(context.actionsSkipped.get(action.id))
      .map { runtimeInfo =>
        ActionExpressionData(
          state = runtimeInfo.state.toString,
          partitionValues = runtimeInfo.results.flatMap(_.partitionValues).distinct.map(_.getMapString),
          metrics = runtimeInfo.results
            .map(subFeed => subFeed.dataObjectId.id -> subFeed.metrics.getOrElse(Map()).view.mapValues(_.toString).toMap)
            .toMap,
          executionModeOptions = runtimeInfo.results.map(_.executionModeResultOptions).reduceOption(_ ++ _).getOrElse(Map()),
          inputIds = runtimeInfo.inputIds.map(_.id),
          outputIds = runtimeInfo.outputIds.map(_.id),
          startTstmp = runtimeInfo.startTstmp.map(Timestamp.valueOf),
          endTstmp = runtimeInfo.endTstmp.map(Timestamp.valueOf),
          durationMillis = runtimeInfo.duration.map(_.toMillis),
          runId = sdlExecutionId(runtimeInfo).map(_.runId),
          attemptId = sdlExecutionId(runtimeInfo).map(_.attemptId)
        )
      }
  }

  private def sdlExecutionId(runtimeInfo: RuntimeInfo): Option[SDLExecutionId] = runtimeInfo.executionId match {
    case id: SDLExecutionId => Some(id)
    case _ => None // asynchronous executions, e.g. SparkStreamingExecutionId, have no runId/attemptId
  }
}
