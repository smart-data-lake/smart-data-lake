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
package io.smartdatalake.workflow.action.executionMode

import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.DataFrameActionImpl
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Extension point for DataFrame-based streaming execution modes (e.g. SparkStreamingMode).
 * Implementations live in engine-specific modules (sdl-spark) and override the streaming
 * lifecycle hooks that DataFrameActionImpl calls via this generic interface.
 */
trait DataFrameStreamingExecutionMode extends ExecutionMode {
  override def isStreamingMode: Boolean = true

  /**
   * Enrich a SubFeed with a streaming DataFrame for the given input DataObject.
   * Called from DataFrameActionImpl.enrichSubFeedDataFrame when the execution mode is a streaming mode.
   *
   * @param input           input DataObject (must support streaming reads)
   * @param subFeed         the SubFeed to enrich
   * @param phase           current execution phase
   * @param refreshDataFrame true if a fresh streaming DataFrame must be obtained from the DataObject
   * @return enriched SubFeed with streaming DataFrame
   */
  def enrichSubFeedForStreamingInput(
    input: DataObject with CanCreateDataFrame,
    subFeed: DataFrameSubFeed,
    phase: ExecutionPhase,
    refreshDataFrame: Boolean
  )(implicit context: ActionPipelineContext): DataFrameSubFeed

  /**
   * Write a SubFeed to an output DataObject using streaming semantics.
   * Called from DataFrameActionImpl.writeSubFeed when the execution mode is a streaming mode.
   *
   * @param action    the enclosing action (used for metrics/listener registration)
   * @param subFeed   SubFeed carrying the streaming DataFrame to write
   * @param output    target DataObject (must support streaming writes)
   * @param queryName unique name for the streaming query
   * @return resulting SubFeed with metrics
   */
  def writeSubFeedStreaming(
    action: DataFrameActionImpl,
    subFeed: DataFrameSubFeed,
    output: DataObject with CanWriteDataFrame,
    queryName: String
  )(implicit context: ActionPipelineContext): DataFrameSubFeed
}
