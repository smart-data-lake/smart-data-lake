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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.CanReceiveParameterNotification
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, ParameterSubFeed, SubFeedConverter}

/**
 * Implementation of logic needed for Script Actions
 */
abstract class ScriptActionImpl extends ActionSubFeedsImpl[ParameterSubFeed] {

  override def inputs: Seq[DataObject]
  override def outputs: Seq[DataObject with CanReceiveParameterNotification]

  override val executionMode: Option[ExecutionMode] = None // no use for execution mode with scripts so far
  override def metricsFailCondition: Option[String] = None // no metrics for script execution so far

  override def subFeedConverter: SubFeedConverter[ParameterSubFeed] = ParameterSubFeed

  /**
   * To be implemented by sub-classes
   */
  protected def execScript(inputSubFeeds: Seq[ParameterSubFeed], outputSubFeeds: Seq[ParameterSubFeed])(implicit context: ActionPipelineContext): Seq[ParameterSubFeed]

  override protected def transform(inputSubFeeds: Seq[ParameterSubFeed], outputSubFeeds: Seq[ParameterSubFeed])(implicit context: ActionPipelineContext): Seq[ParameterSubFeed] = {
    // execute scripts in exec phase
    if (context.isExecPhase) {
      execScript(inputSubFeeds, outputSubFeeds)
    } else outputSubFeeds
  }

  override def writeSubFeed(subFeed: ParameterSubFeed, isRecursive: Boolean)(implicit context: ActionPipelineContext): ParameterSubFeed = {
    val output = outputs.find(_.id == subFeed.dataObjectId).getOrElse(throw new IllegalStateException(s"($id) output for subFeed ${subFeed.dataObjectId} not found"))
    output.parameterNotification(subFeed.parameters.getOrElse(Map()))
    subFeed
  }
}