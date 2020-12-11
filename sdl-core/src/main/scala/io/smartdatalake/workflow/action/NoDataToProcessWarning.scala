/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.workflow.DAGHelper.NodeId
import io.smartdatalake.workflow.{SubFeed, TaskSkippedDontStopWarning, TaskSkippedWarning}
import org.apache.spark.annotation.DeveloperApi

/**
 * Execution modes can throw this exception to indicate that there is no data to process, and dependent Actions should not be executed.
 */
@DeveloperApi
case class NoDataToProcessWarning(actionId: NodeId, msg: String) extends TaskSkippedWarning(actionId, msg)

/**
 * Execution modes can throw this exception to indicate that there is no data to process, and dependent Actions should be executed nevertheless.
 */
@DeveloperApi
case class NoDataToProcessDontStopWarning(actionId: NodeId, msg: String, results: Option[Seq[SubFeed]] = None) extends TaskSkippedDontStopWarning(actionId, msg, results)
