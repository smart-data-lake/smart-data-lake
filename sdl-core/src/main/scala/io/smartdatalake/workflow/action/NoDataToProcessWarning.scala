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

import io.smartdatalake.util.dag.DAGHelper.NodeId
import io.smartdatalake.util.dag.{TaskSkippedDontStopWarning, TaskSkippedWarning}
import io.smartdatalake.workflow.SubFeed
import org.apache.spark.annotation.DeveloperApi

/**
 * Execution modes and DataObjects preparing DataFrames can throw this exception to indicate that there is no data to process.
 * @param results SDL might add fake results to this exception to allow further execution of DAG. When creating the exception result should be set to None.
 */
@DeveloperApi
case class NoDataToProcessWarning(actionId: NodeId, msg: String, results: Option[Seq[SubFeed]] = None) extends TaskSkippedDontStopWarning(actionId, msg, results)