/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.executionMode.ExecutionModeResult
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/**
 * An InitSubFeed is used to initialize first Nodes of a [[DAG]].
 *
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isSkipped true if this subfeed is the result of a skipped action
 */
case class InitSubFeed(override val dataObjectId: DataObjectId,
                       override val partitionValues: Seq[PartitionValues],
                       override val isSkipped: Boolean = false,
                       override val metrics: Option[MetricsMap] = None
                      )
  extends SubFeed {
  override def isDAGStart: Boolean = true
  override def breakLineage(implicit context: ActionPipelineContext): InitSubFeed = this
  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): InitSubFeed = {
    if (breakLineageOnChange && partitionValues.nonEmpty) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearPartitionValues")
      this.copy(partitionValues = Seq()).breakLineage
    } else this.copy(partitionValues = Seq())
  }
  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): InitSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    if (breakLineageOnChange && partitionValues != updatedPartitionValues) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from updatePartitionValues")
      this.copy(partitionValues = updatedPartitionValues).breakLineage
    } else this.copy(partitionValues = updatedPartitionValues)
  }
  override def toOutput(dataObjectId: DataObjectId): FileSubFeed = throw new NotImplementedException()
  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case x => this.copy(partitionValues = unionPartitionValues(x.partitionValues), isSkipped = this.isSkipped && other.isSkipped)
  }
  def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false)
  }
  def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): SubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false)
  }
}