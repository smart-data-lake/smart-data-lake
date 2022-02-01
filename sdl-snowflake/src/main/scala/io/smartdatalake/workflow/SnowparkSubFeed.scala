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

import com.snowflake.snowpark.DataFrame
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.ExecutionModeResult
import io.smartdatalake.util.hdfs.PartitionValues

case class SnowparkSubFeed(@transient dataFrame: Option[DataFrame],
                           override val dataObjectId: DataObjectId,
                           override val partitionValues: Seq[PartitionValues],
                           override val isDAGStart: Boolean = false,
                           override val isSkipped: Boolean = false)
  extends SubFeed {

  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    this.copy(partitionValues = Seq())
  }

  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    this.copy(partitionValues = updatedPartitionValues)
  }

  override def clearDAGStart(): SnowparkSubFeed = {
    this.copy(isDAGStart = false)
  }

  override def clearSkipped(): SnowparkSubFeed = {
    this.copy(isSkipped = false)
  }

  override def toOutput(dataObjectId: DataObjectId): SnowparkSubFeed = {
    this.copy(dataFrame = None, isDAGStart = false, isSkipped = false, dataObjectId = dataObjectId)
  }

  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case snowparkSubFeed: SnowparkSubFeed if this.dataFrame.isDefined && snowparkSubFeed.dataFrame.isDefined =>
      this.copy(dataFrame = Some(this.dataFrame.get.unionByName(snowparkSubFeed.dataFrame.get)),
        partitionValues = unionPartitionValues(snowparkSubFeed.partitionValues),
        isDAGStart = this.isDAGStart || snowparkSubFeed.isDAGStart)
    case subFeed =>
      this.copy(dataFrame = None,
        partitionValues = unionPartitionValues(subFeed.partitionValues),
        isDAGStart = this.isDAGStart || subFeed.isDAGStart)
  }

  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false)
  }

  override def breakLineage(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    this.copy()
  }

  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false, dataFrame = None)
  }
}

object SnowparkSubFeed extends SubFeedConverter[SnowparkSubFeed] {
  override def fromSubFeed(subFeed: SubFeed)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    subFeed match {
      case snowparkSubFeed: SnowparkSubFeed => snowparkSubFeed
      case _ => SnowparkSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
}