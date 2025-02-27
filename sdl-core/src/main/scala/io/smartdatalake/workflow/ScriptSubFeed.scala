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
import io.smartdatalake.util.misc.ScalaUtil.optionalizeMap
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.executionMode.ExecutionModeResult

/**
 * A ScriptSubFeed is used to notify DataObjects and subsequent actions about the completion of a script.
 * It allows to pass on arbitrary informations as key/values.
 *
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isDAGStart true if this subfeed is a start node of the dag
 * @param isSkipped true if this subfeed is the result of a skipped action
 * @param parameters arbitrary informations as key/value to pass on
 */
case class ScriptSubFeed(parameters: Option[Map[String,String]] = None,
                         override val dataObjectId: DataObjectId,
                         override val partitionValues: Seq[PartitionValues],
                         override val isDAGStart: Boolean = false,
                         override val isSkipped: Boolean = false,
                         override val metrics: Option[MetricsMap] = None
                        )
  extends SubFeed {
  override def breakLineage(implicit context: ActionPipelineContext): ScriptSubFeed = this
  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    this.copy(partitionValues = Seq())
  }
  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    this.copy(partitionValues = updatedPartitionValues)
  }
  override def toOutput(dataObjectId: DataObjectId): ScriptSubFeed = {
    this.copy(dataObjectId = dataObjectId, parameters = None, isDAGStart = false, isSkipped = false, metrics = None)
  }
  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case subFeed: ScriptSubFeed =>
      this.copy(
        parameters = optionalizeMap(this.parameters.getOrElse(Map()) ++ subFeed.parameters.getOrElse(Map())),
        partitionValues = unionPartitionValues(subFeed.partitionValues), isDAGStart = this.isDAGStart || subFeed.isDAGStart
      )
    case x => this.copy(parameters = None, partitionValues = unionPartitionValues(x.partitionValues), isDAGStart = this.isDAGStart || x.isDAGStart)
  }
  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false)
  }
  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false, parameters = None)
  }
}
object ScriptSubFeed extends SubFeedConverter[ScriptSubFeed] {
  /**
   * This method is used to pass an output SubFeed as input FileSubFeed to the next Action. SubFeed type might need conversion.
   */
  override def fromSubFeed( subFeed: SubFeed )(implicit context: ActionPipelineContext): ScriptSubFeed = {
    subFeed match {
      case subFeed: ScriptSubFeed => subFeed
      case _ => ScriptSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
}