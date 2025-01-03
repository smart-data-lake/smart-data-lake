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
import io.smartdatalake.workflow.dataobject.FileRef

/**
 * A FileSubFeed is used to transport references to files between Actions.
 *
 * @param fileRefs path to files to be processed
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isDAGStart true if this subfeed is a start node of the dag
 * @param isSkipped true if this subfeed is the result of a skipped action
 * @param fileRefMapping store mapping of input to output file references. This is also used for post processing (e.g. delete after read).
 */
case class FileSubFeed(fileRefs: Option[Seq[FileRef]],
                       override val dataObjectId: DataObjectId,
                       override val partitionValues: Seq[PartitionValues],
                       override val isDAGStart: Boolean = false,
                       override val isSkipped: Boolean = false,
                       fileRefMapping: Option[Seq[FileRefMapping]] = None,
                       override val metrics: Option[MetricsMap] = None
)
  extends SubFeed {

  override def breakLineage(implicit context: ActionPipelineContext): FileSubFeed = {
    this.copy(fileRefs = None, fileRefMapping = None)
  }

  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): FileSubFeed = {
    if (breakLineageOnChange && partitionValues.nonEmpty) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearPartitionValues")
      this.copy(partitionValues = Seq()).breakLineage
    } else this.copy(partitionValues = Seq())
  }

  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): FileSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    if (breakLineageOnChange && partitionValues != updatedPartitionValues) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from updatePartitionValues")
      this.copy(partitionValues = updatedPartitionValues).breakLineage
    } else this.copy(partitionValues = updatedPartitionValues)
  }

  def checkPartitionValuesColsExisting(partitions: Set[String]): Boolean = {
    partitionValues.forall(pvs => partitions.diff(pvs.keys).isEmpty)
  }

  override def toOutput(dataObjectId: DataObjectId): FileSubFeed = {
    this.copy(fileRefs = None, fileRefMapping = None, isDAGStart = false, isSkipped = false, dataObjectId = dataObjectId, metrics = None)
  }

  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case fileSubFeed: FileSubFeed if this.fileRefs.isDefined && fileSubFeed.fileRefs.isDefined =>
      this.copy(fileRefs = this.fileRefs.map(_ ++ fileSubFeed.fileRefs.get)
        , partitionValues = unionPartitionValues(fileSubFeed.partitionValues)
        , isDAGStart = this.isDAGStart || fileSubFeed.isDAGStart, isSkipped = this.isSkipped && fileSubFeed.isSkipped)
    case fileSubFeed: FileSubFeed =>
      this.copy(fileRefs = None, partitionValues = unionPartitionValues(fileSubFeed.partitionValues)
        , isDAGStart = this.isDAGStart || fileSubFeed.isDAGStart, isSkipped = this.isSkipped && fileSubFeed.isSkipped)
    case x => this.copy(fileRefs = None, partitionValues = unionPartitionValues(x.partitionValues), isDAGStart = this.isDAGStart || x.isDAGStart, isSkipped = this.isSkipped && x.isSkipped)
  }

  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): FileSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false, fileRefs = result.fileRefs, fileRefMapping = None)
  }
  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): FileSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false, fileRefs = None, fileRefMapping = None)
  }
}
object FileSubFeed extends SubFeedConverter[FileSubFeed] {
  /**
   * This method is used to pass an output SubFeed as input FileSubFeed to the next Action. SubFeed type might need conversion.
   */
  override def fromSubFeed( subFeed: SubFeed )(implicit context: ActionPipelineContext): FileSubFeed = {
    subFeed match {
      case fileSubFeed: FileSubFeed => fileSubFeed
      case _ => FileSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
}

/**
 * Src/Tgt tuple representing the mapping of a file reference
 *
 * @param multiTgt: If multi-target is true, the src FileRef might produce multiple output-streams, which are only known at execution time (e.g. Webservice with paging).
 */
case class FileRefMapping(src: FileRef, tgt: FileRef)