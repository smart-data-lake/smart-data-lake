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
package io.smartdatalake.workflow.dataobject.generic

import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import org.apache.hadoop.fs.Path

import scala.reflect.runtime.universe.Type

trait CanWriteDataFrame {

  // additional streaming options which can be overridden by the DataObject, e.g. Kafka topic to write to
  def streamingOptions: Map[String, String] = Map()

  /**
   * Called during init phase for checks and initialization.
   * If possible do not change the system until execution phase.
   */
  def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = ()

  /**
   * Write a DataFrame to the DataObject
   * @return collected metrics
   */
  def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): MetricsMap

  /**
   * Declare supported Language for writing DataFrame.
   */
  private[smartdatalake] def writeSubFeedSupportedTypes: Seq[Type]

  /**
   * Write DataFrame to specific Path with properties of this DataObject.
   * This is needed for compacting partitions by housekeeping.
   * Note: this is optional to implement.
   */
  private[smartdatalake] def writeDataFrameToPath(df: GenericDataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): Unit = throw new RuntimeException("writeDataFrameToPath not implemented")

}
