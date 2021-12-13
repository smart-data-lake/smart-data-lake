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
package io.smartdatalake.workflow.dataobject

import com.snowflake.snowpark.DataFrame
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext

private[smartdatalake] trait CanWriteSnowparkDataFrame {

  /**
   * Called during init phase for checks and initialization.
   * If possible dont change the system until execution phase.
   */
  def init(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = Unit

  /**
   * Write DataFrame to DataObject
   * @param df the DataFrame to write
   * @param partitionValues partition values included in DataFrames data
   * @param isRecursiveInput if DataFrame needs this DataObject as input - special treatment might be needed in this case.
   */
  def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit
}
