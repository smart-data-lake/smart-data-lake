/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.script

import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.config.{ConfigHolder, ParsableFromConfig}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext

import scala.collection.mutable

/**
 * Interface to implement script execution
 */
trait ScriptDef {
  def name: String
  def description: Option[String]

  /**
   * Function to be implemented to execute command and return stdout as String.
   * @param configObjectId id of the action or dataobject which executes this transformation. This is mainly used to prefix error messages.
   * @param partitionValues partition values to transform
   * @param parameters key-value parameters
   * @param errors an optional Buffer to collect stdErr messages
   * @return standard output of script as String
   */
  def execStdOutString(configObjectId: ConfigObjectId, partitionValues: Seq[PartitionValues], parameters: Map[String,String], errors: mutable.Buffer[String] = mutable.Buffer())(implicit context: ActionPipelineContext): String

  /**
   * Function to be implemented to execute command and return stdout as Stream of lines.
   * @param configObjectId id of the action which executes this transformation. This is mainly used to prefix error messages.
   * @param partitionValues partition values to transform
   * @param parameters key-value parameters
   * @param errors an optional Buffer to collect stdErr messages
   * @return standard output of script as Stream of lines
   */
  def execStdOutStream(configObjectId: ConfigObjectId, partitionValues: Seq[PartitionValues], parameters: Map[String,String], errors: mutable.Buffer[String] = mutable.Buffer())(implicit context: ActionPipelineContext): Stream[String]
}

trait ParsableScriptDef extends ScriptDef with ParsableFromConfig[ParsableScriptDef] with ConfigHolder