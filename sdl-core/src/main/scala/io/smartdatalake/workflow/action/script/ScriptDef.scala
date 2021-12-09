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

import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry, ParsableFromConfig}
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Interface to implement script execution
 */
trait ScriptDef {
  def name: String
  def description: Option[String]

  /**
   * Function to be implemented to execute command
   * @param actionId id of the action which executes this transformation. This is mainly used to prefix error messages.
   * @param partitionValues partition values to transform
   * @param parameters key-value parameters
   * @return standard output of script
   */
  def execStdOut(actionId: ActionId, partitionValues: Seq[PartitionValues], parameters: Map[String,String])(implicit context: ActionPipelineContext): String
}

trait ParsableScriptDef extends ScriptDef with ParsableFromConfig[ParsableScriptDef]