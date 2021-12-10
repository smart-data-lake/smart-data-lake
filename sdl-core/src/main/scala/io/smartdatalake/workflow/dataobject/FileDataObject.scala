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

import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

private[smartdatalake] trait FileDataObject extends DataObject with CanHandlePartitions {

  /**
   * The root path of the files that are handled by this DataObject.
   */
  protected def path: String

  /**
    * default separator for paths
    */
  protected val separator = Path.SEPARATOR_CHAR // default is to use hadoop separator. DataObjects can override this if not suitable.

  override def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.prepare
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
  }

  /**
   * Make a given path relative to this DataObjects base path
   */
  def relativizePath(filePath: String)(implicit session: SparkSession): String
}
