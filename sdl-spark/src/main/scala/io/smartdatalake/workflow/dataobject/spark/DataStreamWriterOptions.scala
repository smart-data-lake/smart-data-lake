/*
 * Smart Data Lake - Build your data lake the smart way.
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

package io.smartdatalake.workflow.dataobject.spark

import io.smartdatalake.util.misc.ProductUtil
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * class containing options to be passed to DataStreamWriter
 * @param trigger
 * @param options
 * @param checkpointLocation
 * @param queryName
 * @param outputMode
 */
case class DataStreamWriterOptions(
    trigger: Trigger = Trigger.AvailableNow(),
    options: Map[String, String] = Map.empty[String, String],
    checkpointLocation: String,
    queryName: String,
    outputMode: OutputMode = OutputMode.Append
) {
  def toDebugString: String = ProductUtil.toDebugString(obj = this)
}
