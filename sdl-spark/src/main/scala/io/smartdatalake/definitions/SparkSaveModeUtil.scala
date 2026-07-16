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
package io.smartdatalake.definitions

import io.smartdatalake.util.hdfs.PartitionValues
import org.apache.spark.sql.{DataFrameWriterV2, Row}

private[smartdatalake] object SparkSaveModeUtil {

  def execV2(saveMode: SDLSaveMode.Value, writer: DataFrameWriterV2[Row], partitionValues: Seq[PartitionValues], partitionOverwriteModeDynamic: Boolean = false): Unit = {
    import org.apache.spark.sql.functions.expr
    saveMode match {
      case SDLSaveMode.Append => writer.append()
      case SDLSaveMode.Overwrite | SDLSaveMode.OverwriteOptimized if partitionValues.nonEmpty =>
        val filterSql = partitionValues.map(_.getFilterExprSql).mkString("(", ") OR (", ")")
        writer.overwrite(expr(filterSql))
      case SDLSaveMode.Overwrite | SDLSaveMode.OverwriteOptimized if partitionValues.isEmpty && partitionOverwriteModeDynamic => writer.overwritePartitions()
      case SDLSaveMode.Overwrite | SDLSaveMode.OverwriteOptimized if partitionValues.isEmpty => writer.replace()
    }
  }
}
