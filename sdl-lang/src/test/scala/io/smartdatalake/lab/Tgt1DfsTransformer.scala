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
package io.smartdatalake.lab

import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}

class Tgt1DfsTransformer extends CustomDfsTransformer {
  def transform(session: SparkSession, increment1: Int, dfSrc1: DataFrame, dfSrc2: DataFrame): Map[String, DataFrame] = {
    import session.implicits._
    Map(
      "tgt1" -> dfSrc1.withColumn("rating", $"rating" + increment1)
    )
  }
}