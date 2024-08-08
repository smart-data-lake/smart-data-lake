/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.testutils.TestUtil
import org.apache.spark.sql.SparkSession

object DeltaLakeTestUtils {

  // set additional spark options for delta lake
  private val additionalSparkProperties = new DeltaLakeModulePlugin().additionalSparkProperties() +
    ("spark.databricks.delta.snapshotPartitions" -> "2") // improve performance for testing with small data sets

  private[smartdatalake] lazy val deltaDb = {
    val dbname = "delta"
    session.sql(s"create database if not exists $dbname;")
    dbname
  }

  def session : SparkSession = additionalSparkProperties
    .foldLeft(TestUtil.sparkSessionBuilder()) {
      case (builder, config) => builder.config(config._1, config._2)
    }.getOrCreate()

}
