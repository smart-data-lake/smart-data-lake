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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.app.ModulePlugin
import io.smartdatalake.util.hdfs.UCFileSystemFactory

class DeltaLakeModulePlugin extends ModulePlugin {

  /**
   * Additional spark properties to be added when creating SparkSession.
   */
  override def additionalSparkProperties(): Map[String, String] = {
    // register DeltaLake extension and catalog if not on Databricks
    if (!UCFileSystemFactory.isDatabricksEnv) {
      Map(
        // DeltaLake Spark SQL extensions
        "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
        // Default catalog implementation supporting DeltaLake
        "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
    } else Map()
  }

}
