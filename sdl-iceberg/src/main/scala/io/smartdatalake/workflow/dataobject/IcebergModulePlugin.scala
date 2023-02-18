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
import org.apache.hadoop.fs.FileSystem

class IcebergModulePlugin extends ModulePlugin {

  /**
   * Additional spark properties to be added when creating SparkSession.
   */
  override def additionalSparkProperties(): Map[String, String] = Map(
    // Icberg Spark SQL extensions
    "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    // Default Spark catalog extension to make it support Iceberg tables. Disabled because it conflicts with delta-lake. You cant configure both for the default Spark catalog.
    //"spark.sql.catalog.spark_catalog.type" -> "hive",
    //"spark.sql.catalog.spark_catalog" -> "org.apache.iceberg.spark.SparkSessionCatalog",
  )

}