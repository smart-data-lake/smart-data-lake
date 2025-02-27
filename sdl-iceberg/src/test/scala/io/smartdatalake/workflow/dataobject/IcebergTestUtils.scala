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

object IcebergTestUtils {

  // set additional spark options for Iceberg
  private val additionalSparkProperties = new IcebergModulePlugin().additionalSparkProperties() ++ Map (
    // Additional Iceberg only catalog, using volatile in-memory db for testing
    "spark.sql.catalog.iceberg1" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg1.catalog-impl" -> "org.apache.iceberg.jdbc.JdbcCatalog",
    "spark.sql.catalog.iceberg1.warehouse" -> "target/iceberg1warehouse",
    "spark.sql.catalog.iceberg1.uri" -> "jdbc:hsqldb:mem:catalog",
    "spark.sql.catalog.iceberg1.jdbc.schema-version" -> "V1",
    // Additional Iceberg Hadoop catalog, for testing compatibility
    "spark.sql.catalog.iceberg_hadoop" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg_hadoop.type" -> "hadoop",
    "spark.sql.catalog.iceberg_hadoop.warehouse" -> "target/icebergHadoopWarehouse",
  )
  def session : SparkSession = additionalSparkProperties
    .foldLeft(TestUtil.sparkSessionBuilder()) {
      case (builder, config) => builder.config(config._1, config._2)
    }.getOrCreate()
}
