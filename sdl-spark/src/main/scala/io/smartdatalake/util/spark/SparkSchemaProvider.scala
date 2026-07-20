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
package io.smartdatalake.util.spark

import io.smartdatalake.util.misc.{SchemaProvider, SchemaProviderType}
import io.smartdatalake.workflow.dataframe.GenericSchema

/**
 * [[SchemaProvider]] implementation for classic Spark, delegating to [[SparkSchemaUtil]].
 * Supports all [[SchemaProviderType]]s.
 * Discovered on the classpath by [[io.smartdatalake.definitions.Environment.schemaProviders]].
 */
object SparkSchemaProvider extends SchemaProvider {

  override def supports(schemaConfig: String): Boolean = SchemaProviderType.parse(schemaConfig).isDefined

  override def readSchemaFromConfigValue(schemaConfig: String, lazyFileReading: Boolean): GenericSchema =
    SparkSchemaUtil.readSchemaFromConfigValue(schemaConfig, lazyFileReading)
}
