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
package io.smartdatalake.util.misc

import io.smartdatalake.workflow.dataframe.GenericSchema

/**
 * A SchemaProvider parses a schema definition given as a configuration value into a [[GenericSchema]].
 * The schema provider type is included in the configuration value as a prefix terminated by '#',
 * see [[SchemaProviderType]].
 *
 * Implementations are provided by the engine modules (e.g. sdl-spark for classic Spark, sdl-sparkconnect for
 * Spark Connect) and discovered on the classpath by reflection (see [[io.smartdatalake.definitions.Environment.schemaProviders]]),
 * similar to how [[io.smartdatalake.workflow.dataobject.generic.DataObjectEngine]] implementations and
 * [[io.smartdatalake.app.ModulePlugin]]s are discovered. A concrete implementation can also be enforced through the
 * global option `schemaProvider` (see [[io.smartdatalake.definitions.Environment.schemaProviders]]), similar to
 * [[io.smartdatalake.definitions.Environment.expressionEvaluatorFactory]].
 *
 * As there may be more than one SchemaProvider on the classpath (e.g. once more DataFrameSubFeed implementations
 * exist), a provider declares through [[supports]] which schema config values it can handle. The schema produced is
 * converted to the schema type of the consuming engine downstream (see
 * [[io.smartdatalake.workflow.dataframe.SchemaConverter]]), so it is not necessary that the parsing provider matches
 * the executing engine exactly.
 *
 * Implementations must be Scala `object`s (a companion object implementing this trait), so they can be instantiated
 * through [[ScalaUtil.companionOf]].
 */
trait SchemaProvider {

  /**
   * Whether this provider can parse the given schema configuration value, based on its schema provider type prefix
   * (see [[SchemaProviderType.parse]]). This must not throw; return `false` for unknown or unsupported prefixes.
   */
  def supports(schemaConfig: String): Boolean

  /**
   * Parse the schema from the configuration value.
   *
   * @param schemaConfig    the schema config value, including the schema provider type prefix terminated by '#'.
   * @param lazyFileReading if true, file-based schema providers may defer reading/parsing the file by returning a
   *                        [[io.smartdatalake.workflow.dataframe.LazyGenericSchema]].
   */
  def readSchemaFromConfigValue(schemaConfig: String, lazyFileReading: Boolean): GenericSchema
}
