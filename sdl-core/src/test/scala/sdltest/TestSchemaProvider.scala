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
// Note: this package is intentionally outside io.smartdatalake so it is NOT picked up by the classpath discovery
// in Environment.schemaProviders (which scans package io.smartdatalake). It is only used through the global option override.
package sdltest

import io.smartdatalake.util.misc.SchemaProvider
import io.smartdatalake.workflow.dataframe.GenericSchema

/** Thrown by [[TestSchemaProvider]] to prove it was selected and invoked. */
class TestSchemaProviderInvoked(schemaConfig: String) extends RuntimeException(schemaConfig)

/** A [[SchemaProvider]] for tests that simply records that it was invoked by throwing. */
object TestSchemaProvider extends SchemaProvider {
  override def supports(schemaConfig: String): Boolean = true
  override def readSchemaFromConfigValue(schemaConfig: String, lazyFileReading: Boolean): GenericSchema =
    throw new TestSchemaProviderInvoked(schemaConfig)
}
