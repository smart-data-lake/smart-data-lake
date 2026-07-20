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

import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.dataframe.LazyGenericSchema
import org.scalatest.funsuite.AnyFunSuite
import sdltest.TestSchemaProviderInvoked

class SchemaProviderTest extends AnyFunSuite {

  test("readSchemaFromConfigValue falls back to LazyGenericSchema when no SchemaProvider is on the classpath") {
    Environment._schemaProviders = None
    try {
      // sdl-core has no SchemaProvider implementation on its (test) classpath, so discovery yields none
      val schema = SchemaUtil.readSchemaFromConfigValue("ddl#a int, b string")
      assert(schema.isInstanceOf[LazyGenericSchema])
    } finally Environment._schemaProviders = None
  }

  test("global option schemaProvider enforces a concrete SchemaProvider") {
    System.setProperty("sdl.schemaProvider", "sdltest.TestSchemaProvider")
    Environment._schemaProviders = None
    try {
      val ex = intercept[TestSchemaProviderInvoked](SchemaUtil.readSchemaFromConfigValue("ddl#a int"))
      assert(ex.getMessage == "ddl#a int")
    } finally {
      System.clearProperty("sdl.schemaProvider")
      Environment._schemaProviders = None
    }
  }
}
