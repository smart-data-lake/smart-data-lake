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
package io.smartdatalake.meta.configexporter

import io.smartdatalake.config.ConfigurationException
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests the command line wiring of [[CatalogSchemaUpdater]].
 * The catalog changes themselves are tested per DataObject, see CatalogMetadataBehaviour.
 */
class CatalogSchemaUpdaterTest extends AnyFunSuite {

  private val configPath = getClass.getResource("/dagexporter/dagexporterTest.conf").getPath

  test("plan mode runs without changing the catalog") {
    // no --source and no global.dataObjectsSchemaSource: there is no exported schema, so the table can not
    // be created and there is nothing to apply. This checks that the command line is wired up correctly.
    CatalogSchemaUpdater.main(Array("-c", configPath, "--mode", "plan", "-i", "dataObjectJdbc14"))
  }

  test("plan mode ignores DataObjects without catalog support") {
    CatalogSchemaUpdater.main(Array("-c", configPath, "-i", "dataObjectCsv1"))
  }

  test("invalid mode is rejected by the parser") {
    val ex = intercept[ConfigurationException] {
      CatalogSchemaUpdater.main(Array("-c", configPath, "--mode", "export"))
    }
    assert(ex.getMessage.contains("command line"))
  }
}
