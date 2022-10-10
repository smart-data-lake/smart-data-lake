/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

import org.scalatest.FunSuite

import java.io.File

class ConfigJsonExporterTest extends FunSuite {

  test("export config") {
    val exporterConfig = ConfigJsonExporterConfig(Seq(getClass.getResource("/dagexporter/dagexporterTest.conf").getPath))
    val actualOutput = ConfigJsonExporter.exportConfigJson(exporterConfig)
    assert(actualOutput.contains("origin"))
    assert(actualOutput.contains("dagexporter/dagexporterTest.conf"))
    assert(actualOutput.contains("lineNumber"))
    assert(actualOutput.contains("actionId1"))
  }

  test("test main") {
    ConfigJsonExporter.main(Array("-c", getClass.getResource("/dagexporter/dagexporterTest.conf").getPath))
    assert(new File("exportedConfig.json").exists())
  }


}
