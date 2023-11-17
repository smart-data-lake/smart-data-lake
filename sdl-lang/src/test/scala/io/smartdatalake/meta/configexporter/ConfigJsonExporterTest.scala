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

import org.json4s.StringInput
import org.json4s.jackson.JsonMethods
import org.scalatest.FunSuite

import java.io.File
import org.json4s.JsonDSL._
import org.json4s._

class ConfigJsonExporterTest extends FunSuite {

  test("export config") {
    val exporterConfig = ConfigJsonExporterConfig(Seq(getClass.getResource("/dagexporter").getPath), descriptionPath = Some(getClass.getResource("/dagexporter/description").getPath))
    val actualOutput = ConfigJsonExporter.exportConfigJson(exporterConfig)
    val actualJsonOutput = JsonMethods.parse(StringInput(actualOutput))
    assert((actualJsonOutput \ "actions").children.size === 8)
    assert((actualJsonOutput \ "dataObjects").children.size === 14)
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_origin" \ "lineNumber" === JInt(66))
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_origin" \ "endLineNumber" === JNothing)
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_origin" \ "path" === JString("dagexporterTest.conf"))
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_columnDescriptions" \ "a" === JString("Beschreibung A"))
    assert((actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_columnDescriptions" \ "b.[].b1").asInstanceOf[JString].s.linesIterator.toSeq === Seq("Beschreibung B1","2nd line B1 text"))
  }

  test("test main") {
    val fileName = "target/exportedConfig.json"
    ConfigJsonExporter.main(Array("-c", getClass.getResource("/dagexporter/dagexporterTest.conf").getFile, "-f", fileName))
    assert(new File(fileName).exists())
  }


}
