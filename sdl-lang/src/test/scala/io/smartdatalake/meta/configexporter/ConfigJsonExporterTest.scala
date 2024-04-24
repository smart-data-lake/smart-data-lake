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

import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, put, putRequestedFor, stubFor, urlPathMatching, verify}
import io.smartdatalake.testutils.TestUtil
import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.JsonMethods
import org.json4s.{StringInput, _}
import org.scalatest.FunSuite

import java.io.File

class ConfigJsonExporterTest extends FunSuite {
  private val descriptionPath = getClass.getResource("/dagexporter/description").getPath

  test("export config") {
    val exporterConfig = ConfigJsonExporterConfig(Seq(getClass.getResource("/dagexporter").getPath), descriptionPath = Some(descriptionPath))
    implicit val hadoopConf: Configuration = new Configuration()
    val actualOutput = ConfigJsonExporter.exportConfigJson(exporterConfig)
    val actualJsonOutput = JsonMethods.parse(StringInput(actualOutput))
    assert((actualJsonOutput \ "actions").children.size === 8)
    assert((actualJsonOutput \ "dataObjects").children.size === 14)
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_origin" \ "lineNumber" === JInt(66))
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_origin" \ "endLineNumber" === JNothing)
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_origin" \ "path" === JString("dagexporterTest.conf"))
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_columnDescriptions" \ "a" === JString("Beschreibung A"))
    assert((actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_columnDescriptions" \ "b.[].b1").asInstanceOf[JString].s.linesIterator.toSeq === Seq("Beschreibung B1","2nd line B1 text"))
    assert(((actualJsonOutput \ "actions" \ "actionId6" \ "transformers")(0) \ "_parameters")(0) \ "name" === JString("session"))
    assert((actualJsonOutput \ "actions" \ "actionId8" \ "transformers")(0) \ "_sourceDoc" === JString("Documentation for TestTransformer.\nThis should be exported by ConfigJsonExporter!"))
  }

  test("test main file export") {
    val fileName = "target/exportedConfig.json"
    ConfigJsonExporter.main(Array("-c", getClass.getResource("/dagexporter/dagexporterTest.conf").getFile, "-f", fileName))
    assert(new File(fileName).exists())
  }

  test("test main with default filename") {
    ConfigJsonExporter.main(Array("-c", getClass.getResource("/dagexporter/dagexporterTest.conf").getFile))
    assert(new File("exportedConfig.json").exists())
  }

  test("test main api uplaod") {
    val port = 8080 // for some reason, only the default port seems to work
    val httpsPort = 8443
    val host = "127.0.0.1"
    val wireMockServer = TestUtil.startWebservice(host, port, httpsPort)
    stubFor(put(urlPathMatching("/api/v1/.*"))
      .willReturn(aResponse().withStatus(200))
    )
    val target = s"http://localhost:$port/api/v1?repo=abc"
    ConfigJsonExporter.main(Array("-c", getClass.getResource("/dagexporter/dagexporterTest.conf").getFile, "-t", target, "-d", descriptionPath, "--uploadDescriptions"))
    verify(putRequestedFor(urlPathMatching("/api/v1/config?.*")))
    verify(putRequestedFor(urlPathMatching("/api/v1/description?.*")))
    wireMockServer.stop()
  }
}
