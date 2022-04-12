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

import com.typesafe.config.ConfigFactory
import io.smartdatalake.testutils.{DataObjectTestSuite, TestUtil}
import io.smartdatalake.util.json.JsonUtils
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.action.script.CmdScript
import org.apache.spark.sql.types.DataType
import org.json4s.{Formats, JObject, JString}

import java.nio.file.Files
import scala.collection.mutable

class AirbyteDataObjectTest extends DataObjectTestSuite {

  private def parseMessage(msg: String): AirbyteMessage = {
    AirbyteMessage.parseOutput(Stream(msg), mutable.Buffer(), false).head
  }

  import session.implicits._

  // Note: this test needs WSL installed on windows
  test("cmd test") {
    val configStr = """
      my-config = "test"
    """
    val config = ConfigFactory.parseString(configStr)
    // read script from resources and write to temp file
    val scriptString = CustomCodeUtil.readResourceFile("airbyteConnectorMock.sh")
    val scriptFile = Files.createTempFile("script", ".sh")
    Files.write(scriptFile, scriptString.replace("\r\n", "\n").getBytes) // remove windows line endings for wsl!
    scriptFile.toFile.setExecutable(true)
    val script = scriptFile.toString
    val dataObject = AirbyteDataObject("test", config, "mystream", cmd = CmdScript(
      winCmd = Some(s"wsl $script"), // replace windows line endings to unix before start
      linuxCmd = Some(script))
    )
    dataObject.prepare
    val actual = dataObject.getSparkDataFrame()(contextExec)
    val expected = Seq(("TEST", "A", "2345.67", "Test Auto")).toDF("produkttyp", "flag", "artikelID", "artikelbezeichnung")
    val resultat = expected.isEqual(actual)
    if (!resultat) TestUtil.printFailedTestResult("wsl cmd test", Seq())(actual)(expected)
    assert(resultat)
  }

  test("parse log") {
    val msg = parseMessage("""{"type": "LOG", "log": {"level": "INFO", "message": "Check succeeded"}}""")
    assert(msg == AirbyteLogMessage(AirbyteLogMessage.Level.INFO, "Check succeeded"))
  }

  test("parse connection status") {
    val msg = parseMessage("""{"type": "CONNECTION_STATUS", "connectionStatus": {"status": "SUCCEEDED"}}""")
    assert(msg == AirbyteConnectionStatus(AirbyteConnectionStatus.Status.SUCCEEDED, None))
  }

  test("parse catalog") {
   	val msg = parseMessage("""{ "type": "CATALOG", "catalog": { "streams": [ { "name": "mystream", "json_schema": { "$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "properties": { "produkttyp": { "type": "string" }, "flag": { "type": "string" }, "artikelID": { "type": "string" }, "artikelbezeichnung": { "type": "string" } } }, "supported_sync_modes": [ "full_refresh" ]} ] } }""")
    val stream = AirbyteStream("mystream",
      json_schema = JObject(List(("$schema",JString("http://json-schema.org/draft-07/schema#")), ("type",JString("object")), ("properties",JObject(("produkttyp",JObject(List(("type",JString("string"))))), ("flag",JObject(List(("type",JString("string"))))), ("artikelID",JObject(List(("type",JString("string"))))), ("artikelbezeichnung",JObject(List(("type",JString("string"))))))))),
      supported_sync_modes = Seq(SyncModeEnum.full_refresh)
    )
    val catalog = AirbyteCatalog(Seq(stream))
    assert(msg.toString == catalog.toString) // interestingly the objects are not equal, but the string representation is!
    val schema = DataType.fromDDL("struct<produkttyp:string,flag:string,artikelID:string,artikelbezeichnung:string>")
    //assert(msg.asInstanceOf[AirbyteCatalog].streams.head.getSparkSchema == schema)
  }

  test("parse record") {
    val msg = parseMessage("""{"type": "RECORD", "record": {"stream": "mystream", "data": {"produkttyp": "TEST", "flag": "A", "artikelID": "2345.67", "artikelbezeichnung": "Test Auto"}, "emitted_at": 1640029476000}}""")
    println(msg)
    val record =  AirbyteRecordMessage("mystream", JObject(List(("produkttyp",JString("TEST")), ("flag",JString("A")), ("artikelID",JString("2345.67")), ("artikelbezeichnung",JString("Test Auto")))), 1640029476000L, None)
    assert(msg.toString == record.toString) // interestingly the objects are not equal, but the string representation is!
  }

  test("de/serialization round-trip") {
    implicit val jsonFormats: Formats = AirbyteMessage.formats
    val stream = AirbyteStream("mystream",JObject(List(("$schema",JString("http://json-schema.org/draft-07/schema#")), ("type",JString("object")), ("properties",JObject(List(("produkttyp",JObject(List(("type",JString("string"))))), ("flag",JObject(List(("type",JString("string"))))), ("artikelID",JObject(List(("type",JString("string"))))), ("artikelbezeichnung",JObject(List(("type",JString("string")))))))))),Seq())
    val catalog = AirbyteCatalog(Seq(stream))
    val jsonMsg = """{"type": "CATALOG", "catalog": """ + JsonUtils.caseClassToJsonString(catalog) + """}"""
    val parsedCatalog = parseMessage(jsonMsg)
    assert(catalog.toString == parsedCatalog.toString)
  }
}
