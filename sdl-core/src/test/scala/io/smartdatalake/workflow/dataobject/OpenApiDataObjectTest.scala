/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

import com.github.tomakehurst.wiremock.client.WireMock._
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.ResourceUtil
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.funsuite.AnyFunSuite

class OpenApiDataObjectTest extends AnyFunSuite {
  protected implicit lazy val session: SparkSession = TestUtil.session
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  implicit val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  val port = 8080 // for some reason, only the default port seems to work
  val httpsPort = 8443
  val host = "127.0.0.1"

  test("read openapi spec from classpath") {
    val do1 = OpenApiDataObject(
      id = "do1",
      baseUrl = "https://test.com",
      apiDocsUrl = "cp:/openApiSpec/sampleApiDoc.json",
      operationId = "getPing",
    )
    do1.prepare
    val df = do1.getSparkDataFrame()(contextInit)

    val schemaExpected = StructType.fromDDL("id long, username string")
    assert(DataType.equalsIgnoreNullability(df.schema, schemaExpected))
  }

  test("read openapi webservice") {
    import session.implicits._
    val wireMockServer = TestUtil.startWebservice(host, port, httpsPort)
    stubFor(get(urlEqualTo("/sampleApiDoc.json"))
      .willReturn(aResponse().withBody(ResourceUtil.readResourceAsString(new Path("cp:/openApiSpec/sampleApiDoc.json"))))
    )
    stubFor(get(urlEqualTo("/ping"))
      .willReturn(aResponse().withBody("""{"id": 123, "username": "john"}"""))
    )
    val do1 = OpenApiDataObject(
      id = "do1",
      baseUrl = s"http://$host:$port",
      apiDocsUrl = "sampleApiDoc.json",
      operationId = "getPing",
    )
    do1.prepare
    val df = do1.getSparkDataFrame()(contextExec)
    val result = df.as[(Long, String)].collect.toSeq
    assert(result == Seq((123L, "john")))

    wireMockServer.stop()
  }

  test("read openapi webservice with paging") {
    import session.implicits._
    val wireMockServer = TestUtil.startWebservice(host, port, httpsPort)
    stubFor(get(urlEqualTo("/sampleApiDocPaging.json"))
      .willReturn(aResponse().withBody(ResourceUtil.readResourceAsString(new Path("cp:/openApiSpec/sampleApiDocPaging.json"))))
    )
    stubFor(get(urlEqualTo("/paging"))
      .willReturn(aResponse().withBody(s"""{"id": 123, "username": "john", "nextLink": "http://$host:$port/paging2"}"""))
    )
    stubFor(get(urlEqualTo("/paging2"))
      .willReturn(aResponse().withBody(s"""{"id": 456, "username": "peter"}"""))
    )
    val do1 = OpenApiDataObject(
      id = "do1",
      baseUrl = s"http://$host:$port",
      apiDocsUrl = "sampleApiDocPaging.json",
      operationId = "getPaging",
      pagingLinkJsonPath = Some("$.nextLink")
    )
    do1.prepare
    val df = do1.getSparkDataFrame()(contextExec)
      .drop("nextLink")
    val result = df.as[(Long, String)].collect.toSeq
    assert(result == Seq((123L, "john"), (456L, "peter")))

    wireMockServer.stop()
  }

}
