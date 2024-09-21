/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.util.webservice

import com.github.tomakehurst.wiremock.WireMockServer
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.authMode.{AuthHeaderMode, BasicAuthMode, CustomHttpAuthMode, CustomHttpAuthModeLogic}
import io.smartdatalake.workflow.dataobject.WebserviceFileDataObject
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import scalaj.http.{Http, HttpResponse}

class WebserviceClientTest extends FunSuite with BeforeAndAfter with BeforeAndAfterAll  {

  val port = 8080 // for some reason, only the default port seems to work
  val httpsPort = 8443
  val host = "127.0.0.1"
  private var wireMockServer: WireMockServer = _

  // provide an empty instance registry
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context : ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  override protected def beforeAll(): Unit = {
    wireMockServer = TestUtil.startWebservice(host, port, httpsPort)
  }

  override protected def afterAll(): Unit = {
    wireMockServer.stop()
  }

  before {
    wireMockServer.resetAll()
    TestUtil.setupWebserviceStubs()
  }

  test("Call webservice with wrong Url") {
    val webserviceDO = WebserviceFileDataObject("do1", url = "http://...")
    val webserviceClient = ScalaJWebserviceClient(webserviceDO)
    val response = webserviceClient.get()
    assert(response.isFailure)
  }

  test("Call webservice without Authentication") {
    val webserviceDO = WebserviceFileDataObject("do1", url = s"http://$host:$port/good/no_auth/")
    val webserviceClient = ScalaJWebserviceClient(webserviceDO)
    val response = webserviceClient.get()
    assert(response.isSuccess)
  }

  // TODO: Get https calls working. Error: Failure(javax.net.ssl.SSLHandshakeException: Remote host closed connection during handshake)
  ignore("Call a URL with Basic authentication") {
    val webserviceDO = WebserviceFileDataObject("do1", url = s"http://$host:$port/good/basic_auth/", authMode = Some(BasicAuthMode(Some(StringOrSecret("testuser")), Some(StringOrSecret("abc")))))
    val webserviceClient = ScalaJWebserviceClient(webserviceDO)
    val response = webserviceClient.get()
    assert(response.isSuccess)
  }

  test("Call webservice with invalid AuthHeader") {
    val webserviceDO = WebserviceFileDataObject("do1", url = s"http://$host:$port/good/basic_auth/", authMode = Some(AuthHeaderMode("auth-header", Some(StringOrSecret("Basic xxxxxxxxxxxxx")))))
    val webserviceClient = ScalaJWebserviceClient(webserviceDO)
    val response = webserviceClient.get()
    assert(response.isFailure)
  }

  test("Check response: http status code == 200") {
    val request = Http("http://...")
    val response = HttpResponse(body = "hello there".getBytes, code = 200, headers = Map())
    val check = ScalaJWebserviceClient.checkResponse(request, response)
    assert(check.isSuccess)
  }

  test("Check response with http error status code") {
    val request = Http("http://...")
    val response = HttpResponse(body = "error".getBytes, code = 403, headers = Map())
    val check = ScalaJWebserviceClient.checkResponse(request, response)
    assert(check.isFailure)
  }

  test("Check posting JSON") {
    val webserviceDO = WebserviceFileDataObject("do1", url = s"http://$host:$port/good/post/no_auth")
    val webserviceClient = ScalaJWebserviceClient(webserviceDO)
    val testJson = s"""{name: "Samantha", age: 31, city: "San Francisco"};""".getBytes
    val response = webserviceClient.post(testJson, "application/json")
    assert(response.isSuccess)
  }

  test("CustomAuthMode") {
    val webserviceDO = WebserviceFileDataObject("do1", url = s"http://$host:$port/good/post/no_auth", authMode = Some(CustomHttpAuthMode(className = classOf[MyCustomHttpAuthMode].getName, options = Map("test"-> StringOrSecret("ok")))))
    webserviceDO.prepare
    val webserviceClient = ScalaJWebserviceClient(webserviceDO)
    assert(webserviceClient.request.headers.toMap.apply("test") == "ok")
  }
}

private class MyCustomHttpAuthMode extends CustomHttpAuthModeLogic {
  var additionalHeaders: Map[String, StringOrSecret] = _
  override def prepare(options: Map[String, StringOrSecret]): Unit = {
    // add options as headers
    additionalHeaders = options
  }
  override def getHeaders = additionalHeaders.mapValues(_.resolve()).toMap
}