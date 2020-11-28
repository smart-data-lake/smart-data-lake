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
import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.testutils.TestUtil
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scalaj.http.HttpResponse

class WebserviceClientTest extends FunSuite with BeforeAndAfterEach  {

  val port = 8080 // for some reason, only the default port seems to work
  val httpsPort = 8443
  val host = "127.0.0.1"
  private var wireMockServer: WireMockServer = _

  override protected def beforeEach(): Unit = {
    wireMockServer = TestUtil.setupWebservice(host, port, httpsPort)
  }

  override protected def afterEach(): Unit = {
    wireMockServer.stop()
  }

  test("Wrong init: authHeader but missing url") {
    intercept[ConfigurationException] {
      DefaultWebserviceClient(None, Some(Map("auth-header"->"AuthHeader")), None, None)
    }
  }

  test("Wrong init: user and password but missing url") {
    intercept[ConfigurationException] {
      DefaultWebserviceClient(None, Some(Map("user"->"user","password"->"password")), None, None)
    }
  }

  test("Wrong init: token but missing url") {
    intercept[ConfigurationException] {
      DefaultWebserviceClient(None, Some(Map("token"->"edizdzzd")), None, None)
    }
  }

  test("Wrong init: missing url and authHeader") {
    intercept[ConfigurationException] {
      DefaultWebserviceClient(url = None, authOptions = None, None, None)
    }
  }

  test("Wrong init: missing url, no authentication") {
    intercept[ConfigurationException] {
      DefaultWebserviceClient(url = None)
    }
  }

  test("Call webservice with wrong Url") {
    val webserviceClient = DefaultWebserviceClient(Some("http://..."), Some(Map("auth-header"->"Basic xyz")), None, None)
    val response = webserviceClient.get()
    assert(response.isFailure)
  }

  test("Call webservice without Authentication") {
    val url = Some(s"http://$host:$port/good/no_auth/")
    val webserviceClient = DefaultWebserviceClient(url)
    val response = webserviceClient.get()
    assert(response.isSuccess)
  }

  // TODO: Get https calls working. Error: Failure(javax.net.ssl.SSLHandshakeException: Remote host closed connection during handshake)
  ignore("Call a URL with Basic authentication") {
    val url = Some(s"https://$host:$httpsPort/good/basic_auth/")

    val webserviceClient = DefaultWebserviceClient(url, Some(Map("auth-header"->"Basic ZnMxOmZyZWl0YWcyMDE3x")), None, None)
    val response = webserviceClient.get()
    assert(response.isSuccess)
  }

  // TODO: Get https calls working. Error: Failure(javax.net.ssl.SSLHandshakeException: Remote host closed connection during handshake)
  ignore("Call Webservice with clientid") {
    val params = Map("client-id"->"127d4463","client-secret"->"17c85ae97525eabada371a2d82d50e3a")
    val call = DefaultWebserviceClient(Some(s"https://$host:$httpsPort/good/client_id/"), Some(params), None, None)
    val resp = call.get()
    assert(resp.isSuccess)
  }

  test("Call webservice with invalid AuthHeader") {
    val url = Some(s"https://$host:$httpsPort/good/basic_auth/") // still calling good url but with bad auth
    val webserviceClient = DefaultWebserviceClient(url, Some(Map("auth-header"->"Basic xxxxxxxxxxxxx")), None, None)
    val response = webserviceClient.get()
    assert(response.isFailure)
  }

  test("Check response: http status code == 200") {
    val webserviceClient = DefaultWebserviceClient(Some(s"http://$host:$port/bad/basic_auth/"), Some(Map("auth-header"->"Basic xyz")), None, None)
    val response = HttpResponse(body = "hello there".getBytes, code = 200, headers = Map())
    val check = webserviceClient.checkResponse(response)
    assert(check.isSuccess)
  }

  test("Check response with http error status code") {
    val webserviceClient = DefaultWebserviceClient(Some(s"http://$host:$port/bad/basic_auth/"), Some(Map("auth-header"->"Basic xyz")), None, None)
    val response = HttpResponse(body = "error".getBytes, code = 403, headers = Map())
    val check = webserviceClient.checkResponse(response)
    assert(check.isFailure)
  }

  test("Check posting JSON") {
    val webserviceClient = DefaultWebserviceClient(Some(s"http://$host:$port/good/post/no_auth"))
    val testJson = s"""{name: "Samantha", age: 31, city: "San Francisco"};""".getBytes
    webserviceClient.post(testJson, "application/json")
    val response = HttpResponse(body=testJson, code = 200, headers= Map())
    val check = webserviceClient.checkResponse(response)
    assert(check.isSuccess)
  }

}
