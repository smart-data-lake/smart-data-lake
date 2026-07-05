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
package io.smartdatalake.testutils

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.dataset.Equality

import java.nio.file.Files

/**
 * Utility methods for testing.
 */
object WebserviceTestUtil extends SmartDataLakeLogger with Equality {

  // extract keystore file from resource jar for wiremock server
  private lazy val wiremockKeyStoreFile = {
    val resource = "test_keystore.pkcs12"
    val keyStorePath = Files.createTempDirectory("test").resolve(resource)
    val inputStream = Option(getClass.getResourceAsStream("/" + resource))
      .getOrElse(throw new RuntimeException(s"Could not find resource $resource in classpath"))
    Files.copy(inputStream, keyStorePath)
    inputStream.close()
    keyStorePath.toString
  }

  /**
   * Setup simple webserver with given ports Different stubs are generated automatically to answer
   * different URLs with predefined return codes
   *
   * @param host
   * bind address, usually localhost / 127.0.0.1
   * @param port
   * port for http calls
   * @param httpsPort
   * port for https calls
   * @return
   * instance of [[WireMockServer]]
   */
  def startWebservice(host: String, port: Int, httpsPort: Int): WireMockServer = {
    configureFor(host, port)
    val wireMockServer =
      new WireMockServer(
        wireMockConfig()
          .port(port)
          .httpsPort(httpsPort)
          .bindAddress(host)
          .keystorePath(wiremockKeyStoreFile)
          .keystorePassword("mytruststorepassword")
          .asynchronousResponseEnabled(false)
      )
    wireMockServer
      .start()
    wireMockServer
  }

  def setupWebserviceStubs(): Unit = {
    stubFor(post(urlEqualTo("/good/post/no_auth"))
      .willReturn(aResponse().withBody("{{request.path.[0]}}"))
    )

    stubFor(get(urlEqualTo("/good/no_auth/"))
      .willReturn(aResponse().withStatus(200))
    )

    stubFor(get(urlMatching("/good/basic_auth/"))
      .withHeader("Authorization", equalTo("Basic ZnMxOmZyZWl0YWcyMDE3x"))
      .willReturn(ok("request looks good"))
    )

    stubFor(get(urlMatching("/good/client_id/"))
      .withHeader("Authorization", equalTo("Basic ZnMxOmZyZWl0YWcyMDE3x"))
      .willReturn(ok("request looks good"))
    )

    stubFor(get(urlMatching("/good/token/"))
      .withHeader("Authorization", equalTo("Bearer ZnMxOmZyZWl0YWcyMDE3x"))
      .willReturn(ok("request looks good"))
    )

    stubFor(get(urlMatching("/bad/*/"))
      .willReturn(aResponse.withStatus(404))
    )
  }

}
