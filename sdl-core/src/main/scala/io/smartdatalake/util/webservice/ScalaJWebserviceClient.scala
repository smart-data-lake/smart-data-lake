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

package io.smartdatalake.util.webservice

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions.{AuthMode, BasicAuthMode, HttpHeaderAuth}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataobject.{HttpProxyConfig, HttpTimeoutConfig, WebserviceFileDataObject}
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpResponse}

import scala.util.{Failure, Success, Try}

private[smartdatalake] case class ScalaJWebserviceClient(request: HttpRequest) extends WebserviceClient {
  private val contentTypeHeader = "content-type"
  override def get(): Try[Array[Byte]] = {
    exec(request)
  }
  override def post(body: Array[Byte], mimeType: String): Try[Array[Byte]] = {
    exec(request.header(contentTypeHeader, mimeType).postData(body))
  }
  override def put(body: Array[Byte], mimeType: String): Try[Array[Byte]] = {
    exec(request.header(contentTypeHeader, mimeType).put(body))
  }
  private def exec(request: HttpRequest): Try[Array[Byte]] = {
    Try(request.asBytes) match {
      case Success(response) => ScalaJWebserviceClient.checkResponse(request, response)
      case Failure(exception) => Failure(exception)
    }
  }
}

private[smartdatalake] object ScalaJWebserviceClient extends SmartDataLakeLogger {

  /**
   * Creates a [[WebserviceClient]] using scalaj.http library
   */
  def apply(config: WebserviceFileDataObject, url: Option[String] = None): ScalaJWebserviceClient = {
    val request = Http(url.getOrElse(config.url))
      .headers(config.additionalHeaders)
      .optionally(config.timeouts, (v:HttpTimeoutConfig, request:HttpRequest) => request.timeout(v.connectionTimeoutMs, v.readTimeoutMs))
      .applyAuthMode(config.authMode)
      .optionally(config.proxy, (v:HttpProxyConfig, request:HttpRequest) => request.proxy(v.host, v.port))
      .option(HttpOptions.followRedirects(config.followRedirects))
    new ScalaJWebserviceClient(request)
  }

  /**
   *
   * Check response of webservice call
   *
   * If a request was sent and a response was received, check:
   * 1. Response has an error status: Failure[WebserviceException] is returned
   * 2. Response has a code 200: Return body as Success[String]
   *
   * @param request Webservice request that was made
   * @param response Response of webservice call or Failure
   * @return Success[Array[Byte]] or Failure[WebserviceException]
   */
  def checkResponse(request: HttpRequest, response: HttpResponse[Array[Byte]]): Try[Array[Byte]] = {
    response match {
      // Request was sent, but the response contains an error
      case errorResponse if errorResponse.isError =>
        logger.error(s"Error when calling ${request.url}: Http status code: ${errorResponse.code}, response body: ${new String(errorResponse.body).take(200)}...")
        Failure(new WebserviceException(s"Webservice Request failed with error <${errorResponse.code}>"))
      // Request was successfull and response can be processed further
      case normalResponse if normalResponse.isSuccess => Success(normalResponse.body)
    }
  }

  /**
   * Pimp my libary to apply optional configurations to HttpRequest
   */
  implicit class HttpRequestExtension(request: HttpRequest) {
    def optionally[T](config: Option[T], func: (T,HttpRequest) => HttpRequest): HttpRequest = {
      if (config.isDefined) func(config.get, request) else request
    }
    def applyAuthMode(authMode: Option[AuthMode]): HttpRequest = {
      request.optionally(authMode, (v:AuthMode, request:HttpRequest) => {
        v match {
          case headerAuth: HttpHeaderAuth => request.headers(headerAuth.getHeaders)
          case basicAuth: BasicAuthMode => request.auth(basicAuth.user, basicAuth.password)
          case x => throw ConfigurationException(s"authentication mode $x is not supported by ScalaJWebserviceClient")
        }
      })
    }
  }
}