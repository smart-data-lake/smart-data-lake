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
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.authMode.{AuthMode, HttpHeaderAuth}
import io.smartdatalake.workflow.dataobject.{HttpProxyConfig, HttpTimeoutConfig, WebserviceFileDataObject}
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpResponse}

import scala.util.{Failure, Success, Try}

private[smartdatalake] case class ScalaJWebserviceClient(request: HttpRequest) extends WebserviceClient {
  private val contentTypeHeader = "content-type"
  override def get(params: Map[String,String] = Map()): Try[Array[Byte]] = {
    exec(request.params(params))
  }
  override def post(body: Array[Byte], mimeType: String, params: Map[String,String] = Map()): Try[Array[Byte]] = {
    exec(request.header(contentTypeHeader, mimeType).params(params).postData(body))
  }
  override def put(body: Array[Byte], mimeType: String, params: Map[String,String] = Map()): Try[Array[Byte]] = {
    exec(request.header(contentTypeHeader, mimeType).params(params).put(body))
  }
  override def patch(body: Array[Byte], mimeType: String, params: Map[String,String] = Map()): Try[Array[Byte]] = {
    exec(request.header(contentTypeHeader, mimeType).params(params).postData(body).method("PATCH"))
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
   * Creates a [[WebserviceClient]] using WebserviceFileDataObject configuration
   */
  def apply(config: WebserviceFileDataObject, url: Option[String] = None): ScalaJWebserviceClient = {
    apply(url.getOrElse(config.url), config.additionalHeaders, config.timeouts, config.authMode, config.proxy, config.followRedirects)
  }

  /**
   * Creates a [[WebserviceClient]]
   */
  def apply(url: String, additionalHeaders: Map[String,String],
            timeouts: Option[HttpTimeoutConfig],
            authMode: Option[AuthMode],
            proxy: Option[HttpProxyConfig],
            followRedirects: Boolean
           ): ScalaJWebserviceClient = {
    logger.debug(s"Creating request for $url: additionalHeaders=$additionalHeaders")
    val request = Http(url)
      .headers(additionalHeaders)
      .optionally(timeouts, (v:HttpTimeoutConfig, request:HttpRequest) => request.timeout(v.connectionTimeoutMs, v.readTimeoutMs))
      .applyAuthMode(authMode)
      .optionally(proxy, (v:HttpProxyConfig, request:HttpRequest) => {
        val request1 = request.proxy(v.host, v.port)
        (v.user, v.password) match {
          case (Some(user), Some(pwd)) => request1.proxyAuth(user.resolve(), pwd.resolve())
          case _ => request1
        }
      })
      .option(HttpOptions.followRedirects(followRedirects))
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
        val bodyStr = if (logger.isDebugEnabled) new String(errorResponse.body) else new String(errorResponse.body).take(200)+"..."
        logger.error(s"Error when calling ${request.url}: Http status code: ${errorResponse.code}, response body: $bodyStr")
        Failure(new WebserviceException(s"Webservice Request failed with error <${errorResponse.code}>", Some(errorResponse.code), Some(new String(errorResponse.body))))
      // Request was successful and response can be processed further
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
          case x => throw ConfigurationException(s"authentication mode $x is not supported by ScalaJWebserviceClient")
        }
      })
    }
  }
}