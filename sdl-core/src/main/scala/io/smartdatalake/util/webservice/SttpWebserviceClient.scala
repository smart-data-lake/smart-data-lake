/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.util.webservice.SttpUtil.{createDefaultBackendOptions, SttpRequest}
import io.smartdatalake.workflow.connection.authMode.{AuthMode, HttpHeaderAuth}
import io.smartdatalake.workflow.dataobject.WebserviceFileDataObject


import scala.util.Try
import scala.concurrent.duration.{Duration, MILLISECONDS}
import sttp.client3.{HttpClientSyncBackend, Identity, RequestT, SttpBackend, asByteArray, basicRequest}
import sttp.model.{Method, Uri}
import sttp.client3._

private[smartdatalake] case class SttpWebserviceClient(uri: Uri,
                                                          request: SttpRequest,
                                                          context: Option[String])
                                                         (implicit httpBackend: SttpBackend[Identity, Any]) extends WebserviceClient {
  val contentTypeHeader = "content-type"
  override def get(params: Map[String, String]): Try[Array[Byte]] = send(method = Method.GET, params = params)

  override def post(body: Array[Byte], mimeType: String, params: Map[String, String]): Try[Array[Byte]] = send(Method.POST, body, mimeType, params)

  override def put(body: Array[Byte], mimeType: String, params: Map[String, String]): Try[Array[Byte]] = send(Method.PUT, body, mimeType, params)

  override def patch(body: Array[Byte], mimeType: String, params: Map[String, String]): Try[Array[Byte]] = send(Method.PATCH, body, mimeType, params)

  private def send(method: Method, body: Array[Byte] = Array(), mimeType: String = "", params: Map[String, String] = Map()): Try[Array[Byte]] = {
    val contextForErrorMsg = context.getOrElse(f"$method method when requesting at $uri")
    val uriWithParams = uri.addParams(params)
    val req = method match {
      case Method.GET => request.get(uriWithParams)
      case Method.PUT => request.put(uriWithParams).header(contentTypeHeader, mimeType).body(body)
      case Method.POST => request.post(uriWithParams).header(contentTypeHeader, mimeType).body(body)
      case Method.PATCH => request.patch(uriWithParams).header(contentTypeHeader, mimeType).body(body)
    }
    Try(SttpUtil.sendRequest(req, contextForErrorMsg))
  }
}

private[smartdatalake] object SttpWebserviceClient extends SmartDataLakeLogger {

  def apply(config: WebserviceFileDataObject, url: Option[String] = None): SttpWebserviceClient = {
    apply(url.getOrElse(config.url), config.additionalHeaders, config.timeouts, config.authMode, config.proxy, config.followRedirects, None)
  }
  def apply(url: String,
            additionalHeaders: Map[String, String],
            timeouts: Option[HttpTimeoutConfig],
            authMode: Option[AuthMode],
            proxy: Option[HttpProxyConfig],
            followRedirects: Boolean,
            sttpBackendOption: Option[SttpBackend[Identity, Any]]): SttpWebserviceClient = {

    def defaultBackend: SttpBackend[Identity, Any] = HttpClientSyncBackend(createDefaultBackendOptions(proxy, timeouts))

    val uri: Uri = uri"$url"
    val request = basicRequest
      .response(asByteArray)
      .headers(additionalHeaders)
      .optionally(timeouts, (v:HttpTimeoutConfig, req:SttpRequest) => req.readTimeout(Duration(v.readTimeoutMs, MILLISECONDS)))
      .applyAuthMode(authMode)
      .followRedirects(followRedirects)
    @transient val sttpBackend = sttpBackendOption.getOrElse(defaultBackend)

    new SttpWebserviceClient(uri = uri, request = request, context = None)(sttpBackend)
  }



  /**
   * Extend functionality of the the RequestT class
   */
  implicit class SttpRequestExtension[T, R](request: RequestT[Empty, T, R]) {
    def optionally[A](config: Option[A], func: (A, RequestT[Empty, T, R]) => RequestT[Empty, T, R]): RequestT[Empty, T, R] = {
      if (config.isDefined) func(config.get, request) else request
    }

    //TODO: also allow OAuth2, which is supported by sttp and already used by ODataDataObject
    def applyAuthMode(authMode: Option[AuthMode]): RequestT[Empty, T, R] = {
      request.optionally(authMode, (v: AuthMode, request: RequestT[Empty, T, R]) => {
        v match {
          case headerAuth: HttpHeaderAuth => request.headers(headerAuth.getHeaders)
          case x => throw ConfigurationException(s"authentication mode $x is not supported by SttpWebserviceClient")
        }
      })
    }
  }

}
