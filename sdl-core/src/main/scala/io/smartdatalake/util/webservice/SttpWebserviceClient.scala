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
package io.smartdatalake.util.webservice

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.SttpUtil.{SttpRequest, SttpRequestExtension, createDefaultBackend, parseUrl}
import io.smartdatalake.workflow.connection.authMode.AuthMode
import io.smartdatalake.workflow.dataobject.WebserviceFileDataObject
import sttp.client3.{Identity, SttpBackend, asByteArray, basicRequest}
import sttp.model.{HeaderNames, Method, Uri}

import scala.util.Try

private[smartdatalake] case class SttpWebserviceClient(uri: Uri,
                                                       request: SttpRequest[Array[Byte]],
                                                       retries: Int,
                                                       context: Option[String])
                                                      (implicit httpBackend: SttpBackend[Identity, Any])
  extends WebserviceClient {
  override def get(params: Map[String, String]): Try[Array[Byte]] = send(method = Method.GET, params = params)

  override def post(body: Array[Byte], mimeType: String, params: Map[String, String]): Try[Array[Byte]] = send(Method.POST, body, mimeType, params)

  override def put(body: Array[Byte], mimeType: String, params: Map[String, String]): Try[Array[Byte]] = send(Method.PUT, body, mimeType, params)

  override def patch(body: Array[Byte], mimeType: String, params: Map[String, String]): Try[Array[Byte]] = send(Method.PATCH, body, mimeType, params)

    private def send(method: Method, body: Array[Byte] = Array(), mimeType: String = "", params: Map[String, String] = Map()): Try[Array[Byte]] = {
      val contextForErrorMsg = context.getOrElse(f"$method method when requesting at $uri")
      val uriWithParams = uri.addParams(params)
      val req = method match {
        case Method.GET => request.get(uriWithParams)
        case Method.PUT => request.put(uriWithParams).header(HeaderNames.ContentType, mimeType).body(body)
        case Method.POST => request.post(uriWithParams).header(HeaderNames.ContentType, mimeType).body(body)
        case Method.PATCH => request.patch(uriWithParams).header(HeaderNames.ContentType, mimeType).body(body)
        case unsupported =>
          throw new IllegalArgumentException(s"Unsupported HTTP method:" +
            s" $unsupported. Only GET, POST, PUT, and PATCH are supported")
      }
      Try(SttpUtil.sendRequest(req, contextForErrorMsg, retries))
    }
}

private[smartdatalake] object SttpWebserviceClient extends SmartDataLakeLogger {

  def apply(config: WebserviceFileDataObject, url: Option[String] = None): SttpWebserviceClient = {
    apply(url.getOrElse(config.url), config.additionalHeaders, config.timeouts, config.authMode, config.proxy, config.followRedirects, config.retries, None)
  }
  def apply(url: String,
            additionalHeaders: Map[String, String],
            timeouts: Option[HttpTimeoutConfig],
            authMode: Option[AuthMode],
            proxy: Option[HttpProxyConfig],
            followRedirects: Boolean,
            retries: Int,
            sttpBackendOption: Option[SttpBackend[Identity, Any]]): SttpWebserviceClient = {

    val uri = parseUrl(url)
    val request = basicRequest
      .response(asByteArray)
      .headers(additionalHeaders)
      .optionalReadTimeout(timeouts)
      .applyAuthMode(authMode)
      .followRedirects(followRedirects)
    val sttpBackend = sttpBackendOption.getOrElse(createDefaultBackend(proxy, timeouts))

    new SttpWebserviceClient(uri = uri, request = request, retries = retries, context = None)(sttpBackend)
  }

}
