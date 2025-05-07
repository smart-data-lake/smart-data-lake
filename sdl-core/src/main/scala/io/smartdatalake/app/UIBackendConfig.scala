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

package io.smartdatalake.app

import io.netty.handler.codec.http.HttpUtil
import io.smartdatalake.util.misc.{SmartDataLakeLogger, StateUploader}
import io.smartdatalake.util.webservice.{HttpProxyConfig, HttpTimeoutConfig}
import io.smartdatalake.util.webservice.SttpUtil.{getContent, createDefaultBackendOptions}
import io.smartdatalake.workflow.connection.authMode.HttpAuthMode
import sttp.client3.{BasicRequestBody, HttpClientSyncBackend, Identity, SttpBackend, SttpBackendOptions, basicRequest}
import sttp.model.Uri.PathSegment
import sttp.model._

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/**
 * Configuration of the UI backend to upload state updates of the Job runs.
 *
 * The UI backend can manage state from different projects and environments.
 * Uploaded job state is stored within the following hierarchy: tenant (customer), repository (project) and environment.
 *
 * Example to configure AWS UI Backend; additionally create a file 'ui-auth' in the project directory which contains key-value pairs for clientId, user and pwd.
 * {{{
 *   global {
 *     uiBackend {
 *     baseUrl = "https://nn92uqy13e.execute-api.eu-central-1.amazonaws.com/DEV/api/v1"
 *     tenant = PrivateTenant
 *     repo = int-testing
 *     env = std
 *     authMode {
 *       type = AWSUserPwdAuthMode
 *       region = eu-central-1
 *       userPool = sdlb-ui
 *       useIdToken = true
 *       clientId = "###FILE#ui-auth;clientId###"
 *       user = "###FILE#ui-auth;user###"
 *       password = "###FILE#ui-auth;pwd###"
 *     }
 *   }
 * }}}
 *
 * @param baseUrl        API base URL for upload. Add tenant, repo (required) and env as query parameters, e.g. https://<host>?tenant=<tenant>&repo=<repository>&env=<environment>"
 * @param tenant         Optional unique name for your organization within the SDLB UI. The userId set through authMode needs to have access to this tenant.
 *                       Default for tenant is the special "PrivateTenant". In this case the API will use the userId of the authenticated user as tenant.
 * @param repo           Repository under which this upload should be stored in the UI backend.
 *                       The repository name normally has the meaning of the project or application.
 * @param env            Environment under which this upload should be stored in the UI backend, e.g. dev or prd.
 *                       Default is 'std'.
 * @param authMode       optional configuration of authentication to access UI backend API through baseUrl.
 * @param stagePath      optional (but recommended) hadoop directory to save state if upload fails temporarily.
 *                       Upload of these files is retried on initialization of next SDLB run.
 * @param processUpdates optional; if false, only initial & final state is uploaded, otherwise all state updates are uploaded as partial updates.
 *                       Default is processUpdates=true.
 * @param timeouts       configuration of HTTP timeouts. Default is connectionTimeout=500ms, readTimeout=5s.
 */
case class UIBackendConfig(
                            baseUrl: String,
                            tenant: String = "PrivateTenant",
                            repo: String,
                            env: String = "std",
                            authMode: Option[HttpAuthMode] = None,
                            stagePath: Option[String] = None,
                            processUpdates: Boolean = true,
                            timeouts: HttpTimeoutConfig = HttpTimeoutConfig(500, 5000),
                            proxy: Option[HttpProxyConfig] = None
                          ) extends SmartDataLakeLogger {

  private val sttpBackendOptions = createDefaultBackendOptions(proxy, Some(timeouts))

  @transient private lazy val httpBackend: SttpBackend[Identity, Any] = HttpClientSyncBackend(sttpBackendOptions)
  private val params = Map("tenant" -> tenant, "repo" -> repo, "env" -> env)

  def getStateListener: StateListener = {
    authMode.map(_.getHeaders)
    logger.info(s"UI Backend upload initialized. baseUrl=$baseUrl tenant=$tenant repo=$repo env=$env authMode=${authMode.map(_.getClass.getSimpleName)}")
    new StateUploader(getUploadService, stagePath, processUpdates)
  }

  def getUploadService: UploadService = {
    new UploadService() {
      override def sendBytes(operation: String, body: Option[Array[Byte]] = None, multipartBody: Option[Seq[Part[BasicRequestBody]]] = None, method: Method = Method.POST, additionalParams: Map[String, String] = Map(), mediaType: MediaType = MediaType.ApplicationJson): Option[String] = {
        assert(body.isEmpty || multipartBody.isEmpty, "Only body or multipartBody can be set.")
        logger.debug(s"operation=$operation method=$method params=$params additionalParams=$additionalParams mediaType=$mediaType bodyLength=${body.map(_.length).getOrElse(0)}")
        var request = basicRequest
          .method(method, Uri.unsafeParse(baseUrl).addPathSegment(PathSegment(operation)).addParams(params ++ additionalParams))
          .header(Header.contentType(mediaType))
          .headers(authMode.map(_.getHeaders).getOrElse(Map()))
          .readTimeout(FiniteDuration(timeouts.readTimeoutMs, TimeUnit.MILLISECONDS))
          .followRedirects(true)
        body.foreach(b => request = request.body(b))
        multipartBody.foreach(mp => request = request.multipartBody(mp.head, mp.tail: _*))
        val response = request.send(httpBackend)
        Option(getContent(response, s"$method $operation")).filter(_.nonEmpty)
      }
    }
  }
}

trait UploadService extends SmartDataLakeLogger {
  def sendBytes(operation: String, body: Option[Array[Byte]] = None, multipartBody: Option[Seq[Part[BasicRequestBody]]] = None, method: Method = Method.POST, additionalParams: Map[String, String] = Map(), mediaType: MediaType = MediaType.ApplicationJson): Option[String]

  def send(operation: String, body: Option[String] = None, method: Method = Method.POST, additionalParams: Map[String, String] = Map(), mediaType: MediaType = MediaType.ApplicationJson): Option[String] = {
    sendBytes(operation, body = body.map(_.getBytes("UTF-8")), method = method, additionalParams = additionalParams, mediaType = mediaType)
  }
}

case class FileDescriptor(name: String, mediaType: String, size: Long, lastModified: Timestamp)

private[smartdatalake] object UploadDefaults {
  val versionDefault = "latest"
}