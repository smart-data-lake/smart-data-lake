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

package io.smartdatalake.workflow.connection.authMode

import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.webservice.SttpUtil.{SttpRequestExtension, parseUrl}
import io.smartdatalake.util.webservice.{HttpProxyConfig, HttpTimeoutConfig, OAuth2Response, OAuth2Service}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import sttp.client3.basicRequest
import sttp.model.{Header, MediaType}

/**
 * [[OAuthMode]] contains the coordinates and credentials to gain access to the OData DataSource
 *
 * @param oauthUrl URL to the OAuth2 authorization instance like "https://login.microsoftonline.com/{tenant-guid}/oauth2/v2.0/token" (supports secret providers)
 * @param clientId Name of the user (supports secrets providers)
 * @param clientSecret Password of the user (supports secret providers)
 * @param oauthScope OAuth authorization scope (like https://xxx.crm4.dynamics.com/.default) (supports secret providers)
 * @param useIdToken If true, id_token is used for Http Authorization header, otherwise access_token. Default is false.
 */
case class OAuthMode (
                       oauthUrl: StringOrSecret,
                       clientId: StringOrSecret,
                       clientSecret: StringOrSecret,
                       oauthScope: StringOrSecret,
                       useIdToken: Boolean = false,
                       proxy: Option[HttpProxyConfig] = None,
                       timeouts: Option[HttpTimeoutConfig] = None
                     ) extends HttpAuthMode with SmartDataLakeLogger {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  private lazy val oAuth2Service = OAuth2Service(oauthUrl.resolve(), Some(clientId.resolve()), clientCredentialsInit, proxy, timeouts)
  private val oauthUri = parseUrl(oauthUrl.resolve())

  override def prepare(): Unit = {
    // initialize oAuth2Service
    oAuth2Service
  }

  private def clientCredentialsInit(): OAuth2Response = {
    logger.info(s"Authenticating using client_credentials flow")

    val payload: Map[String, String] = Map(
      "grant_type" -> "client_credentials",
      "client_id" -> clientId.resolve(),
      "client_secret" -> clientSecret.resolve(),
      "scope" -> oauthScope.resolve()
    )

    val request = basicRequest
      .optionalReadTimeout(timeouts)
      .post(oauthUri)
      .header("Content-Type", "application/x-www-form-urlencoded")
      .header(Header.accept(MediaType.ApplicationJson))
      .followRedirects(true)
      .body(payload) // Map is automatically serialized as "application/x-www-form-urlencoded" by sttp

    parse(oAuth2Service.sendRequest(request, "AWS initiate auth")).extract[OAuth2Response]
  }

  override def getHeaders: Map[String, String] = {
    Map(oAuth2Service.getToken.getAuthHeader(useIdToken))
  }

  override def factory: FromConfigFactory[HttpAuthMode] = OAuthMode
}


object OAuthMode extends FromConfigFactory[HttpAuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): OAuthMode = {
    extract[OAuthMode](config)
  }
}