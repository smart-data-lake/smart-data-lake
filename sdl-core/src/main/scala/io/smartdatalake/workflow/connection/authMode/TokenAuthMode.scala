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
package io.smartdatalake.workflow.connection.authMode

import com.typesafe.config.Config
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.secrets.StringOrSecret

/**
 * Interface to generalize authentication for token based authentication
 */
trait TokenAuth {

  /**
   * Return authentication token.
   */
  def getToken: String
}

/**
 * Authenticate using a predefined token.
 *
 * For HTTP Connections the token is used as Authorization header.
 *
 * The header is built as "tokenType token", e.g. "Authorization: Bearer eyJ...". Use this for services
 * accessed with a long-living personal access token or API key. If the token has to be requested and
 * refreshed during the run, use [[OAuthMode]] instead; if the credential goes into a non-standard header,
 * use [[AuthHeaderMode]].
 *
 * Example:
 * {{{
 * dataObjects = {
 *   ext-departures {
 *     type = WebserviceFileDataObject
 *     url = "https://opensky-network.org/api/flights/departure"
 *     authMode = {
 *       type = TokenAuthMode
 *       token = "###ENV#API_TOKEN###"
 *     }
 *   }
 * }
 * }}}
 *
 * @param tokenType
 *   token type to use in HTTP Authorization header. Default is "Bearer".
 * @param token
 *   token to authenticate with (supports secret providers). Although declared optional for configuration
 *   parsing, it must be defined, otherwise a ConfigurationException is thrown.
 */
case class TokenAuthMode(
    tokenType: String = "Bearer",
    private val token: Option[StringOrSecret]
) extends HttpAuthMode with TokenAuth with HttpHeaderAuth {
  private[smartdatalake] val tokenSecret: StringOrSecret = token
    .getOrElse(throw ConfigurationException(s"token must be defined."))

  override def getHeaders: Map[String, String] =
    Map("Authorization" -> s"$tokenType ${tokenSecret.resolve()}")

  override def getToken: String = tokenSecret.resolve()

  override def factory: FromConfigFactory[HttpAuthMode] = TokenAuthMode
}

object TokenAuthMode extends FromConfigFactory[HttpAuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): TokenAuthMode =
    extract[TokenAuthMode](config)
}
