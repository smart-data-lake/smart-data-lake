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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.secrets.StringOrSecret

/**
 * Authenticate using a custom HTTP header.
 *
 * Use this when the webservice expects its credential in a proprietary header like `X-API-KEY` instead of
 * the standard `Authorization` header. The secret is sent verbatim as header value, so a scheme prefix such
 * as "Basic " or "Bearer " must be part of the secret itself. Only usable for HTTP based connections and
 * DataObjects.
 *
 * Example:
 * {{{
 * dataObjects = {
 *   ext-departures {
 *     type = WebserviceFileDataObject
 *     url = "https://opensky-network.org/api/flights/departure"
 *     authMode = {
 *       type = AuthHeaderMode
 *       headerName = "X-API-KEY"
 *       secret = "###ENV#API_KEY###"
 *     }
 *   }
 * }
 * }}}
 *
 * @param headerName name of the HTTP header to set, e.g. "X-API-KEY"
 * @param secret     value to set as header content (supports secret providers)
 * @see [[BasicAuthMode]] and [[TokenAuthMode]] for the standard `Authorization` header variants.
 */
case class AuthHeaderMode(
    headerName: String,
    private val secret: StringOrSecret
) extends HttpAuthMode {

  override def getHeaders: Map[String, String] = Map(headerName -> secret.resolve())
}

object AuthHeaderMode extends FromConfigFactory[HttpAuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AuthHeaderMode =
    extract[AuthHeaderMode](config)
}
