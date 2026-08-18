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
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.secrets.StringOrSecret


/**
 * Connect with custom HTTP-header based authentication
 *
 * The configured class implements [[CustomHttpAuthModeLogic]] and returns the HTTP headers to add to every
 * request; `options` are handed over once in the prepare phase, e.g. to acquire a token.
 *
 * Deprecated since 2.7.1: implement [[HttpAuthMode]] (or [[AuthMode]]) directly instead, which gives access
 * to typed configuration attributes rather than an untyped option map.
 *
 * Example:
 * {{{
 * dataObjects = {
 *   ext-departures {
 *     type = WebserviceFileDataObject
 *     url = "https://opensky-network.org/api/flights/departure"
 *     authMode = {
 *       type = CustomHttpAuthMode
 *       className = "com.example.auth.MyCustomHttpAuthMode"
 *       options = { apiKey = "###ENV#API_KEY###" }
 *     }
 *   }
 * }
 * }}}
 *
 * @param className class name implementing trait [[CustomHttpAuthModeLogic]]
 * @param options   Options to pass to the custom auth mode logic in prepare function.
 */
@deprecated("Implement AuthMode directly instead", "2.7.1")
case class CustomHttpAuthMode(className: String, options: Map[String, StringOrSecret]) extends HttpAuthMode with HttpHeaderAuth {
  private val impl = CustomCodeUtil.getClassInstanceByName[CustomHttpAuthModeLogic](className)

  override def prepare(): Unit = impl.prepare(options)

  override def getHeaders: Map[String, String] = impl.getHeaders
}

object CustomHttpAuthMode extends FromConfigFactory[HttpAuthMode] {
  @scala.annotation.nowarn("cat=deprecation")
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomHttpAuthMode = {
    extract[CustomHttpAuthMode](config)
  }
}

trait CustomHttpAuthModeLogic extends HttpHeaderAuth {
  def prepare(options: Map[String, StringOrSecret]): Unit = ()
}
