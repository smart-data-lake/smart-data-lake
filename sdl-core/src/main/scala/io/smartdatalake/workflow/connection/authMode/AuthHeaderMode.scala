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
import io.smartdatalake.util.secrets.{SecretsUtil, StringOrSecret}


/**
 * Authenticate using a custom HTTP header.
 */
case class AuthHeaderMode(
                           headerName: String,
                           private val secret: Option[StringOrSecret],
                           @Deprecated @deprecated("Use `secret` instead", "2.5.0") private val secretVariable: Option[String] = None
                         ) extends HttpAuthMode {
  private val _secret = secret.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(secretVariable.get))
  private[smartdatalake] val stringOrSecret: StringOrSecret = _secret

  override def getHeaders: Map[String, String] = Map(headerName -> stringOrSecret.resolve())

  override def factory: FromConfigFactory[HttpAuthMode] = AuthHeaderMode
}

object AuthHeaderMode extends FromConfigFactory[HttpAuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AuthHeaderMode = {
    extract[AuthHeaderMode](config)
  }
}