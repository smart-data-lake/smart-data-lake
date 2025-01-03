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
import scalaj.http.HttpConstants

/**
 * Authenticate using basic user/pwd authentication.
 *
 * For http connection this will create a basic authentication header.
 */
case class BasicAuthMode(private val user: Option[StringOrSecret],
                         private val password: Option[StringOrSecret],
                         @Deprecated @deprecated("Use `user` instead", "2.5.0") private val userVariable: Option[String] = None,
                         @Deprecated @deprecated("Use `password` instead", "2.5.0") private val passwordVariable: Option[String] = None)
  extends HttpAuthMode {
  private val _user: StringOrSecret = user.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(userVariable.get))
  private val _password: StringOrSecret = password.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(passwordVariable.get))

  def userSecret: StringOrSecret = _user

  def passwordSecret: StringOrSecret = _password

  override def getHeaders: Map[String, String] = Map("Authorization" -> HttpConstants.basicAuthValue(userSecret.resolve(), passwordSecret.resolve()))

  override def factory: FromConfigFactory[HttpAuthMode] = BasicAuthMode

}

object BasicAuthMode extends FromConfigFactory[HttpAuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): BasicAuthMode = {
    extract[BasicAuthMode](config)
  }
}