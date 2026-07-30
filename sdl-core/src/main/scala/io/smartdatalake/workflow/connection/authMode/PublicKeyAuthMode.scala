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
 * Validate by user and private/public key Private key is read from .ssh
 *
 * Only the user name is configured; the private key is taken from the default ssh location (~/.ssh) of the
 * process running SDLB, and the corresponding public key must be registered on the server. Choose this over
 * [[BasicAuthMode]] to avoid storing a password in the configuration. Currently supported by
 * SFtpFileRefConnection only.
 *
 * Example:
 * {{{
 * connections = {
 *   sftp-src {
 *     type = SFtpFileRefConnection
 *     host = "sftp.example.com"
 *     authMode = {
 *       type = PublicKeyAuthMode
 *       user = "###ENV#SFTP_USER###"
 *     }
 *   }
 * }
 * }}}
 *
 * @param user user to login with (supports secret providers). Although declared optional for configuration
 *             parsing, it must be defined, otherwise a ConfigurationException is thrown.
 */
case class PublicKeyAuthMode(private val user: Option[StringOrSecret]) extends AuthMode {
  private[smartdatalake] val userSecret: StringOrSecret = user
    .getOrElse(throw ConfigurationException(s"user must be defined."))

  override def factory: FromConfigFactory[AuthMode] = PublicKeyAuthMode
}

object PublicKeyAuthMode extends FromConfigFactory[AuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): PublicKeyAuthMode =
    extract[PublicKeyAuthMode](config)
}
