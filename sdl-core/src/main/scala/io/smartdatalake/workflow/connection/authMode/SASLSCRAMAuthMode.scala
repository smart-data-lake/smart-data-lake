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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.secrets.{SecretsUtil, StringOrSecret}

/**
 * Authenticate using SASL_SSL Authentication.
 *
 * Configuration needed are user, password and a Java truststore.
 */
case class SASLSCRAMAuthMode(
                              username: StringOrSecret,
                              @Deprecated @deprecated("Use `password` instead", "2.5.0") private val passwordVariable: Option[String] = None,
                              private val password: Option[StringOrSecret],
                              sslMechanism: String,
                              truststorePath: Option[String],
                              truststoreType: String = "JKS",
                              @Deprecated @deprecated("Use `truststorePass` instead", "2.5.0") private val truststorePassVariable: Option[String] = None,
                              private val truststorePass: Option[StringOrSecret],
                            ) extends AuthMode {
  private[smartdatalake] val passwordSecret: StringOrSecret = password.orElse(passwordVariable.map(SecretsUtil.convertSecretVariableToStringOrSecret))
    .getOrElse(throw ConfigurationException(s"password or passwordVariable must be defined."))
  private[smartdatalake] val truststorePassSecret: Option[StringOrSecret] = truststorePass.orElse(truststorePassVariable.map(SecretsUtil.convertSecretVariableToStringOrSecret))

  override def factory: FromConfigFactory[AuthMode] = SASLSCRAMAuthMode
}

object SASLSCRAMAuthMode extends FromConfigFactory[AuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SASLSCRAMAuthMode = {
    extract[SASLSCRAMAuthMode](config)
  }
}