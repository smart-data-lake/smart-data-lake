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
 * Authenticate using SSL Certificates.
 *
 * Configuration needed are a Java keystore and truststore.
 */
case class SSLCertsAuthMode(
                             keystorePath: String,
                             keystoreType: String = "JKS",
                             @Deprecated @deprecated("Use `keystorePass` instead", "2.5.0") private val keystorePassVariable: Option[String] = None,
                             private val keystorePass: Option[StringOrSecret],
                             truststorePath: String,
                             truststoreType: String = "JKS",
                             @Deprecated @deprecated("Use `truststorePass` instead", "2.5.0") private val truststorePassVariable: Option[String] = None,
                             private val truststorePass: Option[StringOrSecret]
                           ) extends AuthMode {
  private val _keystorePass = keystorePass.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(keystorePassVariable.get))
  private val _truststorePass = truststorePass.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(truststorePassVariable.get))

  private[smartdatalake] val truststorePassSecret: StringOrSecret = _keystorePass
  private[smartdatalake] val keystorePassSecret: StringOrSecret = _truststorePass

  override def factory: FromConfigFactory[AuthMode] = SSLCertsAuthMode
}

object SSLCertsAuthMode extends FromConfigFactory[AuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SSLCertsAuthMode = {
    extract[SSLCertsAuthMode](config)
  }
}