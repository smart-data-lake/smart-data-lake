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
 * Authenticate using SSL Certificates.
 *
 * Configuration needed are a Java keystore and truststore.
 */
case class SSLCertsAuthMode(
    keystorePath: String,
    keystoreType: String = "JKS",
    private val keystorePass: Option[StringOrSecret],
    truststorePath: String,
    truststoreType: String = "JKS",
    private val truststorePass: Option[StringOrSecret]
) extends AuthMode {
  private[smartdatalake] val truststorePassSecret: StringOrSecret = truststorePass
    .getOrElse(throw ConfigurationException(s"truststorePass must be defined."))
  private[smartdatalake] val keystorePassSecret: StringOrSecret = keystorePass
    .getOrElse(throw ConfigurationException(s"keystorePass must be defined."))
  override def factory: FromConfigFactory[AuthMode] = SSLCertsAuthMode
}

object SSLCertsAuthMode extends FromConfigFactory[AuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SSLCertsAuthMode =
    extract[SSLCertsAuthMode](config)
}
