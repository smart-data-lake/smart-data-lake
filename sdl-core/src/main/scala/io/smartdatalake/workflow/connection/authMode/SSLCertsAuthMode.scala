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
 *
 * Sets Kafka `security.protocol=SSL` and configures mutual TLS: the keystore holds the client certificate and
 * private key presented to the broker, the truststore holds the CA certificate used to verify the broker.
 * Pick this over [[SASLSCRAMAuthMode]] if the broker authenticates clients by certificate instead of
 * user/password. Currently supported by KafkaConnection only.
 *
 * Example:
 * {{{
 * connections = {
 *   kafka-con {
 *     type = KafkaConnection
 *     brokers = "kafka-broker-1:9093,kafka-broker-2:9093"
 *     authMode = {
 *       type = SSLCertsAuthMode
 *       keystorePath = "/etc/kafka/secrets/kafka.keystore.jks"
 *       keystorePass = "###ENV#KAFKA_KEYSTORE_PWD###"
 *       truststorePath = "/etc/kafka/secrets/kafka.truststore.jks"
 *       truststorePass = "###ENV#KAFKA_TRUSTSTORE_PWD###"
 *     }
 *   }
 * }
 * }}}
 *
 * @param keystorePath   path to the Java keystore containing the client certificate and private key
 * @param keystoreType   type of the keystore, e.g. "JKS" or "PKCS12" (default: "JKS")
 * @param keystorePass   password of the keystore (supports secret providers). Although declared optional for
 *                       configuration parsing, it must be defined, otherwise a ConfigurationException is thrown.
 * @param truststorePath path to the Java truststore containing the CA certificate of the server
 * @param truststoreType type of the truststore, e.g. "JKS" or "PKCS12" (default: "JKS")
 * @param truststorePass password of the truststore (supports secret providers). Although declared optional for
 *                       configuration parsing, it must be defined, otherwise a ConfigurationException is thrown.
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
}

object SSLCertsAuthMode extends FromConfigFactory[AuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SSLCertsAuthMode =
    extract[SSLCertsAuthMode](config)
}
