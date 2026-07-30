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
 * Authenticate using SASL_SSL Authentication.
 *
 * Configuration needed are username and password, plus a Java truststore if the brokers certificate is not trusted
 * by the default JVM truststore.
 *
 * Sets Kafka `security.protocol=SASL_SSL` and builds the corresponding `sasl.jaas.config`. Pick this over
 * [[SSLCertsAuthMode]] if the broker authenticates users by name/password (SCRAM or PLAIN) rather than by
 * client certificate. Currently supported by KafkaConnection only.
 *
 * Example:
 * {{{
 * connections = {
 *   kafka-con {
 *     type = KafkaConnection
 *     brokers = "kafka-broker-1:9093,kafka-broker-2:9093"
 *     authMode = {
 *       type = SASLSCRAMAuthMode
 *       username = "###ENV#KAFKA_USER###"
 *       password = "###ENV#KAFKA_PWD###"
 *       sslMechanism = "SCRAM-SHA-512"
 *       truststorePath = "/etc/kafka/secrets/kafka.truststore.jks"
 *       truststorePassSecret = "###ENV#KAFKA_TRUSTSTORE_PWD###"
 *     }
 *   }
 * }
 * }}}
 *
 * @param username             user name to authenticate with (supports secret providers)
 * @param password             password to authenticate with (supports secret providers). Although declared
 *                             optional for configuration parsing, it must be defined, otherwise a
 *                             ConfigurationException is thrown.
 * @param sslMechanism         SASL mechanism to use, e.g. "SCRAM-SHA-512", "SCRAM-SHA-256" or "PLAIN". It is
 *                             passed as Kafka `sasl.mechanism` and selects the JAAS login module
 *                             (PlainLoginModule for "plain", ScramLoginModule otherwise).
 * @param truststorePath       optional path to the Java truststore holding the brokers CA certificate. Only
 *                             needed if the broker certificate is not trusted by the default JVM truststore.
 * @param truststoreType       type of the truststore, e.g. "JKS" or "PKCS12" (default: "JKS")
 * @param truststorePassSecret password of the truststore (supports secret providers). Mandatory if
 *                             `truststorePath` is set.
 */
case class SASLSCRAMAuthMode(
    username: StringOrSecret,
    private val password: Option[StringOrSecret],
    sslMechanism: String,
    truststorePath: Option[String],
    truststoreType: String = "JKS",
    private[smartdatalake] val truststorePassSecret: Option[StringOrSecret]
) extends AuthMode {
  private[smartdatalake] val passwordSecret: StringOrSecret = password
    .getOrElse(throw ConfigurationException(s"password must be defined."))
  override def factory: FromConfigFactory[AuthMode] = SASLSCRAMAuthMode
}

object SASLSCRAMAuthMode extends FromConfigFactory[AuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SASLSCRAMAuthMode =
    extract[SASLSCRAMAuthMode](config)
}
