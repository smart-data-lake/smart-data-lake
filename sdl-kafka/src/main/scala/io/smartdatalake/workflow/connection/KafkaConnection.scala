/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.connection

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.workflow.connection.authMode.{AuthMode, SASLSCRAMAuthMode, SSLCertsAuthMode}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.spark.sql.confluent.ConfluentClient

import java.util.Properties
import scala.jdk.CollectionConverters._

/**
 * Connection information for kafka
 *
 * @param id             unique id of this connection
 * @param brokers        comma separated list of kafka bootstrap server incl. port, e.g. "host1:9092,host2:9092:
 * @param schemaRegistry url of schema registry service, e.g. "https://host2"
 * @param options        Options for the Kafka stream reader (see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
 * @param metadata
 */
case class KafkaConnection(override val id: ConnectionId,
                           brokers: String,
                           schemaRegistry: Option[String] = None,
                           options: Map[String, String] = Map(),
                           authMode: Option[AuthMode] = None,
                           override val metadata: Option[ConnectionMetadata] = None
                          ) extends Connection {

  @transient private lazy val adminClient = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    authProps.asScala.foreach { case (k, v) => props.setProperty(k, v) }
    AdminClient.create(props)
  }

  private[smartdatalake] val KafkaConfigOptionPrefix = "kafka."
  private val KafkaSSLSecurityProtocol = "SSL"
  private val KafkaSASLSSLSecurityProtocol = "SASL_SSL"

  // Early validation and partition init use kafka clients directly and need
  // to authenticate to broker
  private[workflow] val authProps = {
    val props = new Properties()
    authMode match {
      case Some(m: SSLCertsAuthMode) => {
        props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, KafkaSSLSecurityProtocol)
        props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, m.keystorePath)
        props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, m.keystorePassSecret.resolve())
        props.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, m.keystoreType)
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, m.truststorePath)
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, m.truststorePassSecret.resolve())
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, m.truststoreType)
      }
      case Some(m: SASLSCRAMAuthMode) => {
        props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, KafkaSASLSSLSecurityProtocol)
        // set sasl auth
        props.setProperty("sasl.mechanism", m.sslMechanism)
        val jaasLoginModule = if (m.sslMechanism.toLowerCase == "plain") "org.apache.kafka.common.security.plain.PlainLoginModule"
        else "org.apache.kafka.common.security.scram.ScramLoginModule"
        props.setProperty("sasl.jaas.config", s"""$jaasLoginModule required username="${m.username.resolve()}" password="${m.passwordSecret.resolve()}";""")
        // set truststore if defined
        m.truststorePath.foreach { truststorePath =>
          props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath)
          props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            m.truststorePassSecret.map(_.resolve()).getOrElse(throw ConfigurationException("truststorePassSecret must be set if truststorePath is set.")))
          props.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, m.truststoreType)
        }
      }
      case Some(m) => throw ConfigurationException(s"${m.getClass.getSimpleName} is not supported for ${getClass.getSimpleName}")
      case None => ()
    }
    props
  }

  // Kafka Configs are prepended with "kafka." in data source option map
  private val authOptions = authProps.asScala.map(c => (s"${KafkaConfigOptionPrefix}${c._1}", c._2))
  private[workflow] val sparkOptions = authOptions ++ options + (KafkaConfigOptionPrefix + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers)

  def topicExists(topic: String): Boolean = {
    adminClient.listTopics.names.get.asScala.contains(topic)
  }

  def testSchemaRegistry(): Unit = {
    try {
      schemaRegistry.foreach(new ConfluentClient(_).test())
    } catch {
      case e:Exception => throw ConfigurationException(s"($id) Can not connect to schema registry (${schemaRegistry.get})", None, e)
    }
  }

  override def factory: FromConfigFactory[Connection] = KafkaConnection
}

object KafkaConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): KafkaConnection = {
    extract[KafkaConnection](config)
  }
}
