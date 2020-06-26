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

import java.util.Properties

import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.definitions.{AuthMode, SSLCertsAuthMode}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.config.SslConfigs



import scala.collection.JavaConverters._

/**
 * Connection information for kafka
 *
 * @param id unique id of this connection
 * @param brokers comma separated list of kafka bootstrap server incl. port, e.g. "host1:9092,host2:9092:
 * @param schemaRegistry url of schema registry service, e.g. "https://host2"
 * @param dataSourceOptions Options for the Kafka stream reader (see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
 * @param metadata
 */
case class KafkaConnection(override val id: ConnectionId,
                           brokers: String,
                           schemaRegistry: Option[String] = None,
                           dataSourceOptions: Map[String,String] = Map(),
                           authMode: Option[AuthMode] = None,
                           override val metadata: Option[ConnectionMetadata] = None
                          ) extends Connection {


      @transient private lazy val adminClient = {
        val props = new Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.putAll(authProps)
        AdminClient.create(props)
      }

  val KafkaConfigOptionPrefix = "kafka."
  val KafkaSSLSecurityProtocol = "SSL"

  // Early validation and partition init use kafka clients directly and need
  // to authenticate to broker
  val authProps = {
    val props = new Properties()
    authMode match {
      case Some(m: SSLCertsAuthMode) => {
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, KafkaSSLSecurityProtocol)
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, m.keyStorePath)
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, m.keyStorePass)
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, m.keyStoreType.getOrElse(SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE))
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, m.trustStorePath)
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, m.trustStorePass)
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, m.trustStoreType.getOrElse(SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE))
      }
      case _ => {}
    }
        props
    }

  // Kafka Configs are prepended with "kafka." in data source option map
  val authOptions = authProps.asScala.map(c => (s"${KafkaConfigOptionPrefix}${c._1}", c._2))

      def topicExists(topic: String): Boolean = {
        adminClient.listTopics.names.get.asScala.contains(topic)
      }

      override def factory: FromConfigFactory[Connection] = KafkaConnection
    }

      object KafkaConnection extends FromConfigFactory[Connection] {
        override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): KafkaConnection = {
          import configs.syntax.ConfigOps
          config.extract[KafkaConnection].value
        }
      }
