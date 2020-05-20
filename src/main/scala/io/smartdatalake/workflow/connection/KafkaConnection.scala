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
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.JavaConverters._

case class KafkaConnection(override val id: ConnectionId,
                           brokers: String,
                           schemaRegistry: Option[String],
                           override val metadata: Option[ConnectionMetadata] = None
                          ) extends Connection {

  @transient private lazy val adminClient = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    AdminClient.create(props)
  }

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
