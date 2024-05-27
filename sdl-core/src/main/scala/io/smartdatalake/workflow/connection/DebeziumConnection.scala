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

package io.smartdatalake.workflow.connection
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{AuthMode, BasicAuthMode}
import io.debezium.connector.mysql.MySqlConnector
import io.debezium.connector.postgresql.PostgresConnector
import io.smartdatalake.workflow.connection.DebeziumDatabaseEngine.DebeziumDatabaseEngine

import java.util.Properties

case class DebeziumConnection(override val id: ConnectionId,
                              dbEngine: DebeziumDatabaseEngine,
                              hostname: String,
                              port: Int,
                              authMode: Option[AuthMode] = None,
                              override val metadata: Option[ConnectionMetadata] = None
                             ) extends Connection {

  // Allow only supported authentication modes
  private val supportedAuthModes = Seq(classOf[BasicAuthMode])
  require(authMode.isEmpty || supportedAuthModes.contains(authMode.get.getClass), s"${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuthModes.map(_.getSimpleName).mkString(", ")}.")


  private def getDebeziumConnectionProperties: Properties = {
    val properties = new Properties()

    properties.setProperty("connector.class", dbEngine.toString)
    properties.setProperty("database.hostname", hostname)
    properties.setProperty("database.port", port.toString)
    properties.setProperty("database.user", authMode.asInstanceOf[BasicAuthMode].userSecret.resolve()) // TODO: Check with Zach regarding security
    properties.setProperty("database.password", authMode.asInstanceOf[BasicAuthMode].passwordSecret.resolve()) // TODO: Check with Zach regarding security

    properties
  }

  /**
   * Returns the factory that can parse this type (that is, type `CO`).
   *
   * Typically, implementations of this method should return the companion object of the implementing class.
   * The companion object in turn should implement [[FromConfigFactory]].
   *
   * @return the factory (object) for this class.
   */
  override def factory: FromConfigFactory[Connection] = DebeziumConnection
}

object DebeziumConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DebeziumConnection = {
    extract[DebeziumConnection](config)
  }
}

object DebeziumDatabaseEngine extends Enumeration {
  type DebeziumDatabaseEngine = Value

  val MySql: Value = Value(classOf[MySqlConnector].getName)
  val PostgreSql: Value = Value(classOf[PostgresConnector].getName)

}
