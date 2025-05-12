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
import io.debezium.connector.db2.Db2Connector
import shaded.io.debezium.connector.mariadb.MariaDbConnector
import io.debezium.connector.mongodb.MongoDbConnector
import shaded.io.debezium.connector.mysql.MySqlConnector
import io.debezium.connector.oracle.OracleConnector
import io.debezium.connector.postgresql.PostgresConnector
import io.debezium.connector.spanner.SpannerConnector
import io.debezium.connector.sqlserver.SqlServerConnector
import io.debezium.connector.vitess.VitessConnector
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.workflow.connection.authMode.{AuthMode, BasicAuthMode}

/**
 * Connection information for debezium connection
 *
 * @param id unique id of this connection
 * @param dbEngine what database engine to use, currently supported engines: mysql, postgresql, oracle, mariadb, mongodb, sqlserver, db2, vitess, spanner
 * @param hostname database server
 * @param db database to read data from
 * @param port
 * @param authMode authentication information: for now BasicAuthMode is supported.
 * @param metadata optional connection metadata
 */
case class DebeziumConnection(override val id: ConnectionId,
                              dbEngine: String,
                              hostname: String,
                              db: Option[String] = None,
                              port: Int,
                              authMode: AuthMode,
                              override val metadata: Option[ConnectionMetadata] = None
                             ) extends Connection {

  // Allow only supported authentication modes
  private val supportedAuthModes = Seq(classOf[BasicAuthMode])
  require(supportedAuthModes.contains(authMode.getClass), s"${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuthModes.map(_.getSimpleName).mkString(", ")}.")

  // Allow only supported databases engines
  private val supportedDbEngines = DbEngineHelper.supportedDbEngines()
  require(supportedDbEngines.contains(dbEngine.toLowerCase), s"Engine '${dbEngine}' not supported by ${this.getClass.getSimpleName}. Supported database engines are (${supportedDbEngines.map(_.toLowerCase).mkString(", ")})")

  private[smartdatalake] def connectionPropertiesMap: Map[String, String] = {

    authMode match {
      case m: BasicAuthMode => {
        val propertiesMap =   Map(
          "connector.class" -> DbEngineHelper.getDbEngineConnectorClassName(dbEngine).get,
          "database.hostname" -> hostname,
          "database.port" -> port.toString,
          "database.user" -> m.userSecret.resolve(),
          "database.password" -> m.passwordSecret.resolve()
        )

        if(db.isDefined) propertiesMap ++ Map("database.dbname" -> db.get) else propertiesMap

      }
      case _ => throw new IllegalArgumentException(s"($id) No supported authMode given for Debezium connection.")
    }

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

private object DbEngineHelper {

  private val dbEngineConnectorClassNames: Map[String, String] = Map(
    "mysql" -> classOf[MySqlConnector].getName,
    "postgresql" -> classOf[PostgresConnector].getName,
    "oracle" -> classOf[OracleConnector].getName,
    "mariadb" -> classOf[MariaDbConnector].getName,
    "mongodb" -> classOf[MongoDbConnector].getName,
    "sqlserver" -> classOf[SqlServerConnector].getName,
    "db2" -> classOf[Db2Connector].getName,
    "vitess" -> classOf[VitessConnector].getName,
    "spanner" -> classOf[SpannerConnector].getName
  )

  def getDbEngineConnectorClassName(dbName: String): Option[String] = {
    dbEngineConnectorClassNames.get(dbName.toLowerCase)
  }

  def supportedDbEngines(): Seq[String] = {
    dbEngineConnectorClassNames.keys.toSeq
  }
}
