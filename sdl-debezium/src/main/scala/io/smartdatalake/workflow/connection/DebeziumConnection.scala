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
package io.smartdatalake.workflow.connection
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Environment
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
 *
 *  Example config:
 *
 *  dbzCon {
 *  type = DebeziumConnection
 *  dbEngine = "postgresql"
 *  hostname = "localhost"
 *  db = "test"
 *  port = 5432
 *  authMode {
 *    type = BasicAuthMode
 *    userVariable = "ENV#POSTGRES_USER"
 *    passwordVariable  = "ENV#POSTGRES_PW"
 *   }
 *  }
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
  require(supportedDbEngines.contains(dbEngine.toLowerCase) || dbEngine.contains("."), s"Engine '${dbEngine}' not supported by ${this.getClass.getSimpleName}. Supported database engines are (${supportedDbEngines.map(_.toLowerCase).mkString(", ")})")

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
    "mysql" -> "io.debezium.connector.mysql.MySqlConnector",
    "postgresql" -> "io.debezium.connector.postgresql.PostgresConnector",
    "oracle" -> "io.debezium.connector.oracle.OracleConnector",
    "mariadb" -> "io.debezium.connector.mariadb.MariaDbConnector",
    "sqlserver" -> "io.debezium.connector.sqlserver.SqlServerConnector",
    "db2" -> "io.debezium.connector.db2.Db2Connector"
  )

  def getDbEngineConnectorClassName(dbName: String): Option[String] = {

    val dbEngineConnectorClassName = dbEngineConnectorClassNames.get(dbName.toLowerCase)

    try {
      // check if class exists on classpath
      Environment.classLoader().loadClass(dbEngineConnectorClassName.get)
    } catch {
      case e: ClassNotFoundException =>
        throw new ClassNotFoundException(s"Class not found: '${dbEngineConnectorClassName.get}'. Make sure to add the debezium connector dependency for $dbName. Pay attention to the correct configuration of the maven dependency (see SDLB documentation).", e)
    }

    dbEngineConnectorClassName
  }

  def supportedDbEngines(): Seq[String] = {
    dbEngineConnectorClassNames.keys.toSeq
  }
}
