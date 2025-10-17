/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

import com.snowflake.snowpark.Session
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.{ConnectionPoolConfig, JdbcExecution, SmartDataLakeLogger}
import io.smartdatalake.util.webservice.HttpProxyConfig
import io.smartdatalake.workflow.connection.authMode.{AuthMode, BasicAuthMode, OAuthMode}
import io.smartdatalake.workflow.connection.jdbc.DefaultJdbcCatalog
import net.snowflake.spark.snowflake.Utils
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}

import java.sql.{Connection => SqlConnection}

/**
 * Connection information for Snowflake databases.
 * The connection can be used for SnowflakeTableDataObjects
 * If multiple SnowflakeTableDataObjects share a connection, they share the same Snowpark session
 *
 * @param id        unique id of this connection
 * @param url       Snowflake connection url
 * @param warehouse Snowflake namespace
 * @param database  Snowflake database
 * @param role      Snowflake role
 * @param authMode  optional authentication information: for now BasicAuthMode is supported.
 * @param proxy     optional HTTP Proxy for Snowflake connection (Jdbc & Snowpark)
 * @param sparkOptions Options for the Snowflake Spark Connector, see https://docs.snowflake.com/en/user-guide/spark-connector-use#additional-options.
 * @param metadata  Connection metadata
 */
case class SnowflakeConnection(override val id: ConnectionId,
                               url: String,
                               warehouse: String,
                               database: String,
                               role: String,
                               authMode: AuthMode,
                               proxy: Option[HttpProxyConfig] = None,
                               sparkOptions: Map[String, String] = Map(),
                               override val metadata: Option[ConnectionMetadata] = None
                              ) extends Connection with JdbcExecution with SmartDataLakeLogger {

  private val supportedAuths = Seq(classOf[BasicAuthMode], classOf[OAuthMode])
  private var _snowparkSession: Option[Session] = None
  require(supportedAuths.contains(authMode.getClass), s"($id) ${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")

  // prepare JDBC catalog implementation
  val catalog: DefaultJdbcCatalog = new DefaultJdbcCatalog(this)
  // setup JDBC connection pool for metadata and ddl queries
  override val pool: GenericObjectPool[SqlConnection] = ConnectionPoolConfig().create(maxParallelConnections = 3, () => Utils.getJDBCConnection(getJdbcAuthOptions("")), initSql = None, autoCommit = false)
  // set autoCommit=false as recommended
  override val autoCommit: Boolean = false
  override val jdbcDialect: JdbcDialect = JdbcDialects.get("snowflake")

  def getProxyOptions: Map[String,String] = {
    proxy.map(p =>
      Map("useProxy" -> "true", "proxyHost" -> p.host, "proxyPort" -> p.port.toString)
        ++ p.user.map("proxyUser" -> _.resolve())
        ++ p.password.map("proxyPassword" -> _.resolve())
    ).getOrElse(Map())
  }

  def getJdbcAuthOptions(schema: String): Map[String, String] = {
    val connectionOptions = Map(
      "sfURL" -> url,
      "sfDatabase" -> database,
      "sfRole" -> role,
      "sfSchema" -> schema,
      "sfWarehouse" -> warehouse
    )
    val authOptions = authMode match {
      case m: BasicAuthMode => Map(
        "sfUser" -> m.userSecret.resolve(),
        "sfPassword" -> m.passwordSecret.resolve(),
      )
      case m: OAuthMode => Map(
        "sfAuthenticator" -> "oauth",
        "sfToken" -> m.getToken
      )
      case _ => throw new IllegalArgumentException(s"($id) No supported authMode given for Snowflake connection.")
    }
    connectionOptions ++ authOptions ++ getProxyOptions
  }

  def getSnowparkSession: Session = {
    _snowparkSession.synchronized {
      if (_snowparkSession.isEmpty) {
        _snowparkSession = Some(createSnowparkSession)
      }
    }
    _snowparkSession.get
  }

  private def createSnowparkSession: Session = {
    val commonOptions = Map(
      "URL" -> url,
      "ROLE" -> role,
      "WAREHOUSE" -> warehouse,
      "DB" -> database,
    )
    val authOptions = authMode match {
      case m: BasicAuthMode => Map(
        "USER" -> m.userSecret.resolve(),
        "PASSWORD" -> m.passwordSecret.resolve(),
      )
      case m: OAuthMode => Map(
        "AUTHENTICATOR" -> "oauth",
        "TOKEN" -> m.getToken
      )
      case _ => throw new IllegalArgumentException(s"($id) No supported authMode given for Snowflake connection.")
    }
    Session.builder.configs(commonOptions ++ authOptions ++ getProxyOptions).create
  }

  override def factory: FromConfigFactory[Connection] = SnowflakeConnection
}

object SnowflakeConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SnowflakeConnection = {
    extract[SnowflakeConnection](config)
  }
}
