/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

import java.sql.ResultSet

import com.snowflake.snowpark.Session
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{AuthMode, BasicAuthMode}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import net.snowflake.spark.snowflake.Utils

/**
 * Connection information for Snowflake tables.
 * If authentication is needed, user and password must be provided.
 *
 * @param id        unique id of this connection
 * @param url       snowflake connection url
 * @param warehouse Snowflake namespace
 * @param database  Snowflake database
 * @param role      Snowflake role
 * @param authMode  optional authentication information: for now BasicAuthMode is supported.
 * @param metadata  Connection metadata
 */
case class SnowflakeTableConnection(override val id: ConnectionId,
                                    url: String,
                                    warehouse: String,
                                    database: String,
                                    role: String,
                                    authMode: AuthMode,
                                    override val metadata: Option[ConnectionMetadata] = None
                                   ) extends Connection with SmartDataLakeLogger {

  private val supportedAuths = Seq(classOf[BasicAuthMode])
  require(supportedAuths.contains(authMode.getClass), s"($id) ${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")

  def execSnowflakeStatement(sql: String, logging: Boolean = true): ResultSet = {
    if (logging) logger.info(s"($id) execSnowflakeStatement: $sql")
    Utils.runQuery(getSnowflakeOptions(""), sql)
  }

  def getSnowparkSession(schema: String): Session = {
    authMode match {
      case m: BasicAuthMode =>
        val builder = Session.builder.configs(Map(
          "URL" -> url,
          "USER" -> m.user,
          "PASSWORD" -> m.password,
          "ROLE" -> role,
          "WAREHOUSE" -> warehouse,
          "DB" -> database,
          "SCHEMA" -> schema
        ))
        builder.create
      case _ => throw new IllegalArgumentException(s"($id) No supported authMode given for Snowflake connection.")
    }
  }

  def getSnowflakeOptions(schema: String): Map[String, String] = {
    authMode match {
      case m: BasicAuthMode =>
        Map(
          "sfURL" -> url,
          "sfUser" -> m.user,
          "sfPassword" -> m.password,
          "sfDatabase" -> database,
          "sfRole" -> role,
          "sfSchema" -> schema,
          "sfWarehouse" -> warehouse
        )
      case _ => throw new IllegalArgumentException(s"($id) No supported authMode given for Snowflake connection.")
    }
  }

  override def factory: FromConfigFactory[Connection] = SnowflakeTableConnection
}

object SnowflakeTableConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SnowflakeTableConnection = {
    extract[SnowflakeTableConnection](config)
  }
}
