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
package io.smartdatalake.workflow.connection.jdbc

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.ConnectionPoolConfig
import io.smartdatalake.workflow.connection.authMode.AuthMode
import io.smartdatalake.workflow.connection.{Connection, ConnectionMetadata}

/**
 * Former name of [[JdbcConnection]], kept for backward compatibility of existing configurations.
 * It behaves exactly like [[JdbcConnection]], see there for a description and the available parameters.
 *
 * The name was too narrow: the connection is not only used by JdbcTableDataObject, but also by JdbcViewDataObject,
 * and as engine connection of the SQL engine.
 *
 * @deprecated Renamed to [[JdbcConnection]] in version 3.0.0. Change `type = JdbcTableConnection` to
 *             `type = JdbcConnection` in your configuration, all parameters stay the same.
 */
@Deprecated
@deprecated("Use JdbcConnection instead", "3.0.0")
case class JdbcTableConnection(
    override val id: ConnectionId,
    url: String,
    driver: String,
    authMode: Option[AuthMode] = None,
    db: Option[String] = None,
    maxParallelConnections: Int = 3,
    connectionInitSql: Option[String] = None,
    directTableOverwrite: Boolean = false,
    connectionPool: ConnectionPoolConfig = ConnectionPoolConfig(),
    dialect: Option[String] = None,
    override val metadata: Option[ConnectionMetadata] = None
) extends JdbcConnectionImpl {

  logger.warn(s"($id) JdbcTableConnection is deprecated, use JdbcConnection instead. All parameters stay the same.")
}

object JdbcTableConnection extends FromConfigFactory[Connection] {

  @scala.annotation.nowarn("cat=deprecation")
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JdbcTableConnection =
    extract[JdbcTableConnection](config)

  /**
   * @deprecated Use [[JdbcConnection.dialectFromUrl]] instead.
   */
  @deprecated("Use JdbcConnection.dialectFromUrl instead", "3.0.0")
  def dialectFromUrl(url: String): Option[String] = JdbcConnection.dialectFromUrl(url)
}
