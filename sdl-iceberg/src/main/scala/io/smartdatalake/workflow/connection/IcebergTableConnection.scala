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

/**
 * Connection information for Iceberg tables.
 *
 * It centralizes catalog, database and the base directory of the table files, so that IcebergTableDataObjects
 * referencing it through `connectionId` do not need to repeat them. Catalog and db of the DataObjects `table` are
 * taken from this connection, and the DataObjects `path` is resolved relative to `pathPrefix`. This keeps
 * environment specific storage locations out of the DataObject definitions.
 *
 * Example:
 * {{{
 * connections {
 *   iceberg-int {
 *     type = IcebergTableConnection
 *     db = "integration"
 *     pathPrefix = "~{env.basedir}/iceberg"
 *     addFilesParallelism = 8
 *   }
 * }
 * }}}
 *
 * @note the database given in `db` must already exist; SDLB does not create it.
 *
 * @param id unique id of this connection
 * @param catalog optional catalog to be used for this connection
 * @param db database to be used for this connection
 * @param pathPrefix schema, authority and base path for tables directory on hadoop
 * @param checkIcebergSparkOptions if true check if IcebergSparkSessionExtensions is registered through spark.sql.extensions property.
 * Default is true.
 * @param addFilesParallelism Number of thread to use for file reading when migrating table from parquet to iceberg using procedure 'system.add_files'.
 * Icberg Default value is 1, but should be increased for acceptable performance with larger tables.
 */
case class IcebergTableConnection(override val id: ConnectionId,
                                  catalog: Option[String] = None,
                                  db: String,
                                  pathPrefix: String,
                                  checkIcebergSparkOptions: Boolean = true,
                                  addFilesParallelism: Option[Int] = None,
                                  override val metadata: Option[ConnectionMetadata] = None
                               ) extends Connection {

  override def factory: FromConfigFactory[Connection] = IcebergTableConnection
}

object IcebergTableConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): IcebergTableConnection = {
    extract[IcebergTableConnection](config)
  }
}
