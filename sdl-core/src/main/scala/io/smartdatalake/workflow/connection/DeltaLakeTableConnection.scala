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
 * Connection information for DeltaLake tables.
 *
 * It centralizes catalog, database and the base directory of the table files, so that DeltaLakeTableDataObjects
 * referencing it through `connectionId` do not need to repeat them. Catalog and db of the DataObjects `table` are
 * taken from this connection, and the DataObjects `path` is resolved relative to `pathPrefix`. This keeps
 * environment specific storage locations out of the DataObject definitions.
 *
 * Example:
 * {{{
 * connections {
 *   deltalake-int {
 *     type = DeltaLakeTableConnection
 *     db = "integration"
 *     pathPrefix = "~{env.basedir}/deltalake"
 *   }
 * }
 * }}}
 *
 * @note the database given in `db` must already exist; SDLB does not create it.
 *
 * @param id unique id of this connection
 * @param catalog optional catalog to be used for this connection
 * @param db hive db
 * @param pathPrefix schema, authority and base path for tables directory on hadoop
 * @param checkDeltaLakeSparkOptions if true (default) it is verified on prepare that the Spark session registers
 *                                   `io.delta.sql.DeltaSparkSessionExtension` in `spark.sql.extensions`.
 *                                   Set to false to skip this check. The check is skipped automatically on Databricks.
 */
case class DeltaLakeTableConnection(override val id: ConnectionId,
                                    catalog: Option[String] = None,
                                    db: String,
                                    pathPrefix: String,
                                    checkDeltaLakeSparkOptions: Boolean = true,
                                    override val metadata: Option[ConnectionMetadata] = None
                               ) extends Connection

object DeltaLakeTableConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeltaLakeTableConnection = {
    extract[DeltaLakeTableConnection](config)
  }
}
