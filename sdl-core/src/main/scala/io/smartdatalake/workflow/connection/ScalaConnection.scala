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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry, SdlConfigObject}
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Engine connection for the plain Scala execution engine.
 *
 * An [[EngineConnection]] declares which DataFrame engine an Action uses; this one selects the lightweight,
 * Spark-free engine working on `ScalaSubFeed`. Pick it over `SparkClassicConnection` for small pipelines and
 * unit tests where starting a Spark session is unnecessary overhead. Actions select an engine connection with
 * their `engineConnectionId` attribute; if none is given the connection with id `default-engine` is used
 * (configurable through the SDLB parameter `defaultEngineConnectionId`).
 *
 * Example:
 * {{{
 * connections {
 *   default-engine {
 *     type = ScalaConnection
 *   }
 * }
 * }}}
 *
 * @param id unique id of this connection, e.g. `default-engine`
 * @param metadata additional metadata for this connection (name, description, layer, ...)
 */
case class ScalaConnection (
                             id: SdlConfigObject.ConnectionId,
                             metadata: Option[ConnectionMetadata] = None
                           ) extends Connection with EngineConnection {

  override def subFeedType: universe.Type = typeOf[ScalaSubFeed]

  override def factory: FromConfigFactory[Connection] = ScalaConnection
}

object ScalaConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaConnection = {
    extract[ScalaConnection](config)
  }
}