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
package io.smartdatalake.config

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.workflow.connection.{Connection, ConnectionMetadata}

/**
 * A dummy [[Connection]] for unit tests.
 *
 * @param id The unique identified of this object.
 */
case class TestConnection( override val id: ConnectionId,
                           override val metadata: Option[ConnectionMetadata] = None)
                         ( implicit val instanceRegistry: InstanceRegistry)
extends Connection {

  override def factory: FromConfigFactory[Connection] = TestConnection
}

object TestConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): TestConnection = {
    extract[TestConnection](config)
  }
}