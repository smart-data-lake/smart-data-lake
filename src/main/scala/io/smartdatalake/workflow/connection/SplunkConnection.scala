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
package io.smartdatalake.workflow.connection

import com.splunk.{SSLSecurityProtocol, Service, ServiceArgs}
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions._

/**
 * Connection information for splunk queries
 *
 * @param id unique id of this connection
 * @param host
 * @param port
 * @param authMode
 * @param metadata
 */
case class SplunkConnection( override val id: ConnectionId,
                             host: String,
                             port: Int,
                             authMode: SplunkAuthMode,
                             override val metadata: Option[ConnectionMetadata] = None
                           ) extends Connection with SplunkConnectionService {


  override def connectToSplunk: Service = {

    val connectionArgs = new ServiceArgs
    connectionArgs.setHost(host)
    connectionArgs.setPort(port)
    connectionArgs.setSSLSecurityProtocol(SSLSecurityProtocol.TLSv1_2)

    authMode match {
      case BasicAuthMode(u,p) => {
        connectionArgs.setUsername(authMode.splunkOpts(AuthUser.name))
        connectionArgs.setPassword(authMode.splunkOpts(AuthPassword.name))
      }
      case TokenAuthMode(t) => {
        connectionArgs
          .setToken(authMode.splunkOpts(AuthToken.name))
      }
    }

    Service.connect(connectionArgs)
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[Connection] = SplunkConnection
}

trait SplunkConnectionService {
  def connectToSplunk: Service
}

object SplunkConnection extends FromConfigFactory[Connection] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): SplunkConnection = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[SplunkConnection].value
  }
}


