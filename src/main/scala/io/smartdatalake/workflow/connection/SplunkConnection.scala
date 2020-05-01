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
import io.smartdatalake.definitions.{AuthenticationMode, TokenAuthMode, UserPassAuthMode}
import io.smartdatalake.util.misc.CredentialsUtil

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
                             authMode: AuthenticationMode,
                             override val metadata: Option[ConnectionMetadata] = None
                           ) extends Connection with SplunkConnectionService {


  override def connectToSplunk: Service = {

    val basicConnectionArgs = new ServiceArgs
    basicConnectionArgs.setHost(host)
    basicConnectionArgs.setPort(port)
    basicConnectionArgs.setSSLSecurityProtocol(SSLSecurityProtocol.TLSv1_2)

    val connectionArgs = authMode match {
      case UserPassAuthMode(u, p) => addUsernamePass(basicConnectionArgs, u, p)
      case TokenAuthMode(t) => addToken(basicConnectionArgs,t)
    }

    Service.connect(connectionArgs)
  }

  /**
   * Add username/password to the connection parameters for authentication
   *
   * @param connectionArgs : basic connection details for the Splunk connector
   * @param userVariable : A variable referencing the username. Variable convention as per passwords.
   * @param passwordVariable : A variable referencing the password. Variable convention as per passwords.
   * @return : A service configuration including credentials
   */
  private def addUsernamePass(connectionArgs: ServiceArgs, userVariable: String, passwordVariable: String): ServiceArgs = {

    val splunkConnectionUser = CredentialsUtil.getCredentials(userVariable)
    val splunkConnectionPassword = CredentialsUtil.getCredentials(passwordVariable)

    connectionArgs.setUsername(splunkConnectionUser)
    connectionArgs.setPassword(splunkConnectionPassword)

    connectionArgs

  }

  /**
   * Add a token to the connection parameters for authentication
   *
   * @param connectionArgs : basic connection details for the Splunk connector
   * @param tokenVariable : A variable referencing the user token. Variable convention as per passwords.
   * @return : A service configuration including credentials
   */
  private def addToken(connectionArgs: ServiceArgs, tokenVariable: String): ServiceArgs = {

    val splunkToken = CredentialsUtil.getCredentials(tokenVariable)
    connectionArgs.setPassword(splunkToken)

    connectionArgs

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


