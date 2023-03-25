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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{AuthMode, BasicAuthMode, PublicKeyAuthMode}
import io.smartdatalake.util.filetransfer.SshUtil
import io.smartdatalake.util.misc.TryWithResourcePool
import net.schmizz.sshj.sftp.SFTPClient
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

import java.net.{InetSocketAddress, Proxy}

/**
 * SFTP Connection information
 *
 * @param id unique id of this connection
 * @param host sftp host
 * @param port port of sftp service, default is 22
 * @param authMode authentication information: for now BasicAuthMode and PublicKeyAuthMode are supported.
 * @param proxy optional proxy configuration
 * @param ignoreHostKeyVerification do not validate host key if true, default is false
 * @param maxParallelConnections number of parallel sftp connections created by an instance of this connection
 * @param connectionPoolMaxIdleTimeSec timeout to close unused connections in the pool
 * @param metadata
 */
case class SFtpFileRefConnection(override val id: ConnectionId,
                                 host: String,
                                 port: Int = 22,
                                 authMode: AuthMode,
                                 proxy: Option[JavaNetProxyConfig] = None,
                                 ignoreHostKeyVerification: Boolean = false,
                                 maxParallelConnections: Int = 1,
                                 connectionPoolMaxIdleTimeSec: Int = 3,
                                 override val metadata: Option[ConnectionMetadata] = None
                                 ) extends Connection {


  // Allow only supported authentication modes
  private val supportedAuths = Seq(classOf[BasicAuthMode], classOf[PublicKeyAuthMode])
  require(supportedAuths.contains(authMode.getClass), s"${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")

  def execWithSFtpClient[A]( func: SFTPClient => A ): A = {
    TryWithResourcePool.exec(pool){
      sftp => func(sftp)
    }
  }

  def test(): Unit = {
    TryWithResourcePool.exec(pool){ sftp => Unit } // no operation
  }

  // setup connection pool
  val pool = new GenericObjectPool[SFTPClient](new SFtpClientPoolFactory)
  pool.setMaxTotal(maxParallelConnections)
  pool.setMaxIdle(1) // keep max one idle sftp connection
  pool.setMinEvictableIdleTimeMillis(connectionPoolMaxIdleTimeSec * 1000) // timeout to close sftp connection if not in use
  private class SFtpClientPoolFactory extends BasePooledObjectFactory[SFTPClient] {
    override def create(): SFTPClient = {
      authMode match {
        case m: BasicAuthMode => SshUtil.connectWithUserPw(host, port,
          m.userSecret.resolve(), m.passwordSecret.resolve(), proxy.map(_.instance), ignoreHostKeyVerification).newSFTPClient()
        case m: PublicKeyAuthMode => SshUtil.connectWithPublicKey(host, port, m.userSecret.resolve(), proxy.map(_.instance), ignoreHostKeyVerification).newSFTPClient()
        case _ => throw new IllegalArgumentException(s"${authMode.getClass.getSimpleName} not supported.")
      }
    }
    override def wrap(sftp: SFTPClient): PooledObject[SFTPClient] = new DefaultPooledObject(sftp)
    override def destroyObject(p: PooledObject[SFTPClient]): Unit =
      p.getObject.close()
  }

  override def factory: FromConfigFactory[Connection] = SFtpFileRefConnection
}

object SFtpFileRefConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SFtpFileRefConnection = {
    extract[SFtpFileRefConnection](config)
  }
}

/**
 * Proxy configuration to create java.net.Proxy instance.
 * @param host proxy host
 * @param port proxy port
 * @param proxyType Type of proxy: HTTP or SOCKS. Default is HTTP.
 */
case class JavaNetProxyConfig(host: String, port: Int, proxyType: Proxy.Type = Proxy.Type.HTTP) {
  val instance: Proxy = new Proxy(proxyType, new InetSocketAddress(host, port))
}





