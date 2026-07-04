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
package io.smartdatalake.testutils

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.dataset.Equality
import org.apache.sshd.common.file.nativefs.NativeFileSystemFactory
import org.apache.sshd.server.SshServer
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.apache.sshd.server.session.ServerSession
import org.apache.sshd.server.subsystem.SubsystemFactory
import org.apache.sshd.sftp.server.SftpSubsystemFactory

import java.nio.file.Files
import scala.jdk.CollectionConverters._

/**
 * Utility methods for testing.
 */
object SshTestUtil extends SmartDataLakeLogger with Equality {

  def setupSSHServer(port: Int, usr: String, pwd: String): SshServer = {
    val sshd = SshServer.setUpDefaultServer()
    sshd.setFileSystemFactory(new NativeFileSystemFactory())
    sshd.setPort(port)
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(Files.createTempDirectory("sshd").resolve("hostkey.ser")))
    sshd.setSubsystemFactories(List(new SftpSubsystemFactory().asInstanceOf[SubsystemFactory]).asJava)
    sshd.setPasswordAuthenticator(new PasswordAuthenticator() {
      override def authenticate(user: String, password: String, session: ServerSession): Boolean = user == usr && password == pwd
    })
    sshd.start()
    // Thread.sleep(1000000)
    // return
    sshd
  }

}
