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
package io.smartdatalake.workflow.connection.authMode

import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.secrets.StringOrSecret

import java.util.Base64

/**
 * Authenticate using basic user/pwd authentication.
 *
 * For http connection this will create a basic authentication header.
 */
case class BasicAuthMode(
    private val user: StringOrSecret,
    private val password: StringOrSecret
) extends HttpAuthMode {

  def userSecret: StringOrSecret = user

  def passwordSecret: StringOrSecret = password

  def basicAuthValue(user: String, password: String): String = {
    val c = new String(Base64.getEncoder.encode(s"$user:$password".getBytes("utf-8")), "utf-8")
    s"Basic $c"
  }
  override def getHeaders: Map[String, String] = Map("Authorization" -> basicAuthValue(userSecret.resolve(), passwordSecret.resolve()))

  override def factory: FromConfigFactory[HttpAuthMode] = BasicAuthMode

}

object BasicAuthMode extends FromConfigFactory[HttpAuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): BasicAuthMode =
    extract[BasicAuthMode](config)
}
