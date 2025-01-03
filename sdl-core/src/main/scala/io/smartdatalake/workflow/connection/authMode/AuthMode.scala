/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config._

/**
 * Authentication modes define how an application authenticates itself
 * to a given data object/connection
 *
 * You need to define one of the AuthModes (subclasses) as type, i.e.
 * {{{
 * authMode {
 *   type = BasicAuthMode
 *   user = myUser
 *   password = myPassword
 * }
 * }}}
 *
 */
trait AuthMode extends ParsableFromConfig[AuthMode] with ConfigHolder {
  /**
   * This method is called in prepare phase through the data object.
   * It allows to check configuration and setup variables.
   */
  private[smartdatalake] def prepare(): Unit = ()

  /**
   * This method is called after exec phase through the postExec method of the data object.
   * It allows to release any resources that were reserved.
   */
  private[smartdatalake] def close(): Unit = ()
}

/**
 * Interface to generalize authentication for HTTP requests
 */
trait HttpHeaderAuth {
  /**
   * Return additional headers to add/overwrite in http request.
   */
  def getHeaders: Map[String, String]
}

/**
 * Authentication modes define how an application authenticates itself
 * to a given data object/connection
 *
 * HttpAuthMode is a subset of AuthMode's to authenticate for Http based connections.
 *
 * You need to define one of the AuthModes (subclasses) as type, i.e.
 * {{{
 * authMode {
 *   type = BasicAuthMode
 *   user = myUser
 *   password = myPassword
 * }
 * }}}
 *
 */
trait HttpAuthMode extends AuthMode with ParsableFromConfig[HttpAuthMode] with HttpHeaderAuth