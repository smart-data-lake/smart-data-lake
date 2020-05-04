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
package io.smartdatalake.definitions

import io.smartdatalake.util.misc.CredentialsUtil

/**
 * AuthMode Parameters define keys on authentication mode maps
 * which can be imported and referenced in connections
 */
sealed trait AuthModeParameter { def name: String }

case object AuthToken extends AuthModeParameter{val name="token"}
case object AuthUser extends AuthModeParameter{val name="user"}
case object AuthPassword extends AuthModeParameter{val name="password"}

/**
 * Authentication modes define how an application authenticates itself
 * to a given data object/connection
 *
 */
sealed trait AuthenticationMode {
  /**
   * Each mode should at a minimum provide a method to decrypt the credentials
    * @return : Map of decrypted credentials
   */
  def simpleOpts: Map[String, String]
}

/**
 * The individual traits indicate which modes are supported by each connection type
 * and prevent passing unsupported credentials into a connection/DO
 */
sealed trait SplunkAuthMode extends AuthenticationMode { def splunkOpts: Map[String, String]}
sealed trait KafkaAuthMode extends AuthenticationMode { def kafkaOpts: Map[String, String]}
sealed trait HttpAuthMode extends AuthenticationMode { def headerOpts: Map[String, String]}

/**
 * Derive options for various connection types to connect by basic authentication
 */
case class BasicAuthMode(user: String, password: String)
  extends SplunkAuthMode
{
  val simpleOpts: Map[String, String] = Map(
    (AuthUser.name,CredentialsUtil.getCredentials(user)),
    (AuthPassword.name, CredentialsUtil.getCredentials(password))
  )
  val splunkOpts = simpleOpts
}

/**
 * Derive options for various connection types to connect by token
  */
case class TokenAuthMode(token: String)
  extends SplunkAuthMode
{
  val simpleOpts: Map[String, String] = Map((AuthToken.name,CredentialsUtil.getCredentials(token)))
  val splunkOpts: Map[String, String] = simpleOpts
}


