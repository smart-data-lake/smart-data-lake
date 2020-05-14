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

/**
 * Authentication modes define how an application authenticates itself
 * to a given data object/connection
 *
 */
sealed trait AuthMode

/**
 * Derive options for various connection types to connect by basic authentication
 */
case class BasicAuthMode(user: String, password: String) extends AuthMode

/**
 * Derive options for various connection types to connect by token
  */
case class TokenAuthMode(token: String) extends AuthMode


