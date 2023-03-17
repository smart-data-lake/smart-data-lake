/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.secrets

/**
 * A String wrapper for secrets. String values in the configuration will automatically be converted to [[StringOrSecret]].
 *
 * @param secret The value can be provided in plaintext or with the id of a secret provider and the
 *               name of the secret in the format {{{###<PROVIDERID>#<SECRETNAME>###}}},
 *               e.g. {{{###ENV#<ENV_VARIABLE_NAME>###}}} to get a secret from an environment variable.
 */
case class StringOrSecret(private val secret: String) {

  /**
   * Resolves the secret, if necessary by retrieving it from a secret provider.
   */
  def resolve(): String = {
    SecretsUtil.resolveSecret(secret)
  }
}