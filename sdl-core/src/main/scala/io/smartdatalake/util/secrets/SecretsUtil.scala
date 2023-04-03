/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.{ConfigUtil, ConfigurationException}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.annotation.DeveloperApi

import scala.collection.mutable

@DeveloperApi
object SecretsUtil extends SmartDataLakeLogger {

  private val providers = mutable.Map[String, SecretProvider]()
  def registerProvider(id: String, provider: SecretProvider): Unit = {
    logger.debug(s"Register secret provider $id")
    providers.put(id, provider)
  }

  // register default secret providers
  registerProvider("CLEAR", ClearTextSecretProvider)
  registerProvider("ENV", EnvironmentVariableSecretProvider)
  registerProvider("FILE", GenericFileSecretProvider)

  private val SECRET_WITH_PROVIDER_REGEX = "^###([^#]*)#(.*)###$".r

  private[smartdatalake] def convertSecretVariableToStringOrSecret(secretVariable: String): StringOrSecret = {
    StringOrSecret(s"###$secretVariable###")
  }

  /**
   * Parses the secret to retrieve the secret value from the secret provider if necessary.
   * @param secret is either the secret in plaintext or it contains the id of the provider and the
   *                       name of the secret in the format ###<PROVIDERID>#<SECRETNAME>###,
   *                       e.g. ###ENV#<ENV_VARIABLE_NAME>### to get a secret from an environment variable.
   */
  private[secrets] def resolveSecret(secret: String): String = {
    secret match {
      case SECRET_WITH_PROVIDER_REGEX(providerId, secretName) =>
        logger.debug(s"getting secret ${secret}")
        val provider = providers.getOrElse(providerId, throw new ConfigurationException(s"There is no registered secret provider with id ${providerId}"))
        provider.getSecret(secretName)
      case _ => secret
    }
  }
}