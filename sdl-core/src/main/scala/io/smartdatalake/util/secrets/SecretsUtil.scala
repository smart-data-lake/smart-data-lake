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

import io.smartdatalake.config.ConfigurationException
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

  /**
   * Parses the secret config value to know what provider to use and the secret name to query.
   * @param secretConfigValue contains the id of the provider and the name of the secret with format <PROVIDERID>#<SECRETNAME>,
   *                          e.g. ENV#<ENV_VARIABLE_NAME> to get a secret from an environment variable.
   */
  def getSecret(secretConfigValue: String): String = {
    logger.debug(s"getting secret $secretConfigValue")
    val (providerId, secretName) = parseConfigValue(secretConfigValue)
    val provider = providers.getOrElse(providerId, throw new ConfigurationException(s"There is no registered secret provider with id ${providerId}"))
    provider.getSecret(secretName)
  }

  private val configValuePattern = "([^#]*)#(.*)".r
  private def parseConfigValue(credentialsConfig: String): (String, String) = {
    logger.debug(s"Parsing variable ${credentialsConfig}")
    credentialsConfig match {
      case configValuePattern(providerId, secretName) => (providerId, secretName)
      case _ => throw new ConfigurationException(s"Secret config value $credentialsConfig is invalid, make sure it's of the form PROVIDERID#SECRETNAME")
    }
  }
}