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

package io.smartdatalake.util.azure

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.security.keyvault.secrets.SecretClientBuilder
import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.secrets.SecretProvider

/**
 * Secret provider to read secrets from AzureKeyVault
 *
 * Usage:
 * global = {
 *   secretProviders = {
 *     AZKV = {
 *       className: io.smartdatalake.utils.azure.AzureKeyVaultSecretProvider
 *       options = {
 *         keyVaultName: <azure-key-vault-name>
 *       }
 *     }
 *   }
 * }
 *
 * @param keyVaultName Name of the key vault
 * @param azureCredentialBuilderClass optional AzureCredentialBuilder class. Default is to use DefaultAzureCredentialBuilder.
 */
class AzureKeyVaultSecretProvider(keyVaultName: String, azureCredentialBuilderClass: Option[String] = None) extends SecretProvider {
  // alternative constructor
  def this(options: Map[String, String]) {
    this(
      options.getOrElse("keyVaultName", throw new ConfigurationException(s"Cannot create AzureKeyVaultSecretProvider, option 'keyVaultName' missing.")),
      options.get("azureCredentialBuilderClass")
    )
  }
  // initializiation
  private val keyVaultUri = s"https://$keyVaultName.vault.azure.net"
  private val azureCredentialBuilder =
    azureCredentialBuilderClass.map(CustomCodeUtil.getClassInstanceByName).getOrElse(new DefaultAzureCredentialBuilder)
  private val credential = azureCredentialBuilder.build
  private val secretClient = new SecretClientBuilder().vaultUrl(keyVaultUri).credential(credential).buildClient
  // method to get secrets from azure key vault
  override def getSecret(name: String): String =
    try {
      val secret = secretClient.getSecret(name).getValue
      if (secret.isEmpty) {
        throw new ConfigurationException(s"AzureKeyVault secret $name in keyVault $keyVaultName is an empty string")
      } else secret
    } catch {
      case e: Exception => throw new ConfigurationException(s"AzureKeyVault secret $name in keyVault $keyVaultName cannot be read. Error ${e.getClass.getSimpleName}: ${e.getMessage}", None, e)
    }

}