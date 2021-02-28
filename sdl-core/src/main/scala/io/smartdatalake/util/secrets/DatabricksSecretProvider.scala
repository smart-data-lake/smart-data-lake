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

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import io.smartdatalake.config.ConfigurationException

/**
 * Define a secret provider for a specific Databricks secret scope.
 * @param scope scope to read secrets from databricks
 */
class DatabricksSecretProvider(scope: String) extends SecretProvider {
  def this(options: Map[String, String]) {
    this(
      options.getOrElse("file", throw new ConfigurationException(s"Cannot create FileSecretProvider, option 'file' missing."))
    )
  }
  override def getSecret(name: String): String =
    try {
      val secret = dbutils.secrets.get(scope = scope, key = name)
      if (secret.isEmpty) {
        throw new ConfigurationException(s"Databricks secret $name in scope $scope is an empty string")
      } else secret
    } catch {
      case e: Exception => throw new ConfigurationException(s"Databricks secret $name in scope $scope cannot be read. Error ${e.getClass.getSimpleName}: ${e.getMessage}", None, e)
    }
}