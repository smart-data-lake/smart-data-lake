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
import org.apache.spark.annotation.DeveloperApi

/**
 * Configuration to register a SecretProvider.
 *
 * @param className fully qualified class name of class implementing SecretProvider interface. The class needs a constructor with parameter "options: Map[String,String]".
 * @param options Options are passed to SecretProvider apply method.
 */
case class SecretProviderConfig(className: String, options: Map[String,String] = Map()) {
  // instantiate SecretProvider
  private[smartdatalake] val provider: SecretProvider = try {
    val clazz = Class.forName(className)
    val constructor = clazz.getConstructor(classOf[Map[String,String]])
    constructor.newInstance(options).asInstanceOf[SecretProvider]
  } catch {
    case e: NoSuchMethodException => throw ConfigurationException(s"""SecretProvider class $className needs constructor with parameter "options: Map[String,String]": ${e.getMessage}""", Some("globalConfig.secretProviders"), e)
    case e: Exception => throw ConfigurationException(s"Cannot instantiate SecretProviderCreator class $className: ${e.getMessage}", Some("globalConfig.secretProviders"), e)
  }
}

/**
 * Interface to by implement by a SecretProvider.
 */
@DeveloperApi
trait SecretProvider {
  def getSecret(name: String): String
}