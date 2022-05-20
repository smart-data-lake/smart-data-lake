/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.config

object ConfigUtil {

  /**
   * Split special configuration values defining a providerId into the providerId and the value to be passed to the provider.
   * The providerId is included in the configuration value as prefix terminated by '#'.
   * A default providerId can be given if no providerId exists.
   * If no default providerId is defined and the configuration value does not include a providerId, an exception is thrown.
   */
  private[smartdatalake] def parseProviderConfigValue(config: String, defaultProviderId: Option[String] = None): (String, String) = {
    config match {
      case configValuePattern(providerId, value) => (providerId, value)
      case _ if defaultProviderId.isDefined => (defaultProviderId.get, config)
      case _ => throw new ConfigurationException(s"Config $config is invalid, make sure it's of the form PROVIDERID#VALUE")
    }
  }
  private[smartdatalake] val configValuePattern = "([^#]*)#(.*)".r
}
