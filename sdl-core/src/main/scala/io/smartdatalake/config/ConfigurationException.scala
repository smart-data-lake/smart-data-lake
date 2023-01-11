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
package io.smartdatalake.config

/**
 * An exception to indicate a missing or problematic SDL configuration.
 *
 * @param configurationPath Some configuration path associated to this exception or None.
 */
private[smartdatalake] case class ConfigurationException(message: String, configurationPath: Option[String] = None, throwable: Throwable = null) extends RuntimeException(message, throwable) {

  /**
   * Get the path of the configuration property (if any) that is associated to this exception.
   *
   * @return Some configuration path associated to this exception or None.
   */
  def getConfigurationPath: Option[String] = configurationPath
}

object ConfigurationException {
  def fromException(message: String, path: String, t: Throwable): ConfigurationException = {
    ConfigurationException(s"$message - ${t.getClass.getSimpleName}: ${t.getMessage}", Some(path), t)
  }
}
