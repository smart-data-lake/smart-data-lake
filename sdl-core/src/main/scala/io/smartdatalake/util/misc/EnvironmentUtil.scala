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

package io.smartdatalake.util.misc

import io.smartdatalake.definitions.Environment
import org.apache.spark.sql.SparkSession

object EnvironmentUtil {

  /**
   * check if running on windows os
   */
  def isWindowsOS: Boolean = {
    sys.env.get("OS").exists(_.toLowerCase.startsWith("windows"))
  }

  /**
   * check if Spark AQE (Spark 3.0) is enabled
   */
  def isSparkAdaptiveQueryExecEnabled(implicit session: SparkSession): Boolean = {
    session.version.takeWhile(_ != '.').toInt >= 3 && session.conf.get("spark.sql.adaptive.enabled").toBoolean
  }

  /**
   * get environment parameter value from Java system properties, environment variables or global.environment configuration file section.
   * To lookup java system properties the key is prefixed with "sdl.".
   * To lookup environment variables the key is prefixed with "SDL_" and camelcase notation is converted to uppercase notation separated by "_".
   * Java system properties have precedence over environment variables.
   *
   * @param key the name of the parameter in camelCase notation, starting with a lowercase letter.
   */
  def getSdlParameter(key: String): Option[String] = {
    sys.props.get("sdl."+key)
      .orElse(sys.env.get("SDL_"+camelCaseToUpper(key)))
      .orElse(Option(Environment.globalConfig).flatMap(_.environment.get(key)))
  }

  private[smartdatalake] def camelCaseToUpper(key: String): String = key.replaceAll("([A-Z])", "_$1").toUpperCase

}
