/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019 ELCA Informatique SA (<https://www.elca.ch>)
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

import org.apache.spark.SparkContext

/**
 * Provides utility functions for logging with Spark.
 */
private[smartdatalake] object LogUtil {

  /**
   * Sets the util log level.
   *
   * Uses the system property "loglevel" if it is set.
   * If not, the default level "INFO" is being used.
   *
   * @param sparkContext [[SparkContext]] on which the loglevel should bet set
   */
  def setLogLevel(sparkContext: SparkContext) = {
    if (System.getProperty("logLevel") != null) {
      sparkContext.setLogLevel(System.getProperty("logLevel"))
    }
  }

}
