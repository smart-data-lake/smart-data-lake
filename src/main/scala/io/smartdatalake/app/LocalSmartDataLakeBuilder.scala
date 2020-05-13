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
package io.smartdatalake.app

import io.smartdatalake.config.ConfigurationException

/**
 * Smart Data Lake Builder application for local mode.
 *
 * Sets master to local[*] and deployMode to client by default.
 */
object LocalSmartDataLakeBuilder extends SmartDataLakeBuilder {

  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    logger.info(s"Starting Program $appType v$appVersion")

    // Set local defaults
    val config = initConfigFromEnvironment.copy(
      master = Some("local[*]"),
      deployMode = Some("client")
    )

    // Parse all command line arguments
    parseCommandLineArguments(args, config) match {
      case Some(config) =>

        // checking environment variables for local mode
        require( System.getenv("HADOOP_HOME")!=null, "Env variable HADOOP_HOME needs to be set in local mode!" )
        require( !config.master.contains("yarn") || System.getenv("SPARK_HOME")!=null, "Env variable SPARK_HOME needs to be set in local mode with master=yarn!" )

        // start
        val stats = run(config)
        logger.info(s"$appType finished successfully: $stats")
      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}
