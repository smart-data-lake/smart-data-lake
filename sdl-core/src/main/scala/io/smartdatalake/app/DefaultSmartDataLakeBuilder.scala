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

import io.smartdatalake.app.DatabricksSmartDataLakeBuilder.parser
import io.smartdatalake.config.ConfigurationException
import scopt.OParser

/**
 * Default Smart Data Lake Command Line Application.
 *
 * Implementation Note: This must be a class and not an object in order to be found by reflection in DatabricksSmartDataLakeBuilder
 */
class DefaultSmartDataLakeBuilder extends SmartDataLakeBuilder {

  def parseAndRun(args: Array[String], ignoreOverrideJars: Boolean = false): Unit = {
    logger.info(s"Starting Program $appType $appVersion")

    OParser.parse(parser, args, SmartDataLakeBuilderConfig()) match {
      case Some (config) =>
        assert(config.overrideJars.isEmpty || ignoreOverrideJars, "Option override-jars is not supported by DefaultSmartDataLakeBuilder. Use DatabricksSmartDataLakeBuilder for this option.")
        val stats = run(config)
          .toSeq.sortBy(_._1).map(x => x._1.toString + "=" + x._2).mkString(" ") // convert stats to string
        logger.info(s"$appType finished successfully: $stats")
      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}

object DefaultSmartDataLakeBuilder {

  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    val app = new DefaultSmartDataLakeBuilder
    app.parseAndRun(args)
  }
}
