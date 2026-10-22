/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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

/**
 * Default Smart Data Lake Command Line Application.
 * Note: When running SDLB with Spark, this entrypoint assumes that it is running in an environment where Spark Config
 * such as master and deploy-mode are already set. This is for example the case with Databricks.
 * For other environments use SparkSmartDataLakeBuilder
 */
object DefaultSmartDataLakeBuilder extends SmartDataLakeBuilder {

  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    logProgramStart()

    parse(args.toList) match {
      case Some(config) =>
        val stats = run(config)
        logStats(stats)
      case None =>
        throwOParserError()
    }
  }
}
