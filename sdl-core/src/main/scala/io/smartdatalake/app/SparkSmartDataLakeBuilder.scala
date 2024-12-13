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

import io.smartdatalake.util.misc.EnvironmentUtil
import scopt.OParser

/**
 * Smart Data Lake Builder application for running SDL with Spark.
 * Allows to explicitly override master and deploy-mode settings of Spark using the command-line.
 * This entrypoint should be used when there is no existing Spark-Session running on the environment where SDLB is started,
 * for example for starting SDLB locally on your laptop with Spark.
 */
object SparkSmartDataLakeBuilder extends SmartDataLakeBuilder {

  private val sparkParser: OParser[_, SmartDataLakeBuilderConfig] = {
    val builder = OParser.builder[SmartDataLakeBuilderConfig]
    import builder._
    OParser.sequence(
      parserGeneric(),
      opt[String]('m', "master")
        .action((arg, config) => config.copy(master = Some(arg)))
        .text("The Spark master URL passed to SparkContext (default=local[*], yarn, spark://HOST:PORT, mesos://HOST:PORT, k8s://HOST:PORT)."),
      opt[String]('x', "deploy-mode")
        .action((arg, config) => config.copy(deployMode = Some(arg)))
        .text("The Spark deploy mode passed to SparkContext (default=client, cluster).")
    )
  }

  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    logProgramStart()

    // Parse all command line arguments
    OParser.parse(sparkParser, args, SmartDataLakeBuilderConfig()) match {
      case Some(config) =>

        // checking environment variables for local mode
        require(!EnvironmentUtil.isWindowsOS || System.getenv("HADOOP_HOME") != null, "Env variable HADOOP_HOME needs to be set in local mode in Windows!")
        require(!config.master.contains("yarn") || System.getenv("SPARK_HOME") != null, "Env variable SPARK_HOME needs to be set in local mode with master=yarn!")

        // run
        val stats = run(config)
        logStats(stats)

      case None =>
        throwOParserError()
    }
  }
}
