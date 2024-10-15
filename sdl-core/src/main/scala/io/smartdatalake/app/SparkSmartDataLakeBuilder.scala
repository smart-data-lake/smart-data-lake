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
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.EnvironmentUtil
import scopt.OParser

import java.io.File

/**
 * Smart Data Lake Builder application for running SDL with Spark.
 * Allows to explicitly override master and deploy-mode settings of Spark using the command-line.
 * This entrypoint should be used when there is no existing Spark-Settings on the environment where SDLB is running,
 * for example for running SDLB locally on your laptop with Spark.
 */
object SparkSmartDataLakeBuilder extends SmartDataLakeBuilder {
  val localParser: OParser[_, SmartDataLakeBuilderConfig] = {
    val builder = OParser.builder[SmartDataLakeBuilderConfig]
    import builder._
    OParser.sequence(
      parserGeneric(),
      opt[String]('m', "master")
        .action((arg, config) => config.copy(master = Some(arg)))
        .text("The Spark master URL passed to SparkContext (default=local[*], yarn, spark://HOST:PORT, mesos://HOST:PORT, k8s://HOST:PORT).")
      .required(),
      opt[String]('x', "deploy-mode")
        .action((arg, config) => config.copy(deployMode = Some(arg)))
        .text("The Spark deploy mode passed to SparkContext (default=client, cluster).")
        .required()
    )
  }


  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    logger.info(s"Starting Program $appType $appVersion")

    // Parse all command line arguments
    OParser.parse(localParser, args, SmartDataLakeBuilderConfig()) match {
      case Some(config) =>

        // checking environment variables for local mode
        require(!EnvironmentUtil.isWindowsOS || System.getenv("HADOOP_HOME") != null, "Env variable HADOOP_HOME needs to be set in local mode in Windows!")
        require(!config.master.contains("yarn") || System.getenv("SPARK_HOME") != null, "Env variable SPARK_HOME needs to be set in local mode with master=yarn!")

        val stats = run(config)
          .toSeq.sortBy(_._1).map(x => x._1 + "=" + x._2).mkString(" ") // convert stats to string
        logger.info(s"$appType finished successfully: $stats")
      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}
