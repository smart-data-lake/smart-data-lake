/*
 * Smart Data Lake - Build your data lake the smart way.
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

package io.smartdatalake.util.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object GetSession {
  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  var _loggEnvDone: Boolean = false

  def loggEnv(implicit session: SparkSession, logger: Logger): Unit = {
    if (!_loggEnvDone) {
      val sparkConfSettings = List("spark.driver.host", "spark.driver.port", "spark.driver.cores",
        "spark.driver.maxResultSize", "spark.driver.memory",
        "spark.dynamicAllocation.enabled", "spark.dynamicAllocation.executorAllocationRatio",
        "spark.dynamicAllocation.executorIdleTimeout", "spark.dynamicAllocation.maxExecutors",
        "spark.dynamicAllocation.minExecutors", "spark.executor.cores", "spark.executor.memory",
        "spark.executor.memoryOverhead", "spark.sql.maxPlanStringLength")
      val runtimeConfigSettings = List("spark.sql.hive.filesourcePartitionFileCacheSize",
        "spark.sql.hive.version", "spark.sql.mapKeyDedupPolicy",
        "spark.sql.optimizer.maxIterations", "spark.shuffle.file.buffer",
        "spark.sql.maxPlanStringLength", "spark.sql.shuffle.partitions",
        "spark.sql.warehouse.dir")

      import session.implicits._
      val os: String = System.getProperty("os.name")
      val javaVersion: String = System.getProperty("java.version")
      val scalaVersion: String = scala.util.Properties.versionString
      val sparkContext: SparkContext = session.sparkContext

      def getSparkConfSetting(propName: String): Option[String] = sparkContext.getConf.getOption(propName)

      def getRuntimeConfigSetting(propName: String): Option[String] = session.conf.getOption(propName)

      logger.info(s"logger.isDebugEnabled ? ${logger.isDebugEnabled()}")
      logger.info(s"OS            : $os")
      logger.info(s"Java  Version : $javaVersion")
      logger.info(s"Java  Command : ${System.getProperty("sun.java.command")}")
      logger.info(s"Java TimeZone : ${java.util.TimeZone.getDefault.getDisplayName()}")
      logger.info(s"Scala Version : $scalaVersion")
      logger.info(s"Spark Version : ${sparkContext.version}")
      logger.info(s"Spark AppId   : ${sparkContext.getConf.getAppId}")

      logger.info("Spark Conf Settings :")
      sparkConfSettings
        .map(k => (k, getSparkConfSetting(k))).toDF("spark_conf", "value")
        .orderBy("spark_conf").show(false)

      logger.info("Runtime Config Settings :")
      runtimeConfigSettings
        .map(k => (k, getRuntimeConfigSetting(k))).toDF("runtime_config", "value")
        .orderBy("runtime_config").show(false)

      logger.info(s"Documentation: https://spark.apache.org/docs/${sparkContext.version}/configuration.html")
      _loggEnvDone = true
    }
  }

  /**
   * only return SessionBuilder, this allows to modify config
   * Even if IntelliJ recommends so, do not make it private so that we can play with ith.
   *
   * @param nCores how many cores do you want
   *
   */
  def sessionBuilder(nCores: Int = 1): SparkSession.Builder = SparkSession.builder()
    .appName("UnitTest")
    .master(s"local[$nCores]")
    // performance tuning
    .config("spark.sql.shuffle.partitions", nCores)
    .config("spark.ui.enabled", value = false)
    // avoid timeout during debugging with breakpoints
    .config("spark.network.timeout", "10000")
    .config("spark.executor.heartbeatInterval", "100s")

  def createSparkSession(nCores: Int = 4): SparkSession = {
    val newSparkSession = sessionBuilder(nCores).getOrCreate()
    loggEnv(newSparkSession, logger)
    newSparkSession
  }

}
