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

import com.typesafe.config.Config
import io.smartdatalake.util.misc.MemoryUtils
import org.apache.spark.sql.SparkSession
import configs.syntax._
import org.apache.spark.{ExecutorPlugin, SparkEnv}

/**
 * Global configuration options
 *
 * @param kryoClasses classes to register for spark kryo serialization
 * @param sparkOptions spark options
 * @param enableHive enable hive for spark session
 * @param memoryLogTimer enable periodic memory usage logging, see detailled configuration [[MemoryLogTimerConfig]]
 * @param shutdownHookLogger enable shutdown hook logger to trace shutdown cause
 */
case class GlobalConfig( kryoClasses: Option[Seq[String]] = None, sparkOptions: Option[Map[String,String]] = None, enableHive: Boolean = true, memoryLogTimer: Option[MemoryLogTimerConfig] = None, shutdownHookLogger: Boolean = false ) {

  // start memory logger, else log memory once
  if (memoryLogTimer.isDefined) {
    memoryLogTimer.get.startTimer()
  }
  else MemoryUtils.logHeapInfo(false, false, false)

  // add debug shutdown hook logger
  if (shutdownHookLogger) MemoryUtils.addDebugShutdownHooks()

  /**
   * Create a spark session using settings from this global config
   */
  def createSparkSession(appName: String, master: Option[String], deployMode: Option[String] = None): SparkSession = {
    // prepare additional spark options
    // enable MemoryLoggerExecutorPlugin if memoryLogTimer is enabled
    val executorPlugins = (sparkOptions.flatMap(_.get("spark.executor.plugins")).toSeq ++ (if (memoryLogTimer.isDefined) Seq("io.smartdatalake.app.MemoryLoggerExecutorPlugin") else Seq())).mkString(",")
    // config for MemoryLoggerExecutorPlugin can only be transfered to Executor by spark-options
    val memoryLogOptions = if (memoryLogTimer.isDefined) Map("smartdatalake.intervalSec"->memoryLogTimer.get.intervalSec.toString, "smartdatalake.logLinuxMem"->memoryLogTimer.get.logLinuxMem.toString, "smartdatalake.logLinuxCgroupMem"->memoryLogTimer.get.logLinuxCgroupMem.toString, "smartdatalake.logBuffers"->memoryLogTimer.get.logBuffers.toString) else Map()
    val sparkOptionsExtended = sparkOptions.getOrElse(Map()) ++ memoryLogOptions + ("spark.executor.plugins" -> executorPlugins)
    AppUtil.createSparkSession(appName, master, deployMode, kryoClasses, Some(sparkOptionsExtended), enableHive)
  }
}
object GlobalConfig {
  private[smartdatalake] def from(config: Config): GlobalConfig = {
    globalConfig = Some(config.get[Option[GlobalConfig]]("global").value.getOrElse(GlobalConfig()))
    globalConfig.get
  }
  // store global config to be used in MemoryLoggerExecutorPlugin
  var globalConfig: Option[GlobalConfig] = None
}



/**
 * Configuration for periodic memory usage logging
 *
 * @param intervalSec interval in seconds between memory usage logs
 * @param logLinuxMem enable logging linux memory
 * @param logLinuxCgroupMem enable logging details about linux cgroup memory
 * @param logBuffers enable logging details about different jvm buffers
 */
case class MemoryLogTimerConfig( intervalSec: Int, logLinuxMem: Boolean = true, logLinuxCgroupMem: Boolean = false, logBuffers: Boolean = false ) {
  def startTimer(): Unit = MemoryUtils.startMemoryLogger(intervalSec, logLinuxMem, logLinuxCgroupMem, logBuffers)
}

/**
 * Executor plugin to start memory usage logging on executors
 */
private[smartdatalake] class MemoryLoggerExecutorPlugin extends ExecutorPlugin {
  override def init(): Unit = {
    // config can only be transfered to Executor by spark-options
    val sparkConf = SparkEnv.get.conf
    val memoryLogConfig = MemoryLogTimerConfig(sparkConf.get("smartdatalake.intervalSec").toInt, sparkConf.get("smartdatalake.logLinuxMem").toBoolean, sparkConf.get("smartdatalake.logLinuxCgroupMem").toBoolean, sparkConf.get("smartdatalake.logBuffers").toBoolean)
    memoryLogConfig.startTimer()
  }
  override def shutdown(): Unit = {
    MemoryUtils.stopMemoryLogger()
  }
}