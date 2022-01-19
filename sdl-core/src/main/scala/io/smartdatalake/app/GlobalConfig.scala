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
import configs.ConfigReader
import configs.syntax._
import io.smartdatalake.config.ConfigImplicits
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.{LogUtil, MemoryUtils, SmartDataLakeLogger}
import io.smartdatalake.util.secrets.{SecretProviderConfig, SecretsUtil}
import io.smartdatalake.workflow.action.customlogic.{PythonUDFCreatorConfig, SparkUDFCreatorConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.custom.PrivateAccessor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.custom.ExpressionEvaluator

/**
 * Global configuration options
 *
 * @param kryoClasses    classes to register for spark kryo serialization
 * @param sparkOptions   spark options
 * @param enableHive     enable hive for spark session
 * @param memoryLogTimer enable periodic memory usage logging, see detailed configuration [[MemoryLogTimerConfig]]
 * @param shutdownHookLogger enable shutdown hook logger to trace shutdown cause
 * @param stateListeners Define state listeners to be registered for receiving events of the execution of SmartDataLake job
 * @param sparkUDFs      Define UDFs to be registered in spark session. The registered UDFs are available in Spark SQL transformations
 *                       and expression evaluation, e.g. configuration of ExecutionModes.
 * @param pythonUDFs     Define UDFs in python to be registered in spark session. The registered UDFs are available in Spark SQL transformations
 *                       but not for expression evaluation.
 * @param secretProviders Define SecretProvider's to be registered.
 * @param allowOverwriteAllPartitionsWithoutPartitionValues Configure a list of exceptions for partitioned DataObject id's,
 *                       which are allowed to overwrite the all partitions of a table if no partition values are set.
 *                       This is used to override/avoid a protective error when using SDLSaveMode.OverwriteOptimized|OverwritePreserveDirectories.
 *                       Define it as a list of DataObject id's.
 * @param runtimeDataNumberOfExecutionsToKeep Number of Executions to keep runtime data for in streaming mode (default = 10).
 *                       Must be bigger than 1.
 * @param synchronousStreamingTriggerIntervalSec Trigger interval for synchronous actions in streaming mode in seconds (default = 60 seconds)
 *                       The synchronous actions of the DAG will be executed with this interval if possile.
 *                       Note that for asynchronous actions there are separate settings, e.g. SparkStreamingMode.triggerInterval.
 */
case class GlobalConfig( kryoClasses: Option[Seq[String]] = None
                       , sparkOptions: Option[Map[String,String]] = None
                       , restApi: Option[RestApi] = None
                       , enableHive: Boolean = true
                       , memoryLogTimer: Option[MemoryLogTimerConfig] = None
                       , shutdownHookLogger: Boolean = false
                       , stateListeners: Seq[StateListenerConfig] = Seq()
                       , sparkUDFs: Option[Map[String,SparkUDFCreatorConfig]] = None
                       , pythonUDFs: Option[Map[String,PythonUDFCreatorConfig]] = None
                       , secretProviders: Option[Map[String,SecretProviderConfig]] = None
                       , allowOverwriteAllPartitionsWithoutPartitionValues: Seq[DataObjectId] = Seq()
                       , runtimeDataNumberOfExecutionsToKeep: Int = 10
                       , synchronousStreamingTriggerIntervalSec: Int = 60
                       )
extends SmartDataLakeLogger {
  assert(runtimeDataNumberOfExecutionsToKeep>1, "GlobalConfig.runtimeDataNumberOfExecutionsToKeep must be bigger than 1.")

  // start memory logger, else log memory once
  if (memoryLogTimer.isDefined) {
    memoryLogTimer.get.startTimer()
  }
  else MemoryUtils.logHeapInfo(false, false, false)

  // add debug shutdown hook logger
  if (shutdownHookLogger) MemoryUtils.addDebugShutdownHooks()

  // register secret providers
  secretProviders.getOrElse(Map()).foreach { case (id, providerConfig) =>
    SecretsUtil.registerProvider(id, providerConfig.provider)
  }

  /**
   * Get Hadoop configuration as Spark would see it.
   * This is using potential hadoop properties defined in sparkOptions.
   */
  def getHadoopConfiguration: Configuration = {
    PrivateAccessor.getHadoopConfiguration(sparkOptions.getOrElse(Map()))
  }

  /**
   * Create a spark session using settings from this global config
   */
  def createSparkSession(appName: String, master: Option[String], deployMode: Option[String] = None): SparkSession = {
    if (Environment._sparkSession != null) logger.warn("Your SparkSession was already set, that should not happen. We will re-initialize it anyway now.")
    // prepare additional spark options
    // enable MemoryLoggerExecutorPlugin if memoryLogTimer is enabled
    val executorPlugins = sparkOptions.flatMap(_.get("spark.plugins")).toSeq ++ (if (memoryLogTimer.isDefined) Seq(classOf[MemoryLoggerExecutorPlugin].getName) else Seq())
    val executorPluginOptions = if (executorPlugins.nonEmpty) Map("spark.executor.plugins" -> executorPlugins.mkString(",")) else Map[String,String]()
    // config for MemoryLoggerExecutorPlugin can only be transferred to Executor by spark-options
    val memoryLogOptions = memoryLogTimer.map(_.getAsMap).getOrElse(Map())
    // get additional options from modules
    val moduleOptions = ModulePlugin.modules.map(_.additionalSparkProperties()).reduceOption(mergeSparkOptions).getOrElse(Map())
    // combine all options: custom options will override framework options
    val sparkOptionsExtended = Seq(moduleOptions, memoryLogOptions, executorPluginOptions).reduceOption(mergeSparkOptions).getOrElse(Map()) ++  sparkOptions.getOrElse(Map())
    Environment._sparkSession = AppUtil.createSparkSession(appName, master, deployMode, kryoClasses, sparkOptionsExtended, enableHive)
    sparkUDFs.getOrElse(Map()).foreach { case (name,config) =>
      val udf = config.getUDF
      // register in SDL spark session
      Environment._sparkSession.udf.register(name, udf)
      // register for use in expression evaluation
      ExpressionEvaluator.registerUdf(name, udf)
    }
    pythonUDFs.getOrElse(Map()).foreach { case (name,config) =>
      // register in SDL spark session
      config.registerUDF(name, Environment._sparkSession)
    }
    // adjust log level
    LogUtil.setLogLevel(Environment._sparkSession.sparkContext)
    // return
    Environment._sparkSession
  }

  /**
   * When merging Spark options special care must be taken for properties which are comma separated lists.
   */
  private def mergeSparkOptions(m1: Map[String,String], m2: Map[String,String]): Map[String,String] = {
    val listOptions = Seq("spark.plugins", "spark.executor.plugins", "spark.sql.extensions")
    m2.foldLeft(m1) {
      case (m, (k,v)) =>
        val mergedV = if (listOptions.contains(k)) (m.getOrElse(k, "").split(',') ++ v.split(',')).distinct.mkString(",") else v
        m.updated(k, mergedV)
    }
  }
}
object GlobalConfig extends ConfigImplicits {
  private[smartdatalake] def from(config: Config): GlobalConfig = {
    implicit val customStateListenerConfig: ConfigReader[StateListenerConfig] = ConfigReader.derive[StateListenerConfig]
    globalConfig = Some(config.get[Option[GlobalConfig]]("global").value.getOrElse(GlobalConfig()))
    globalConfig.get
  }
  // store global config to be used in MemoryLoggerExecutorPlugin
  var globalConfig: Option[GlobalConfig] = None
}