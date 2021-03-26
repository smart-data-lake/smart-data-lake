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
import configs.Configs
import configs.syntax._
import io.smartdatalake.config.ConfigImplicits
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.{MemoryUtils, SmartDataLakeLogger}
import io.smartdatalake.util.secrets.{SecretProviderConfig, SecretsUtil}
import io.smartdatalake.workflow.action.customlogic.{PythonUDFCreatorConfig, SparkUDFCreatorConfig}
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
 */
case class GlobalConfig( kryoClasses: Option[Seq[String]] = None
                       , sparkOptions: Option[Map[String,String]] = None
                       , enableHive: Boolean = true
                       , memoryLogTimer: Option[MemoryLogTimerConfig] = None
                       , shutdownHookLogger: Boolean = false
                       , stateListeners: Seq[StateListenerConfig] = Seq()
                       , sparkUDFs: Option[Map[String,SparkUDFCreatorConfig]] = None
                       , pythonUDFs: Option[Map[String,PythonUDFCreatorConfig]] = None
                       , secretProviders: Option[Map[String,SecretProviderConfig]] = None
                       , allowOverwriteAllPartitionsWithoutPartitionValues: Seq[DataObjectId] = Seq()
                       )
extends SmartDataLakeLogger {

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
   * Create a spark session using settings from this global config
   */
  def createSparkSession(appName: String, master: Option[String], deployMode: Option[String] = None): SparkSession = {
    if (Environment._sparkSession != null) logger.warn("Your SparkSession was already set, that should not happen. We will re-initialize it anyway now.")
    // prepare additional spark options
    // enable MemoryLoggerExecutorPlugin if memoryLogTimer is enabled
    val executorPlugins = (sparkOptions.flatMap(_.get("spark.executor.plugins")).toSeq ++ (if (memoryLogTimer.isDefined) Seq(classOf[MemoryLoggerExecutorPlugin].getName) else Seq()))
    // config for MemoryLoggerExecutorPlugin can only be transferred to Executor by spark-options
    val memoryLogOptions = memoryLogTimer.map(_.getAsMap).getOrElse(Map())
    val sparkOptionsExtended = sparkOptions.getOrElse(Map()) ++ memoryLogOptions ++ (if (executorPlugins.nonEmpty) Map("spark.executor.plugins" -> executorPlugins.mkString(",")) else Map())
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
    // return
    Environment._sparkSession
  }
}
object GlobalConfig extends ConfigImplicits {
  private[smartdatalake] def from(config: Config): GlobalConfig = {
    implicit val customStateListenerConfig: Configs[StateListenerConfig] = Configs.derive[StateListenerConfig]
    globalConfig = Some(config.get[Option[GlobalConfig]]("global").value.getOrElse(GlobalConfig()))
    globalConfig.get
  }
  // store global config to be used in MemoryLoggerExecutorPlugin
  var globalConfig: Option[GlobalConfig] = None
}