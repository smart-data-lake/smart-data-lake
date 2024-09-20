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
import io.smartdatalake.util.secrets.{SecretProviderConfig, SecretsUtil, StringOrSecret}
import io.smartdatalake.workflow.action.spark.customlogic.{PythonUDFCreatorConfig, SparkUDFCreatorConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.custom.ExpressionEvaluator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.PrivateAccessor

/**
 * Global configuration options
 *
 * Note that global configuration is responsible to hold SparkSession, so that its created once and only once per SDLB job.
 * This is especially important if JVM is shared between different SDL jobs (e.g. Databricks cluster), because sharing SparkSession in object Environment survives the current SDLB job.
 *
 * @param kryoClasses                                       classes to register for spark kryo serialization
 * @param sparkOptions                                      spark options
 * @param statusInfo                                        enable a REST API providing live status info, see detailed configuration [[StatusInfoConfig]]
 * @param enableHive                                        enable hive for spark session
 * @param memoryLogTimer                                    enable periodic memory usage logging, see detailed configuration [[MemoryLogTimerConfig]]
 * @param shutdownHookLogger                                enable shutdown hook logger to trace shutdown cause
 * @param stateListeners                                    Define state listeners to be registered for receiving events of the execution of SmartDataLake job
 * @param sparkUDFs                                         Define UDFs to be registered in spark session. The registered UDFs are available in Spark SQL transformations
 *                                                          and expression evaluation, e.g. configuration of ExecutionModes.
 * @param pythonUDFs                                        Define UDFs in python to be registered in spark session. The registered UDFs are available in Spark SQL transformations
 *                                                          but not for expression evaluation.
 * @param secretProviders                                   Define SecretProvider's to be registered.
 * @param allowOverwriteAllPartitionsWithoutPartitionValues Configure a list of exceptions for partitioned DataObject id's,
 *                       which are allowed to overwrite the all partitions of a table if no partition values are set.
 *                       This is used to override/avoid a protective error when using SDLSaveMode.OverwriteOptimized|OverwritePreserveDirectories.
 *                       Define it as a list of DataObject id's.
 * @param synchronousStreamingTriggerIntervalSec Trigger interval for synchronous actions in streaming mode in seconds (default = 60 seconds)
 *                       The synchronous actions of the DAG will be executed with this interval if possile.
 *                       Note that for asynchronous actions there are separate settings, e.g. SparkStreamingMode.triggerInterval.
 * @param allowAsRecursiveInput List of DataObjects for which the validation rules for Action.recursiveInputIds are *not* checked.
 *                              The validation rules are
 *                              1) that recursive input DataObjects must also be listed in output DataObjects of the same action
 *                              2) the DataObject must implement TransactionalSparkTableDataObject interface
 *                              Listing a DataObject in allowAsRecursiveInput can be used for well thought exceptions, but should be avoided in general.
 *                              Note that if 1) is true, also 2) must be fullfilled for Spark to work properly (because Spark can't read/write the same storage location in the same job),
 *                              but there might be cases with recursions with different Actions involved, that dont need to fullfill 2).
 * @param environment    Override environment settings defined in Environment object by setting the corresponding key to the desired value (key in camelcase notation with the first letter in lowercase)
 * @param pluginOptions  Options for SDLPlugin initialization.
 *                       Note that SDLPlugin.startup is executed before SDLB parses the config, and pluginOptions are only available later when calling SDLPlugin.configure method.
 *                       An SDLPlugin is set through Environment.plugin, normally this is configured through the java system property "sdl.pluginClassName".
 * @param uiBackend      Configuration of the UI backend to upload state updates of the Job runs.
 */
case class GlobalConfig(kryoClasses: Option[Seq[String]] = None
                        , sparkOptions: Option[Map[String, StringOrSecret]] = None
                        , statusInfo: Option[StatusInfoConfig] = None
                        , enableHive: Boolean = true
                        , memoryLogTimer: Option[MemoryLogTimerConfig] = None
                        , shutdownHookLogger: Boolean = false
                        , stateListeners: Seq[StateListenerConfig] = Seq()
                        , sparkUDFs: Option[Map[String, SparkUDFCreatorConfig]] = None
                        , pythonUDFs: Option[Map[String, PythonUDFCreatorConfig]] = None
                        , secretProviders: Option[Map[String, SecretProviderConfig]] = None
                        , allowOverwriteAllPartitionsWithoutPartitionValues: Seq[DataObjectId] = Seq()
                        , allowAsRecursiveInput: Seq[DataObjectId] = Seq()
                        , synchronousStreamingTriggerIntervalSec: Int = 60
                        , environment: Map[String, String] = Map()
                        , pluginOptions: Map[String, StringOrSecret] = Map()
                        , uiBackend: Option[UIBackendConfig] = None
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

  // configure SDLPlugin
  Environment.sdlPlugin.foreach(_.configure(pluginOptions))

  /**
   * Get Hadoop configuration as Spark would see it.
   * This is using potential hadoop properties defined in sparkOptions.
   */
  def getHadoopConfiguration: Configuration = {
    PrivateAccessor.getHadoopConfiguration(sparkOptions.map(_.mapValues(_.resolve()).toMap).getOrElse(Map()))
  }

  /**
   * Create a spark session using settings from this global config
   */
  private def createSparkSession(appName: String, master: Option[String], deployMode: Option[String] = None): SparkSession = {
    val sparkOptionsExtended = additionalSparkOptions ++ sparkOptions.getOrElse(Map())
    checkCaseSensitivityIsConsistent(sparkOptionsExtended)
    val sparkSession = AppUtil.createSparkSession(appName, master, deployMode, kryoClasses, sparkOptionsExtended, enableHive)
    registerUdf(sparkSession)
    // adjust log level
    LogUtil.setLogLevel(sparkSession.sparkContext)
    // set sparkSession in environment
    // Attention: if JVM is shared between different SDL jobs (e.g. Databricks cluster), this overrides values from earlier jobs!
    // Environment._sparkSession is a workaround for accessing it in custom code. It should not be used inside SDLB code.
    Environment._sparkSession = sparkSession
    // return
    sparkSession
  }

  private def additionalSparkOptions: Map[String, StringOrSecret] = {
    // note that any plaintext Spark options will be logged when the Spark session is configured
    // spark.plugins only contains class names, so we can safely resolve the value here without exposing any sensitive information in the logs
    val sparkPlugins = sparkOptions.flatMap(_.get("spark.plugins")).map(_.resolve()).toSeq
    // enable MemoryLoggerExecutorPlugin if memoryLogTimer is enabled
    val executorPlugins = sparkPlugins ++ (if (memoryLogTimer.isDefined) Seq(classOf[MemoryLoggerExecutorPlugin].getName) else Seq())
    val executorPluginOptions = if (executorPlugins.nonEmpty) Map("spark.executor.plugins" -> executorPlugins.mkString(",")) else Map[String, String]()
    // config for MemoryLoggerExecutorPlugin can only be transferred to Executor by spark-options
    val memoryLogOptions = memoryLogTimer.map(_.getAsMap).getOrElse(Map())
    // get additional options from modules
    val moduleOptions = ModulePlugin.modules.map(_.additionalSparkProperties()).reduceOption(mergeSparkOptions).getOrElse(Map())
    // if SDL is case sensitive then Spark should be as well
    val caseSensitivityOptions = Map(SQLConf.CASE_SENSITIVE.key -> Environment.caseSensitive.toString)
    Seq(moduleOptions, memoryLogOptions, executorPluginOptions, caseSensitivityOptions).reduceOption(mergeSparkOptions).map(_.mapValues(StringOrSecret).toMap).getOrElse(Map())
  }

  private def checkCaseSensitivityIsConsistent(options: Map[String, StringOrSecret]): Unit = {
    options.get(SQLConf.CASE_SENSITIVE.key)
      .map(_.resolve().toBoolean)
      .filter(_ != Environment.caseSensitive)
      .foreach(caseSensitive => {
        logger.warn(s"Spark property '${SQLConf.CASE_SENSITIVE.key}' is set to '$caseSensitive' but SDL environment property 'caseSensitive' is '${Environment.caseSensitive}'." +
          " Inconsistent case sensitivity in SDL and Spark may lead to unexpected behaviour.")
      })
  }

  // private holder for spark session
  private[smartdatalake] var _sparkSession: Option[SparkSession] = None

  /**
   * Return SparkSession
   * Create SparkSession if not yet done, but only if it is used.
   */
  def sparkSession(appName: String, master: Option[String], deployMode: Option[String] = None): SparkSession = {
    if (_sparkSession.isEmpty) {
      _sparkSession = Some(createSparkSession(appName, master, deployMode))
    }
    _sparkSession.get
  }

  /**
   * True if a SparkSession has been created in this job
   */
  def hasSparkSession: Boolean = _sparkSession.isDefined

  private[smartdatalake] def setSparkOptions(session:SparkSession): Unit = {
    sparkOptions.getOrElse(Map()).foreach{ case (k,v) => session.conf.set(k,v.resolve())}
  }

  private[smartdatalake] def registerUdf(session: SparkSession): Unit = {
    sparkUDFs.getOrElse(Map()).foreach { case (name,config) =>
      val udf = config.getUDF
      // register in SDL spark session
      session.udf.register(name, udf)
      // register for use in expression evaluation
      ExpressionEvaluator.registerUdf(name, udf)
    }
    pythonUDFs.getOrElse(Map()).foreach { case (name,config) =>
      // register in SDL spark session
      config.registerUDF(name, session)
    }
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