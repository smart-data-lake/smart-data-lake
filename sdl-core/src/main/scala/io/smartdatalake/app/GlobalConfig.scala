/*
 * sdl-core - Build your data lake the smart way.
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

import com.typesafe.config.Config
import configs.ConfigReader
import configs.syntax._
import io.smartdatalake.config.ConfigImplicits
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.exporter.ExportWriter
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.{MemoryUtils, SmartDataLakeLogger}
import io.smartdatalake.util.secrets.{SecretProviderConfig, SecretsUtil, StringOrSecret}
import io.smartdatalake.workflow.dataframe.GenericSchema
import org.apache.hadoop.conf.Configuration

/**
 * Global configuration options
 *
 * Note that global configuration is responsible to hold SparkSession, so that its created once and
 * only once per SDLB job. This is especially important if JVM is shared between different SDL jobs
 * (e.g. Databricks cluster), because sharing SparkSession in object Environment survives the
 * current SDLB job.
 *
 * @param statusInfo
 *   enable a REST API providing live status info, see detailed configuration [[StatusInfoConfig]]
 * @param memoryLogTimer
 *   enable periodic memory usage logging, see detailed configuration [[MemoryLogTimerConfig]]
 * @param shutdownHookLogger
 *   enable shutdown hook logger to trace shutdown cause
 * @param stateListeners
 *   Define state listeners to be registered for receiving events of the execution of SmartDataLake
 *   job
 * @param secretProviders
 *   Define SecretProvider's to be registered.
 * @param allowOverwriteAllPartitionsWithoutPartitionValues
 *   Configure a list of exceptions for partitioned DataObject id's, which are allowed to overwrite
 *   the all partitions of a table if no partition values are set. This is used to override/avoid a
 *   protective error when using SDLSaveMode.OverwriteOptimized|OverwritePreserveDirectories. Define
 *   it as a list of DataObject id's.
 * @param synchronousStreamingTriggerIntervalSec
 *   Trigger interval for synchronous actions in streaming mode in seconds (default = 60 seconds)
 *   The synchronous actions of the DAG will be executed with this interval if possile. Note that
 *   for asynchronous actions there are separate settings, e.g. SparkStreamingMode.triggerInterval.
 * @param allowAsRecursiveInput
 *   List of DataObjects for which the validation rules for Action.recursiveInputIds are *not*
 *   checked. The validation rules are 1) that recursive input DataObjects must also be listed in
 *   output DataObjects of the same action 2) the DataObject must implement
 *   TransactionalSparkTableDataObject interface Listing a DataObject in allowAsRecursiveInput can
 *   be used for well thought exceptions, but should be avoided in general. Note that if 1) is true,
 *   also 2) must be fulfilled for Spark to work properly (because Spark can't read/write the same
 *   storage location in the same job), but there might be cases with recursions with different
 *   Actions involved, that dont need to fullfill 2).
 * @param environment
 *   Override environment settings defined in Environment object by setting the corresponding key to
 *   the desired value (key in camelcase notation with the first letter in lowercase)
 * @param pluginOptions
 *   Options for SDLPlugin initialization. Note that SDLPlugin.startup is executed before SDLB
 *   parses the config, and pluginOptions are only available later when calling SDLPlugin.configure
 *   method. An SDLPlugin is set through Environment.plugin, normally this is configured through the
 *   java system property "sdl.pluginClassName".
 * @param uiBackend
 *   Configuration of the UI backend to upload state updates of the Job runs.
 * @param dataObjectsSchemaSource
 *   Optional source URI for DataObjects schemas for development.* This is used for development on
 *   local environment without access to data. Schemas can be exported on dev/prod environment using
 *   the DataObjectSchemaExporter application, and used on local environment to define DataObject
 *   schemas for executing dry-run's. Source must be a path like `file:./schema` or `uiBackend`.
 *   uiBackend will use global.uiBackend configuration to query UI backend for schemas. Default:
 *   `file:./schema`.
 * @param defaultSparkConnectionId
 *   Optional default ConnectionId for Spark connections. This is used to avoid having to specify
 *   the Spark connection for every Spark Action. Note that the default connection can still be
 *   overwritten for each Spark Action by specifying the sparkConnectionId in the action's
 *   configuration.
 */
case class GlobalConfig(
    hadoopOptions: Option[Map[String, StringOrSecret]] = None,
    statusInfo: Option[StatusInfoConfig] = None,
    memoryLogTimer: Option[MemoryLogTimerConfig] = None,
    shutdownHookLogger: Boolean = false,
    stateListeners: Seq[StateListenerConfig] = Seq(),
    secretProviders: Option[Map[String, SecretProviderConfig]] = None,
    allowOverwriteAllPartitionsWithoutPartitionValues: Seq[DataObjectId] = Seq(),
    allowAsRecursiveInput: Seq[DataObjectId] = Seq(),
    synchronousStreamingTriggerIntervalSec: Int = 60,
    environment: Map[String, String] = Map(),
    pluginOptions: Map[String, StringOrSecret] = Map(),
    uiBackend: Option[UIBackendConfig] = None,
    dataObjectsSchemaSource: Option[String] = None,
    defaultSparkConnectionId: Option[ConnectionId] = None
) extends SmartDataLakeLogger {

  // start memory logger, else log memory once
  if (memoryLogTimer.isDefined) {
    memoryLogTimer.get.startTimer()
  } else MemoryUtils.logHeapInfo(false, false, false)

  // add debug shutdown hook logger
  if (shutdownHookLogger) MemoryUtils.addDebugShutdownHooks()

  // register secret providers
  secretProviders.getOrElse(Map()).foreach { case (id, providerConfig) =>
    SecretsUtil.registerProvider(id, providerConfig.provider)
  }

  @transient lazy val getHadoopConfiguration: Configuration = {
    val hadoopConf = new Configuration()
    hadoopOptions.getOrElse(Map()).foreach { case (key, value) =>
      hadoopConf.set(key, value.resolve())
    }
    hadoopConf
  }

  /**
   * pluginOptions are global for all plugins
   */
  Environment.sdlPlugins.foreach(_.configure(pluginOptions))

  def getSchemaFromSource(dataObjectId: DataObjectId)(implicit hadoopConf: Configuration): Option[GenericSchema] =
    if (dataObjectsSchemaSource.isDefined) {
      // get schema
      val connector = ExportWriter.apply(dataObjectsSchemaSource.get, backendClient = uiBackend.map(_.client), hadoopConfig = Some(hadoopConf))
      connector.readLatestSchema(dataObjectId) match {
        case Some(content) => try
            // parse
            Some(ExportWriter.parseSchema(content)._1)
          catch {
            case ex: Exception => throw new IllegalStateException(s"Could not parse schema for DataObject '${dataObjectId.id}': ${ex.getMessage}", ex)
          }
        case None =>
          logger.info(s"No schema found for DataObject '${dataObjectId.id}' in source '${dataObjectsSchemaSource.get}'")
          None
      }
    } else None
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
