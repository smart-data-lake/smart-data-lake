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

package io.smartdatalake.workflow.connection

import com.typesafe.config.Config
import io.smartdatalake.app.AppUtil.{MDC_SDLB_PROPERTIES, createMaskedSecretsKVLog}
import io.smartdatalake.app.ModulePlugin
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.{LogUtil, SmartDataLakeLogger}
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.spark.SDLSparkExtension
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.spark.customlogic.{PythonUDFCreatorConfig, SparkUDFCreatorConfig}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.slf4j.MDC

import scala.reflect.runtime.universe.{Type, typeOf}
import scala.util.Try

/**
 * Connection information for a classic Spark session.
 *
 * @param master
 *  master URL for the Spark session. If not set, it will try to get an existing Spark session from the environment.
 * @param enableHive
 *   enable hive for spark session
 * @param sparkOptions
 *   spark options
 * @param sparkUDFs
 *   Define UDFs to be registered in spark session. The registered UDFs are available in Spark SQL
 *   transformations and expression evaluation, e.g. configuration of ExecutionModes.
 * @param pythonUDFs
 *   Define UDFs in python to be registered in spark session. The registered UDFs are available in
 *   Spark SQL transformations but not for expression evaluation.
 * @param kryoClasses
 *   classes to register for spark kryo serialization
 */
case class SparkClassicConnection(
    override val id: ConnectionId,
    master: Option[String],
    deployMode: Option[String] = None,
    sparkOptions: Map[String, StringOrSecret] = Map(),
    enableHive: Boolean = true,
    sparkUDFs: Option[Map[String, SparkUDFCreatorConfig]] = None,
    pythonUDFs: Option[Map[String, PythonUDFCreatorConfig]] = None,
    kryoClasses: Option[Seq[String]] = None,
    override val metadata: Option[ConnectionMetadata] = None
) extends Connection with EngineConnection with SmartDataLakeLogger {

  val subFeedType: Type = typeOf[SparkSubFeed]

  @transient private var _sparkSession: Option[SparkSession] = None
  def sparkSession(implicit context: ActionPipelineContext): SparkSession = {
    if (_sparkSession.isEmpty) {
      require(!master.contains("yarn") || System.getenv("SPARK_HOME") != null, "Env variable SPARK_HOME needs to be set in local mode with master=yarn!")
      val sparkOptionsExtended = additionalSparkOptions ++ sparkOptions
      checkCaseSensitivityIsConsistent(sparkOptionsExtended)
      val sparkSession = SparkClassicConnection.createSparkSession(context.application, master, deployMode, kryoClasses, sparkOptionsExtended, enableHive)
      registerUdf(sparkSession)
      // adjust log level
      LogUtil.setLogLevel(sparkSession.sparkContext)
      applySdlbRunLoggerContext(sparkSession)
      // return
      _sparkSession = Some(sparkSession)
    }
    _sparkSession.get
  }

  private def additionalSparkOptions(implicit context: ActionPipelineContext): Map[String, StringOrSecret] = {
    // note that any plaintext Spark options will be logged when the Spark session is configured
    // spark.plugins only contains class names, so we can safely resolve the value here without exposing any sensitive information in the logs
    val sparkPlugins = sparkOptions.get("spark.plugins").map(_.resolve()).toSeq
    // enable MemoryLoggerExecutorPlugin if memoryLogTimer is enabled
    val executorPlugins = sparkPlugins
    val executorPluginOptions = if (executorPlugins.nonEmpty) Map("spark.executor.plugins" -> executorPlugins.mkString(",")) else Map[String, String]()
    // get additional options from modules
    val moduleOptions = ModulePlugin.modules.map(_.additionalSparkProperties()).reduceOption(mergeSparkOptions).getOrElse(Map())
    // if SDL is case sensitive then Spark should be as well
    val caseSensitivityOptions = Map(SQLConf.CASE_SENSITIVE.key -> Environment.caseSensitive.toString)
    // get global hadoop options
    val hadoopOptions = context.globalConfig.hadoopOptions.map(_.map { case (k, v) => (s"spark.hadoop.$k", v) }.toMap).getOrElse(Map())
    Seq(moduleOptions, executorPluginOptions, caseSensitivityOptions).reduceOption(mergeSparkOptions).map(
      _.view.mapValues(StringOrSecret).toMap
    ).getOrElse(Map()) ++ hadoopOptions
  }

  private def checkCaseSensitivityIsConsistent(options: Map[String, StringOrSecret]): Unit =
    options.get(SQLConf.CASE_SENSITIVE.key)
      .map(_.resolve().toBoolean)
      .filter(_ != Environment.caseSensitive)
      .foreach(caseSensitive =>
        logger.warn(
          s"Spark property '${SQLConf.CASE_SENSITIVE.key}' is set to '$caseSensitive' but SDL environment property 'caseSensitive' is '${Environment.caseSensitive}'." +
            " Inconsistent case sensitivity in SDL and Spark may lead to unexpected behaviour."
        )
      )

  private[smartdatalake] def setSparkOptions(session: SparkSession): Unit =
    sparkOptions.foreach { case (k, v) => session.conf.set(k, v.resolve()) }

  private[smartdatalake] def registerUdf(session: SparkSession): Unit = {
    sparkUDFs.getOrElse(Map()).foreach { case (name, config) =>
      // register in SDL spark session
      config.registerUdf(name, session)
      // register for use in expression evaluation
      config.registerUdf(name, Environment.expressionEvaluatorFactory())
    }
    pythonUDFs.getOrElse(Map()).foreach { case (name, config) =>
      // register in SDL spark session
      config.registerUDF(name, session)
    }
  }

  /**
   * When merging Spark options special care must be taken for properties which are comma separated
   * lists.
   */
  private def mergeSparkOptions(m1: Map[String, String], m2: Map[String, String]): Map[String, String] = {
    val listOptions = Seq("spark.plugins", "spark.executor.plugins", "spark.sql.extensions")
    m2.foldLeft(m1) {
      case (m, (k, v)) =>
        val mergedV = if (listOptions.contains(k)) (m.getOrElse(k, "").split(',') ++ v.split(',')).distinct.mkString(",") else v
        m.updated(k, mergedV)
    }
  }

  private def applySdlbRunLoggerContext(session: SparkSession): Unit =
    MDC_SDLB_PROPERTIES.foreach(k => session.sparkContext.setLocalProperty(k, MDC.get(k)))

  /**
   * Sets the util job description for better traceability in the Spark UI
   *
   * Note: This sets Spark local properties, which are propagated to the respective executor tasks.
   * We rely on this to match metrics back to Actions and DataObjects.
   * As writing to a DataObject on the Driver happens uninterrupted in the same exclusive thread, this is suitable.
   *
   * @param operation phase description (be short...)
   */
  def setSparkJobMetadata(operation: Option[String] = None)(implicit context: ActionPipelineContext) : Unit = {
    val metadataId = context.currentAction.map(_.id).getOrElse(id)
    sparkSession.sparkContext.setJobGroup(s"${context.appConfig.appName} $metadataId runId=${context.executionId.runId} attemptId=${context.executionId.attemptId}", operation.getOrElse("").take(255))
  }

  override def activate(operation: Option[String])(implicit context: ActionPipelineContext): Unit = {
    setSparkJobMetadata(operation)
  }

  override def factory: FromConfigFactory[Connection] = SparkClassicConnection
}

object SparkClassicConnection extends FromConfigFactory[Connection] with SmartDataLakeLogger {

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SparkClassicConnection = {
    extract[SparkClassicConnection](config)
  }

  def createSparkSession(
      name: String,
      masterOpt: Option[String] = None,
      deployModeOpt: Option[String] = None,
      kryoClassNamesOpt: Option[Seq[String]] = None,
      sparkOptionsOpt: Map[String, StringOrSecret] = Map(),
      enableHive: Boolean = true
  ): SparkSession = {
    if (masterOpt.isDefined) logger.info(
      s"Get or create spark session with parameters: name=$name master=$masterOpt deployMode=$deployModeOpt enableHive=$enableHive kryoClassNamesOpt=$kryoClassNamesOpt sparkOptionsOpt=$sparkOptionsOpt"
    )
    else logger.info(s"Trying to get spark session from environment (master=None)")

    // prepare extensions
    val noDataExtension = if (Environment.enableSparkPlanNoDataCheck) Some(new SDLSparkExtension) else None

    // create configObject
    val sessionBuilder = SparkSession.builder()
      .optionalMaster(masterOpt)
      .appName(name)
      .config("hive.exec.dynamic.partition", value = true) // default value for normal operation of SDL; can be overwritten by configuration (sparkOptionsOpt)
      .config("hive.exec.dynamic.partition.mode",
        "nonstrict") // default value for normal operation of SDL; can be overwritten by configuration (sparkOptionsOpt)
      .config("spark.sql.sources.partitionOverwriteMode",
        "dynamic") // default value for normal operation of SDL; can be overwritten by configuration (sparkOptionsOpt)
      .optionalConfig("deploy-mode", deployModeOpt)
      .optionalConfig("spark.kryo.classesToRegister", kryoClassNamesOpt.map(_.mkString(",")))
      .optionalConfigs(sparkOptionsOpt)
      .optionalEnableHive(enableHive)
      .optionalExtension(noDataExtension)

    // create session
    val session = try
      sessionBuilder.getOrCreate()
    catch {
      case e: SparkException if masterOpt.isEmpty && e.getMessage.startsWith("A master URL must be set in your configuration") =>
        throw new IllegalArgumentException(
          s"This is not an environment with an existing Spark Session. Use SparkSmartDataLakeBuilder instead of e.g. DefaultSmartDataLakeBuilder to configure and create a new Spark session and '--master' and '--deploy-mode' parameter to customize the Spark session."
        )
    }

    // check partitionOverwriteMode
    if (!Try(session.conf.get("spark.sql.sources.partitionOverwriteMode")).toOption.contains("dynamic"))
      logger.warn(
        "Spark property 'spark.sql.sources.partitionOverwriteMode' is not set to 'dynamic'. Overwriting Hadoop/Hive partitions will always overwrite the whole path/table and you might experience data loss!"
      )

    // return
    session
  }

  /**
   * pimpMyLibrary pattern to add SparkSession.Builder utility functions
   */
  private implicit class SparkSessionBuilderUtils(builder: SparkSession.Builder) {
    def optionalMaster(value: Option[String]): SparkSession.Builder =
      if (value.isDefined) builder.master(value.get)
      else builder
    def optionalConfig(key: String, value: Option[String]): SparkSession.Builder =
      if (value.isDefined) {
        logger.info(s"Additional sparkOption: ${createMaskedSecretsKVLog(key, value.get)}")
        builder.config(key, value.get)
      } else builder
    def optionalConfigs(options: Map[String, StringOrSecret]): SparkSession.Builder =
      if (options.nonEmpty) {
        logger.info("Additional sparkOptions: " + options.map { case (k, v) => createMaskedSecretsKVLog(k, v.toString) }.mkString(", "))
        options.foldLeft(builder) {
          case (sb, (key, value)) => sb.config(key, value.resolve())
        }
      } else builder
    def optionalEnableHive(enable: Boolean): SparkSession.Builder =
      if (enable) builder.enableHiveSupport()
      else builder
    def optionalExtension(extension: Option[SparkSessionExtensions => Unit]): SparkSession.Builder =
      extension.map(e => builder.withExtensions(e)).getOrElse(builder)
  }

}
