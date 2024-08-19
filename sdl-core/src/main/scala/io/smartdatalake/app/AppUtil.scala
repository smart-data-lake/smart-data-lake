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
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.{EnvironmentUtil, GraphUtil, SmartDataLakeLogger}
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.spark.SDLSparkExtension
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.{Action, SDLExecutionId}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkEnv
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.util.ChildFirstURLClassLoader
import org.slf4j.MDC

import java.net.{InetAddress, URL, URLClassLoader}
import java.time.LocalDateTime
import scala.annotation.tailrec
import scala.util.Try

/**
 * Utilities and conventions to name and validate command line parameters
 */
object AppUtil extends SmartDataLakeLogger {

  // Kerberos Authentication...
  def authenticate(keytab: String, userAtRealm: String): Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    UserGroupInformation.setConfiguration(hadoopConf)
    UserGroupInformation.loginUserFromKeytab(userAtRealm, keytab)
  }

  def createSparkSession(name:String, masterOpt: Option[String] = None,
                         deployModeOpt: Option[String] = None,
                         kryoClassNamesOpt: Option[Seq[String]] = None,
                         sparkOptionsOpt: Map[String,StringOrSecret] = Map(),
                         enableHive: Boolean = true
                        ): SparkSession = {
    logger.info(s"Creating spark session. Note that None means that the Defaults of the Spark Environment apply." +
      s"name=$name master=$masterOpt deployMode=$deployModeOpt enableHive=$enableHive kryoClassNamesOpt=$kryoClassNamesOpt sparkOptionsOpt=$sparkOptionsOpt")

    // prepare extensions
    val noDataExtension = if (Environment.enableSparkPlanNoDataCheck) Some(new SDLSparkExtension) else None

    // create configObject
    val sessionBuilder = SparkSession.builder()
      .optionalMaster(masterOpt)
      .appName(name)
      .config("hive.exec.dynamic.partition", value = true) // default value for normal operation of SDL; can be overwritten by configuration (sparkOptionsOpt)
      .config("hive.exec.dynamic.partition.mode", "nonstrict") // default value for normal operation of SDL; can be overwritten by configuration (sparkOptionsOpt)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic") // default value for normal operation of SDL; can be overwritten by configuration (sparkOptionsOpt)
      .optionalConfig( "deploy-mode", deployModeOpt)
      .optionalConfig( "spark.kryo.classesToRegister", kryoClassNamesOpt.map(_.mkString(",")))
      .optionalConfigs( sparkOptionsOpt )
      .optionalEnableHive(enableHive)
      .optionalExtension(noDataExtension)

    // create session
    val session = sessionBuilder.getOrCreate()
    if (!Try(session.conf.get("spark.sql.sources.partitionOverwriteMode")).toOption.contains("dynamic"))
      logger.warn("Spark property 'spark.sql.sources.partitionOverwriteMode' is not set to 'dynamic'. Overwriting Hadoop/Hive partitions will always overwrite the whole path/table and you might experience data loss!")

    // return
    session
  }

  /**
   * create a class loader which first loads classes from a given list of jar names, instead of delegating to the
   * parent class loader first
   * searches parent classpaths until all jars were found or no more parent classpaths are available
   *
   * @param jars names of jar files available in the classpath
   * @return a class loader
   */
  def getChildFirstClassLoader(jars: Seq[String]): ChildFirstURLClassLoader = {
    val initialLoader = getClass.getClassLoader.asInstanceOf[URLClassLoader]

    @tailrec
    def collectUrls(clazz: ClassLoader, acc: Map[String, URL]): Map[String, URL] = {

      val urlsAcc: Map[String, URL] = acc++
        // add urls on this level to accumulator
        clazz.asInstanceOf[URLClassLoader].getURLs
        .map( url => (url.getFile.split('/').last, url))
        .filter{ case (name, url) => jars.contains(name)}
        .toMap

      // check if any jars without URL are left
      val jarMissing = jars.exists(jar => urlsAcc.get(jar).isEmpty)
      // return accumulated if there is no parent left or no jars are missing anymore
      if (clazz.getParent == null || !jarMissing) urlsAcc else collectUrls(clazz.getParent, urlsAcc)
    }

    // search classpath hierarchy until all jars are found or we have reached the top
    val urlsMap = collectUrls(initialLoader, Map())

    // check if everything found
    val jarsNotFound = jars.filter( jar => urlsMap.get(jar).isEmpty)
    if (jarsNotFound.nonEmpty) {
      logger.info(s"""available jars are ${initialLoader.getURLs.mkString(", ")} (not including parent classpaths)""")
      throw ConfigurationException(s"""jars ${jarsNotFound.mkString(", ")} not found in parent class loaders classpath. Cannot initialize ChildFirstURLClassLoader.""")
    }
    // create child-first classloader
    new ChildFirstURLClassLoader(urlsMap.values.toArray, initialLoader)
  }

  /**
   * pimpMyLibrary pattern to add SparkSession.Builder utility functions
   */
  private implicit class SparkSessionBuilderUtils( builder: SparkSession.Builder ) {
    def optionalMaster( value: Option[String] ): SparkSession.Builder = {
      if (value.isDefined) builder.master(value.get)
      else builder
    }
    def optionalConfig( key: String, value: Option[String] ): SparkSession.Builder = {
      if (value.isDefined) {
        logger.info(s"Additional sparkOption: ${createMaskedSecretsKVLog(key,value.get)}")
        builder.config(key, value.get)
      } else builder
    }
    def optionalConfigs( options: Map[String,StringOrSecret] ): SparkSession.Builder = {
      if (options.nonEmpty) {
        logger.info("Additional sparkOptions: " + options.map{ case (k,v) => createMaskedSecretsKVLog(k,v.toString) }.mkString(", "))
        options.foldLeft( builder ){
          case (sb,(key,value)) => sb.config(key,value.resolve())
        }
      } else builder
    }
    def optionalEnableHive(enable: Boolean ): SparkSession.Builder = {
      if (enable) builder.enableHiveSupport()
      else builder
    }
    def optionalExtension(extension: Option[(SparkSessionExtensions => Unit)]): SparkSession.Builder = {
      extension.map(e => builder.withExtensions(e)).getOrElse(builder)
    }
  }

  /**
   * Create log message text for key/value, where secrets are masked.
   * For now only s3 secrets are catched.
   */
  def createMaskedSecretsKVLog(key: String, value: String): String = {
    // regexp for s3 secrets, see https://databricks.com/blog/2017/05/30/entropy-based-log-redaction-apache-spark-databricks.html
    val s3secretPattern = "(?<![A-Za-z0-9/+])([A-Za-z0-9/+=]|%2F|%2B|%3D|%252F|%252B|%253D){40}(?![A-Za-z0-9/+=])".r
    val maskedValue = value match {
      case s3secretPattern(_) => "..."
      case v => v
    }
    s"$key=$maskedValue"
  }

  /**
   * Filter action list with extended syntax: "<prefix:?><regex>,<operation?><prefix:?><regex>;..."
   * Search behavior can be modified by the following prefixes to a regex expression separated by a colon (:)
   * - 'feeds': select actions where metadata.feed is matched by regex pattern (default)
   * - 'names': select actions where metadata.name is matched by regex pattern
   * - 'ids': select actions where id is matched by regex pattern
   * - 'layers': select actions where metadata.layer of all output DataObjects is matched by regex pattern
   * - 'startFromActionIds': select actions which with id is matched by regex pattern and any dependent action (=successors)
   * - 'endWithActionIds': select actions which with id is matched by regex pattern and their predecessors
   * - 'startFromDataObjectIds': select actions which have an input DataObject with id is matched by regex pattern and any dependent action (=successors)
   * - 'endWithDataObjectIds': select actions which have an output DataObject with id is matched by regex pattern and their predecessors
   * You can combine several pattern by using semicolon (;). Patterns are combined from left to right.
   * By default combining sets is done with union operation. The operation can be modified by adding one of the following characters before the prefix:
   * - pipe symbol (|): the two sets are combined by union operation (default)
   * - ampersand symbol (&): the two sets are combined by intersection operation
   * - minus symbol (-): the second set is subtracted from the first set
   * All matching is done case-insensitive.
   * Example: to filter action 'A' and its successors but only in layer L1 and L2, use the following pattern: "startFromActionIds:a;&layers:(l1|l2)"
   */
  def filterActionList(feedSel: String, actions: Set[Action]): Set[Action] = {
    val patterns = feedSel.toLowerCase.split(',')
    val opMatcher = "([|&-])?(.*)".r
    val prefixMatcher = "([a-z]+:)?(.*)".r
    val inputActionMap = actions.flatMap(a => a.inputs.map(i => (i,a))).toMap
    val actionGraphEdges = actions.flatMap(a => a.outputs.flatMap(o => inputActionMap.get(o).map(o => (a,o))))
    val graph = GraphUtil.Graph(actionGraphEdges)
    patterns.foldLeft(Set[Action]()) {
      case (result, patternWithOp) =>
        val (op,pattern) = patternWithOp match {
          case opMatcher(op,pattern) => (Option(op),pattern)
        }
        val selectedActions = pattern match {
          case prefixMatcher("feeds:", regex) => actions.filter(_.metadata.flatMap(_.feed).exists(_.toLowerCase.matches(regex)))
          case prefixMatcher("names:", regex) => actions.filter(_.metadata.flatMap(_.name).exists(_.toLowerCase.matches(regex)))
          case prefixMatcher("ids:", regex) => actions.filter(_.id.id.toLowerCase.matches(regex))
          case prefixMatcher("layers:", regex) => actions.filter(_.outputs.forall(_.metadata.flatMap(_.layer).exists(_.toLowerCase.matches(regex))))
          case prefixMatcher("startfromactionids:", regex) => actions.filter(_.id.id.toLowerCase.matches(regex)).flatMap(graph.getConnectedNodesForward)
          case prefixMatcher("endwithactionids:", regex) => actions.filter(_.id.id.toLowerCase.matches(regex)).flatMap(graph.getConnectedNodesReverse)
          case prefixMatcher("startfromdataobjectids:", regex) => actions.filter(_.inputs.exists(_.id.id.toLowerCase.matches(regex))).flatMap(graph.getConnectedNodesForward)
          case prefixMatcher("endwithdataobjectids:", regex) => actions.filter(_.outputs.exists(_.id.id.toLowerCase.matches(regex))).flatMap(graph.getConnectedNodesReverse)
          case prefixMatcher(null, regex) => actions.filter(_.metadata.flatMap(_.feed).exists(_.toLowerCase.matches(regex))) // default is filter feeds
          case prefixMatcher(prefix, _) => throw new RuntimeException(s"Unknown prefix $prefix for pattern $pattern in commandline parameter feedSel")
        }
        op match {
          case Some("|") => result.union(selectedActions)
          case Some("&") => result.intersect(selectedActions)
          case Some("-") => result.diff(selectedActions)
          case None => result.union(selectedActions) // default
          case _ => throw new RuntimeException(s"Unknown operation $op for pattern $pattern in commandline parameter feedSel")
        }
    }
  }

  /**
   * read version from package manifest (not defined if project is executed in IntelliJ)
   */
  def getManifestVersion: Option[String] = {
    Option(getClass.getPackage.getImplementationVersion)
  }

  /**
   * Set SDLB job information in MDC context for logger.
   * MDC (Mapped Diagnostic Context) allows to make available additional context information to logger layouts.
   */
  def setSdlbRunLoggerContext(appConfig: SmartDataLakeBuilderConfig, executionId: Option[SDLExecutionId] = None, runStartTime: Option[LocalDateTime] = None): Unit = {
    MDC.put(MDC_SDLB_APP, appConfig.appName)
    executionId.foreach(executionId => MDC.put(MDC_SDLB_RUN_ID, executionId.runId.toString))
    executionId.foreach(executionId => MDC.put(MDC_SDLB_ATTEMPT_ID, executionId.attemptId.toString))
    runStartTime.foreach(runStartTime => MDC.put(MDC_SDLB_START_TIME, runStartTime.toString))
    getMachineContext.foreach{ case (k,v) => MDC.put(k,v)}
  }
  def setSdlbRunLoggerContext(context: ActionPipelineContext): Unit = {
    setSdlbRunLoggerContext(context.appConfig, Some(context.executionId), Some(context.runStartTime))
  }
  private final val MDC_SDLB_APP = "app"
  private final val MDC_SDLB_RUN_ID = "runId"
  private final val MDC_SDLB_ATTEMPT_ID = "attemptId"
  private final val MDC_SDLB_START_TIME = "startTime"
  final val MDC_SDLB_PROPERTIES = Seq(MDC_SDLB_APP, MDC_SDLB_RUN_ID, MDC_SDLB_ATTEMPT_ID, MDC_SDLB_START_TIME)
  def applySdlbRunLoggerContext(session: SparkSession): Unit = {
    MDC_SDLB_PROPERTIES.foreach(k => session.sparkContext.setLocalProperty(k,MDC.get(k)))
  }

  /**
   * get machine information for logger context.
   */
  def getMachineContext: Map[String,String] = {
    Seq(
      Option(SparkEnv.get).map(se => (MDC_EXECUTOR_ID, se.executorId)),
      Some((MDC_HOSTNAME, hostname))
    ).flatten.toMap
  }
  private final val MDC_EXECUTOR_ID = "executorId"
  private final val MDC_HOSTNAME = "hostname"
  @transient private lazy val hostname = InetAddress.getLocalHost.getHostName
}