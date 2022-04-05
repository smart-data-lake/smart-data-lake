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
package io.smartdatalake.definitions

import io.smartdatalake.app.{GlobalConfig, SDLPlugin, StateListener}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.util.misc.{CustomCodeUtil, EnvironmentUtil}
import org.apache.spark.sql.SparkSession
import org.slf4j.event.Level

import java.net.URI

/**
 * Environment dependent configurations.
 * They can be set
 * - by Java system properties (prefixed with "sdl.", e.g. "sdl.hadoopAuthoritiesWithAclsRequired")
 * - by Environment variables (prefixed with "SDL_" and camelCase converted to uppercase, e.g. "SDL_HADOOP_AUTHORITIES_WITH_ACLS_REQUIRED")
 * - by a custom [[io.smartdatalake.app.SmartDataLakeBuilder]] implementation for your environment, which sets these variables directly.
 */
object Environment {

  /**
   * List of hadoop authorities for which acls must be configured
   * The environment parameter can contain multiple authorities separated by comma.
   * An authority is compared against the filesystem URI with contains(...)
   */
  var hadoopAuthoritiesWithAclsRequired: Seq[String] = {
    EnvironmentUtil.getSdlParameter("hadoopAuthoritiesWithAclsRequired")
      .toSeq.flatMap(_.split(','))
  }

  /**
   * Modifying ACL's is only allowed below and including the following level (default=2)
   * See also [[io.smartdatalake.util.misc.AclUtil]]
   */
  var hdfsAclsMinLevelPermissionModify: Int = {
    EnvironmentUtil.getSdlParameter("hdfsAclsMinLevelPermissionModify")
      .map(_.toInt).getOrElse(2)
  }

  /**
   * Overwriting ACL's is only allowed below and including the following level (default=5)
   * See also [[io.smartdatalake.util.misc.AclUtil]]
   */
  var hdfsAclsMinLevelPermissionOverwrite: Int = {
    EnvironmentUtil.getSdlParameter("hdfsAclsMinLevelPermissionOverwrite")
      .map(_.toInt).getOrElse(5)
  }

  /**
   * Limit setting ACL's to Basedir (default=true)
   * See hdfsAclsUserHomeLevel or hdfsBasedir on how the basedir is determined
   */
  var hdfsAclsLimitToBasedir: Boolean = {
    EnvironmentUtil.getSdlParameter("hdfsAclsLimitToBasedir")
      .map(_.toBoolean).getOrElse(true)
  }

  /**
   * Set path level of user home to determine basedir automatically (Default=2 -> /user/myUserHome)
   */
  var hdfsAclsUserHomeLevel: Int = {
    EnvironmentUtil.getSdlParameter("hdfsAclsUserHomeLevel")
      .map(_.toInt).getOrElse(2)
  }

  /**
   * Set basedir explicitly.
   * This overrides automatically detected user home for acl constraints by hdfsAclsUserHomeLevel.
   */
  var hdfsBasedir: Option[URI] = {
    EnvironmentUtil.getSdlParameter("hdfsBasedir")
      .map(new URI(_))
  }

  /**
   * Set default hadoop schema and authority for path
   */
  var hadoopDefaultSchemeAuthority: Option[URI] = {
    EnvironmentUtil.getSdlParameter("hadoopDefaultSchemeAuthority")
      .map(new URI(_))
  }

  /**
   * Set to true to enable check for duplicate first class object definitions when loading configuration (default=true).
   * The check fails if Connections, DataObjects or Actions are defined in multiple locations.
   */
  var enableCheckConfigDuplicates: Boolean = {
    EnvironmentUtil.getSdlParameter("enableCheckConfigDuplicates")
      .map(_.toBoolean).getOrElse(true)
  }

  /**
   * ordering of columns in SchemaEvolution result
   * - true: result schema is ordered according to existing schema, new columns are appended
   * - false: result schema is ordered according to new schema, deleted columns are appended
   */
  var schemaEvolutionNewColumnsLast: Boolean = {
    EnvironmentUtil.getSdlParameter("schemaEvolutionNewColumnsLast")
      .map(_.toBoolean).getOrElse(true)
  }

  /**
   * If `true`, schema validation does not consider nullability of columns/fields when checking for equality.
   * If `false`, schema validation considers two columns/fields different when their nullability property is not equal.
   */
  var schemaValidationIgnoresNullability: Boolean = {
    EnvironmentUtil.getSdlParameter("schemaValidationIgnoresNullability")
      .map(_.toBoolean).getOrElse(true)
  }

  /**
   * If `true`, schema validation inspects the whole hierarchy of structured data types. This allows partial matches
   * for `schemaMin` validation.
   * If `false`, structural data types must match exactly to validate.
   *
   * @example Using [[io.smartdatalake.workflow.dataobject.SchemaValidation.validateSchemaMin]]:
   *          val schema = StructType.fromDDL("c1 STRING, c2 STRUCT(c2_1 INT, c2_2 STRING)") validates
   *          against StructType.fromDDL("c1 STRING, c2 STRUCT(c2_1 INT)") only if `schemaValidationDeepComarison == true`.
   */
  var schemaValidationDeepComarison: Boolean = {
    EnvironmentUtil.getSdlParameter("schemaValidationDeepComarison")
      .map(_.toBoolean).getOrElse(true)
  }

  /**
   * Set to true if you want to enable automatic caching of DataFrames that are used multiple times (default=true).
   */
  var enableAutomaticDataFrameCaching: Boolean = {
    EnvironmentUtil.getSdlParameter("enableAutomaticDataFrameCaching")
      .map(_.toBoolean).getOrElse(true)
  }

  /**
   * Set to true if you want to enable workaround to overwrite unpartitioned SparkFileDataObject on Azure ADLSv2 (default=false).
   */
  var enableOverwriteUnpartitionedSparkFileDataObjectAdls: Boolean = {
    EnvironmentUtil.getSdlParameter("enableOverwriteUnpartitionedSparkFileDataObjectAdls")
      .map(_.toBoolean).getOrElse(false)
  }

  /**
   * Set log level for exceptions about skipped Actions, e.g. NoDataToProcessWarning (default=info).
   */
  var taskSkippedExceptionLogLevel: Level = {
    EnvironmentUtil.getSdlParameter("taskSkippedExceptionLogLevel")
      .map(x => Level.valueOf(x.toLowerCase)).getOrElse(Level.INFO)
  }

  /**
   * Simplify final exception for better usability of log
   * - truncate stacktrace starting from "monix.*" entries
   * - limit logical plan in AnalysisException to 5 lines
   */
  var simplifyFinalExceptionLog: Boolean = {
    EnvironmentUtil.getSdlParameter("simplifyFinalExceptionLog")
      .map(_.toBoolean).getOrElse(true)
  }

  /**
   * Number of Executions to keep runtime data for in streaming mode (default = 10).
   * Must be bigger than 1.
   */
  var runtimeDataNumberOfExecutionsToKeep: Int = {
    val nb = EnvironmentUtil.getSdlParameter("runtimeDataNumberOfExecutionsToKeep")
      .map(_.toInt).getOrElse(10)
    assert(nb>1, "runtimeDataNumberOfExecutionsToKeep must be bigger than 1.")
    // return
    nb
  }

  /**
   * If enabled the temp view name from versions <= 2.2.x is replaced with the new temp view name including a postfix.
   * This is enabled by default for backward compatibility.
   */
  var replaceSqlTransformersOldTempViewName: Boolean = {
    EnvironmentUtil.getSdlParameter("replaceSqlTransformersOldTempViewName").forall(_.toBoolean)
  }

  // static configurations
  val configPathsForLocalSubstitution: Seq[String] = Seq(
      "path", "table.name"
    , "create-sql", "createSql", "pre-read-sql", "preReadSql", "post-read-sql", "postReadSql", "pre-write-sql", "preWriteSql", "post-write-sql", "postWriteSql"
    , "executionMode.checkpointLocation", "execution-mode.checkpoint-location")
  val runIdPartitionColumnName = "run_id"

  // instantiate sdl plugin if configured
  private[smartdatalake] lazy val sdlPlugin: Option[SDLPlugin] = {
    EnvironmentUtil.getSdlParameter("pluginClassName")
      .map(CustomCodeUtil.getClassInstanceByName[SDLPlugin])
  }

  // dynamically shared environment for custom code (see also #106)
  // attention: if JVM is shared between different SDL jobs (e.g. Databricks cluster), these variables will be overwritten by the current job. Therefore they should not been used in SDL code, but might be used in custom code on your own risk.
  def sparkSession: SparkSession = _sparkSession
  private [smartdatalake] var _sparkSession: SparkSession = _
  def instanceRegistry: InstanceRegistry = _instanceRegistry
  private [smartdatalake] var _instanceRegistry: InstanceRegistry = _
  def globalConfig: GlobalConfig = _globalConfig
  private [smartdatalake] var _globalConfig: GlobalConfig = _

  // this class loader is needed to find custom classes in some environments (e.g. Polynote)
  def classLoader: ClassLoader = _classLoader
  private [smartdatalake] var _classLoader: ClassLoader = this.getClass.getClassLoader // initialize with default classloader

  // flag to gracefully stop repeated execution of DAG in streaming mode
  var stopStreamingGracefully: Boolean = false

  // This is for testing only: add state listener programmatically
  // Note: state listeners should be configured by global section of config files, see also [[GlobalConfig]]
  private[smartdatalake] var _additionalStateListeners: Seq[StateListener] = Seq()
}
