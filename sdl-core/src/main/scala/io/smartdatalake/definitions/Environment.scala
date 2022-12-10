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
 * - by the global.environment configuration file section
 * - by a custom [[io.smartdatalake.app.SmartDataLakeBuilder]] implementation for your environment, which sets these variables directly.
 */
object Environment {

  // this class loader needs to be overridden to find custom classes in some environments (e.g. Polynote)
  def classLoader(): ClassLoader = {
    if (_classLoader.isEmpty) {
      _classLoader = Option(this.getClass.getClassLoader)
        .orElse(Option(ClassLoader.getSystemClassLoader))
    }
    _classLoader.get
  }
  var _classLoader: Option[ClassLoader] = None

  /**
   * List of hadoop authorities for which acls must be configured
   * The environment parameter can contain multiple authorities separated by comma.
   * An authority is compared against the filesystem URI with contains(...)
   */
  def hadoopAuthoritiesWithAclsRequired: Seq[String] = {
    if (_hadoopAuthoritiesWithAclsRequired.isEmpty) {
      _hadoopAuthoritiesWithAclsRequired = Some(
        EnvironmentUtil.getSdlParameter("hadoopAuthoritiesWithAclsRequired")
          .toSeq.flatMap(_.split(','))
      )
    }
    _hadoopAuthoritiesWithAclsRequired.get
  }
  var _hadoopAuthoritiesWithAclsRequired: Option[Seq[String]] = None

  /**
   * Modifying ACL's is only allowed below and including the following level (default=2)
   * See also [[io.smartdatalake.util.misc.AclUtil]]
   */
  def hdfsAclsMinLevelPermissionModify: Int = {
    if (_hdfsAclsMinLevelPermissionModify.isEmpty) {
      _hdfsAclsMinLevelPermissionModify = Some(
        EnvironmentUtil.getSdlParameter("hdfsAclsMinLevelPermissionModify")
          .map(_.toInt).getOrElse(2)
      )
    }
    _hdfsAclsMinLevelPermissionModify.get
  }
  var _hdfsAclsMinLevelPermissionModify: Option[Int] = None

  /**
   * Overwriting ACL's is only allowed below and including the following level (default=5)
   * See also [[io.smartdatalake.util.misc.AclUtil]]
   */
  def hdfsAclsMinLevelPermissionOverwrite: Int = {
    if (_hdfsAclsMinLevelPermissionOverwrite.isEmpty) {
      _hdfsAclsMinLevelPermissionOverwrite = Some(
        EnvironmentUtil.getSdlParameter("hdfsAclsMinLevelPermissionOverwrite")
         .map(_.toInt).getOrElse(5)
      )
    }
    _hdfsAclsMinLevelPermissionOverwrite.get
  }
  var _hdfsAclsMinLevelPermissionOverwrite: Option[Int] = None

  /**
   * Limit setting ACL's to Basedir (default=true)
   * See hdfsAclsUserHomeLevel or hdfsBasedir on how the basedir is determined
   */
  def hdfsAclsLimitToBasedir: Boolean = {
    if (_hdfsAclsLimitToBasedir.isEmpty) {
      _hdfsAclsLimitToBasedir = Some(
        EnvironmentUtil.getSdlParameter("hdfsAclsLimitToBasedir")
         .map(_.toBoolean).getOrElse(true)
      )
    }
    _hdfsAclsLimitToBasedir.get
  }
  var _hdfsAclsLimitToBasedir: Option[Boolean] = None

  /**
   * Set path level of user home to determine basedir automatically (Default=2 -> /user/myUserHome)
   */
  def hdfsAclsUserHomeLevel: Int = {
    if (_hdfsAclsLimitToBasedir.isEmpty) {
      _hdfsAclsUserHomeLevel = Some(
        EnvironmentUtil.getSdlParameter("hdfsAclsUserHomeLevel")
         .map(_.toInt).getOrElse(2)
      )
    }
    _hdfsAclsUserHomeLevel.get
  }
  var _hdfsAclsUserHomeLevel: Option[Int] = None

  /**
   * Set basedir explicitly.
   * This overrides automatically detected user home for acl constraints by hdfsAclsUserHomeLevel.
   */
  def hdfsBasedir: Option[URI] = {
    if (_hdfsBasedir.isEmpty) {
      _hdfsBasedir = Some(
        EnvironmentUtil.getSdlParameter("hdfsBasedir")
        .map(new URI(_))
      )
    }
    _hdfsBasedir.get
  }
  var _hdfsBasedir: Option[Option[URI]] = None

  /**
   * Set default hadoop schema and authority for path
   */
  def hadoopDefaultSchemeAuthority: Option[URI] = {
    if (_hadoopDefaultSchemeAuthority.isEmpty) {
      _hadoopDefaultSchemeAuthority = Some(
        EnvironmentUtil.getSdlParameter("hadoopDefaultSchemeAuthority")
          .map(new URI(_))
      )
    }
    _hadoopDefaultSchemeAuthority.get
  }
  var _hadoopDefaultSchemeAuthority: Option[Option[URI]] = None

  /**
   * Set to true to enable check for duplicate first class object definitions when loading configuration (default=true).
   * The check fails if Connections, DataObjects or Actions are defined in multiple locations.
   */
  def enableCheckConfigDuplicates: Boolean = {
    if (_enableCheckConfigDuplicates.isEmpty) {
      _enableCheckConfigDuplicates = Some(
        EnvironmentUtil.getSdlParameter("enableCheckConfigDuplicates")
          .map(_.toBoolean).getOrElse(true)
      )
    }
    _enableCheckConfigDuplicates.get
  }
  var _enableCheckConfigDuplicates: Option[Boolean] = None

  /**
   * ordering of columns in SchemaEvolution result
   * - true: result schema is ordered according to existing schema, new columns are appended
   * - false: result schema is ordered according to new schema, deleted columns are appended
   */
  def schemaEvolutionNewColumnsLast: Boolean = {
    if (_schemaEvolutionNewColumnsLast.isEmpty) {
      _schemaEvolutionNewColumnsLast = Some(
        EnvironmentUtil.getSdlParameter("schemaEvolutionNewColumnsLast")
          .map(_.toBoolean).getOrElse(true)
      )
    }
    _schemaEvolutionNewColumnsLast.get
  }
  var _schemaEvolutionNewColumnsLast: Option[Boolean] = None

  /**
   * If `true`, schema validation does not consider nullability of columns/fields when checking for equality.
   * If `false`, schema validation considers two columns/fields different when their nullability property is not equal.
   */
  def schemaValidationIgnoresNullability: Boolean = {
    if (_schemaValidationIgnoresNullability.isEmpty) {
      _schemaValidationIgnoresNullability = Some(
        EnvironmentUtil.getSdlParameter("schemaValidationIgnoresNullability")
          .map(_.toBoolean).getOrElse(true)
      )
    }
    _schemaValidationIgnoresNullability.get
  }
  var _schemaValidationIgnoresNullability: Option[Boolean] = None

  /**
   * If `true`, schema validation inspects the whole hierarchy of structured data types. This allows partial matches
   * for `schemaMin` validation.
   * If `false`, structural data types must match exactly to validate.
   *
   * @example Using [[io.smartdatalake.workflow.dataobject.SchemaValidation.validateSchemaMin]]:
   *          val schema = StructType.fromDDL("c1 STRING, c2 STRUCT(c2_1 INT, c2_2 STRING)") validates
   *          against StructType.fromDDL("c1 STRING, c2 STRUCT(c2_1 INT)") only if `schemaValidationDeepComarison == true`.
   */
  def schemaValidationDeepComarison: Boolean = {
    if (_schemaValidationDeepComarison.isEmpty) {
      _schemaValidationDeepComarison = Some(
        EnvironmentUtil.getSdlParameter("schemaValidationDeepComarison")
          .map(_.toBoolean).getOrElse(true)
      )
    }
    _schemaValidationDeepComarison.get
  }
  var _schemaValidationDeepComarison: Option[Boolean] = None

  /**
   * Set to true if you want to enable automatic caching of DataFrames that are used multiple times (default=true).
   */
  def enableAutomaticDataFrameCaching: Boolean = {
    if (_enableAutomaticDataFrameCaching.isEmpty) {
      _enableAutomaticDataFrameCaching = Some(
        EnvironmentUtil.getSdlParameter("enableAutomaticDataFrameCaching")
          .map(_.toBoolean).getOrElse(true)
      )
    }
    _enableAutomaticDataFrameCaching.get
  }
  var _enableAutomaticDataFrameCaching: Option[Boolean] = None

  /**
   * Set to true if you want to enable workaround to overwrite unpartitioned SparkFileDataObject on Azure ADLSv2 (default=false).
   */
  def enableOverwriteUnpartitionedSparkFileDataObjectAdls: Boolean = {
    EnvironmentUtil.getSdlParameter("enableOverwriteUnpartitionedSparkFileDataObjectAdls")
      .map(_.toBoolean).getOrElse(false)
  }

  /**
   * Set log level for exceptions about skipped Actions, e.g. NoDataToProcessWarning (default=info).
   */
  def taskSkippedExceptionLogLevel: Level = {
    if (_taskSkippedExceptionLogLevel.isEmpty) {
      _taskSkippedExceptionLogLevel = Some(
        EnvironmentUtil.getSdlParameter("taskSkippedExceptionLogLevel")
          .map(x => Level.valueOf(x.toLowerCase)).getOrElse(Level.INFO)
      )
    }
    _taskSkippedExceptionLogLevel.get
  }
  var _taskSkippedExceptionLogLevel: Option[Level] = None

  /**
   * Simplify final exception for better usability of log
   * - truncate stacktrace starting from "monix.*" entries
   * - limit logical plan in AnalysisException to 5 lines
   */
  def simplifyFinalExceptionLog: Boolean = {
    if (_simplifyFinalExceptionLog.isEmpty) {
      _simplifyFinalExceptionLog = Some(
        EnvironmentUtil.getSdlParameter("simplifyFinalExceptionLog")
          .map(_.toBoolean).getOrElse(true)
      )
    }
    _simplifyFinalExceptionLog.get
  }
  var _simplifyFinalExceptionLog: Option[Boolean] = None

  /**
   * Number of Executions to keep runtime data for in streaming mode (default = 10).
   * Must be bigger than 1.
   */
  def runtimeDataNumberOfExecutionsToKeep: Int = {
    if (_runtimeDataNumberOfExecutionsToKeep.isEmpty) {
      _runtimeDataNumberOfExecutionsToKeep = Some(
        EnvironmentUtil.getSdlParameter("runtimeDataNumberOfExecutionsToKeep")
          .map(_.toInt).getOrElse(10)
      )
      assert(_runtimeDataNumberOfExecutionsToKeep.get > 1, "runtimeDataNumberOfExecutionsToKeep must be bigger than 1.")
    }
    _runtimeDataNumberOfExecutionsToKeep.get
  }
  var _runtimeDataNumberOfExecutionsToKeep: Option[Int] = None

  /**
   * If enabled the temp view name from versions <= 2.2.x is replaced with the new temp view name including a postfix.
   * This is enabled by default for backward compatibility.
   */
  def replaceSqlTransformersOldTempViewName: Boolean = {
    if (_replaceSqlTransformersOldTempViewName.isEmpty) {
      _replaceSqlTransformersOldTempViewName = Some(
        EnvironmentUtil.getSdlParameter("replaceSqlTransformersOldTempViewName")
          .map(_.toBoolean).getOrElse(true)
      )
    }
    _replaceSqlTransformersOldTempViewName.get
  }
  var _replaceSqlTransformersOldTempViewName: Option[Boolean] = None

  /**
   * If enabled, the sample data file for SparkFileDataObject is updated on every load from a file-based Action, otherwise it's just updated if it's missing.
   * The advantage of updating the sample file on every load is to enable automatic schema evolution.
   * This is disabled by default, as it might have performance impact if file size is big. It can be enabled on demand by setting the corresponding java property or environment variable.
   */
  def updateSparkFileDataObjectSampleDataFile: Boolean = {
    if (_updateSparkFileDataObjectSampleDataFile.isEmpty) {
      _updateSparkFileDataObjectSampleDataFile = Some(
        EnvironmentUtil.getSdlParameter("updateSparkFileDataObjectSampleDataFile")
          .map(_.toBoolean).getOrElse(false)
      )
    }
    _updateSparkFileDataObjectSampleDataFile.get
  }
  var _updateSparkFileDataObjectSampleDataFile: Option[Boolean] = None

  /**
   * If enabled, SparkFileDataObject checks in execution plan if there are files available during Exec phase.
   * NoDataToProcessWarning is thrown if there are no files found in the execution plan.
   */
  def enableSparkFileDataObjectNoDataCheck: Boolean = {
    if (_enableSparkFileDataObjectNoDataCheck.isEmpty) {
      _enableSparkFileDataObjectNoDataCheck = Some(
        EnvironmentUtil.getSdlParameter("enableSparkFileDataObjectNoDataCheck")
          .map(_.toBoolean).getOrElse(true)
      )
    }
    _enableSparkFileDataObjectNoDataCheck.get
  }
  var _enableSparkFileDataObjectNoDataCheck: Option[Boolean] = None

  /**
   * Maximal line length for DAG graph log, before switching to list mode.
   */
  def dagGraphLogMaxLineLength: Int = {
    if (_dagGraphLogMaxLineLength.isEmpty) {
      _dagGraphLogMaxLineLength = Some(
        EnvironmentUtil.getSdlParameter("dagGraphLogMaxLineLength")
          .map(_.toInt).getOrElse(250)
      )
    }
    _dagGraphLogMaxLineLength.get
  }
  var _dagGraphLogMaxLineLength: Option[Int] = None

  /**
   * If enabled, the schema file for SparkFileDataObject is updated on every load from a DataFrame-based Action, otherwise it's just updated if it's missing.
   * The advantage of updating the sample file on every load is to enable automatic schema evolution.
   * This is enabled by default, as it has not big impact on performance.
   */
  def updateSparkFileDataObjectSchemaFile: Boolean = {
    if (_updateSparkFileDataObjectSchemaFile.isEmpty) {
      _updateSparkFileDataObjectSchemaFile = Some(
        EnvironmentUtil.getSdlParameter("updateSparkFileDataObjectSchemaFile")
          .map(_.toBoolean).getOrElse(true)
      )
    }
    _updateSparkFileDataObjectSchemaFile.get
  }
  var _updateSparkFileDataObjectSchemaFile: Option[Boolean] = None

  /**
   * Parse schema files only when used, e.g. lazy.
   * This improve startup speed for large configurations, and can fix problems with schemas not being available for some locations.
   */
  def parseSchemaFilesLazy: Boolean = {
    if (_parseSchemaFilesLazy.isEmpty) {
      _parseSchemaFilesLazy = Some(
        EnvironmentUtil.getSdlParameter("parseSchemaFilesLazy")
          .map(_.toBoolean).getOrElse(false)
      )
    }
    _parseSchemaFilesLazy.get
  }
  var _parseSchemaFilesLazy: Option[Boolean] = None

  /**
   * Compile scala code of transformations only when used, e.g. lazy.
   * This improves startup speed for large configurations, and can fix problems with code files not being available for some locations.
   */
  def compileScalaCodeLazy: Boolean = {
    if (_compileScalaCodeLazy.isEmpty) {
      _compileScalaCodeLazy = Some(
        EnvironmentUtil.getSdlParameter("compileScalaCodeLazy")
          .map(_.toBoolean).getOrElse(false)
      )
    }
    _compileScalaCodeLazy.get
  }
  var _compileScalaCodeLazy: Option[Boolean] = None

  // static configurations
  def configPathsForLocalSubstitution: Seq[String] = Seq(
      "path", "table.name"
    , "create-sql", "createSql", "pre-read-sql", "preReadSql", "post-read-sql", "postReadSql", "pre-write-sql", "preWriteSql", "post-write-sql", "postWriteSql"
    , "executionMode.checkpointLocation", "execution-mode.checkpoint-location")
  val runIdPartitionColumnName = "run_id"

  // instantiate sdl plugin if configured
  private[smartdatalake] def sdlPlugin: Option[SDLPlugin] = {
    if (_sdlPlugin.isEmpty) {
      _sdlPlugin = Some(EnvironmentUtil.getSdlParameter("pluginClassName")
        .map(CustomCodeUtil.getClassInstanceByName[SDLPlugin]))

    }
    _sdlPlugin.get
  }
  private[smartdatalake] var _sdlPlugin: Option[Option[SDLPlugin]] = None

  // dynamically shared environment for custom code (see also #106)
  // attention: if JVM is shared between different SDL jobs (e.g. Databricks cluster), these variables will be overwritten by the current job. Therefore they should not been used in SDL code, but might be used in custom code on your own risk.
  def sparkSession: SparkSession = _sparkSession
  private [smartdatalake] var _sparkSession: SparkSession = _
  def instanceRegistry: InstanceRegistry = _instanceRegistry
  private [smartdatalake] var _instanceRegistry: InstanceRegistry = _
  def globalConfig: GlobalConfig = _globalConfig
  private [smartdatalake] var _globalConfig: GlobalConfig = _

  // flag to gracefully stop repeated execution of DAG in streaming mode
  var stopStreamingGracefully: Boolean = false

  // This is for testing only: add state listener programmatically
  // Note: state listeners should be configured by global section of config files, see also [[GlobalConfig]]
  private[smartdatalake] var _additionalStateListeners: Seq[StateListener] = Seq()
}
