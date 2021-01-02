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

import java.net.URI

import io.smartdatalake.app.GlobalConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.util.misc.EnvironmentUtil
import org.apache.spark.sql.SparkSession

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
   * Set to true if you want table and database names to be case sensitive when loading over JDBC.
   * If your database supports case sensitive table names and you want to use that feature, set this to true.
   */
  var enableJdbcCaseSensitivity: Boolean = {
    EnvironmentUtil.getSdlParameter("enableJdbcCaseSensitivity")
      .map(_.toBoolean).getOrElse(false)
  }

  // static configurations
  val configPathsForLocalSubstitution: Seq[String] = Seq(
      "path", "table.name"
    , "create-sql", "createSql", "pre-read-sql", "preReadSql", "post-read-sql", "postReadSql", "pre-write-sql", "preWriteSql", "post-write-sql", "postWriteSql"
    , "executionMode.checkpointLocation", "execution-mode.checkpoint-location")
  val defaultPathSeparator: Char = '/'

  // dynamically shared environment for custom code (see also #106)
  def sparkSession: SparkSession = _sparkSession
  private [smartdatalake] var _sparkSession: SparkSession = _
  def instanceRegistry: InstanceRegistry = _instanceRegistry
  private [smartdatalake] var _instanceRegistry: InstanceRegistry = _
  def globalConfig: GlobalConfig = _globalConfig
  private [smartdatalake] var _globalConfig: GlobalConfig = _

}
