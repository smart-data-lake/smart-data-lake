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

/**
 * Set this environment dependent configurations at the beginning of the [[io.smartdatalake.app.SmartDataLakeBuilder]] implementation for your environment.
 */
object Environment {

  /**
   * Set to true if configuration of acls for HadoopDataObjects is mandatory
   */
  var hdfsAclsRequired: Boolean = false

  /**
   * Modifying ACL's is only allowed below and including the following level (default=2)
   * See also [[io.smartdatalake.util.misc.AclUtil]]
   */
  var hdfsAclsMinLevelPermissionModify = 2 // up to user home

  /**
   * Overwriting ACL's is only allowed below and including the following level (default=5)
   * See also [[io.smartdatalake.util.misc.AclUtil]]
   */
  var hdfsAclsMinLevelPermissionOverwrite = 5 // incl. and underneath feed

  /**
   * Limit setting ACL's to user home (default=true)
   */
  var hdfsAclsLimitToUserHome = true
  var hdfsAclsUserHomeLevel = 2

  /**
   * Set default hadoop schema and authority for path
   */
  var hadoopDefaultSchemeAuthority: Option[URI] = None

  /**
   * ordering of columns in SchemaEvolution result
   * - true: result schema is ordered according to existing schema, new columns are appended
   * - false: result schema is ordered according to new schema, deleted columns are appended
   */
  var schemaEvolutionNewColumnsLast: Boolean = true

  /**
   * If `true`, schema validation does not consider nullability of columns/fields when checking for equality.
   * If `false`, schema validation considers two columns/fields different when their nullability property is not equal.
   */
  var schemaValidationIgnoresNullability: Boolean = true

  /**
   * If `true`, schema validation inspects the whole hierarchy of structured data types. This allows partial matches
   * for `schemaMin` validation.
   * If `false`, structural data types must match exactly to validate.
   *
   * @example Using [[io.smartdatalake.workflow.dataobject.SchemaValidation.validateSchemaMin]]:
   *          val schema = StructType.fromDDL("c1 STRING, c2 STRUCT(c2_1 INT, c2_2 STRING)") validates
   *          against StructType.fromDDL("c1 STRING, c2 STRUCT(c2_1 INT)") only if `schemaValidationDeepComarison == true`.
   */
  var schemaValidationDeepComarison: Boolean = true

  /**
   * Set to true if you want table and database names to be case sensitive when loading over JDBC.
   * If your database supports case sensitive table names and you want to use that feature, set this to true.
   */
  var enableJdbcCaseSensitivity: Boolean = false

  // static configurations
  val configPathsForLocalSubstitution: Seq[String] = Seq("path", "table.name", "create-sql", "createSql", "pre-sql", "preSql", "post-sql", "postSql")
  val defaultPathSeparator: Char = '/'
}
