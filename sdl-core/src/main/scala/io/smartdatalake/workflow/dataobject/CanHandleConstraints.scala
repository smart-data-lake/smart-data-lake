/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.dataobject

/**
 * This trait defines the general approach to handle constraints such as primary and foreign keys
 * within a dataObject.
 */
trait CanHandleConstraints {
  /**
   * @param pkColumns List of columns in a primary key constraint
   * @param pkName Primary Key constraint name. It can be null, since some databases have constraints without names.
   */
  case class PrimaryKeyDefinition(pkColumns: Seq[String], pkName: Option[String] = None)

  def getExistingPKConstraint(catalog: String, schema: String, tableName: String): Option[PrimaryKeyDefinition]

  def getDefinedPKConstraint(): Option[Seq[String]]

  def getDefinedPKConstraintName(): String

  def dropPrimaryKeyConstraint(tableName: String, constraintName: String): Unit

  def createPrimaryKeyConstraint(tableName: String, constraintName: String, cols: Seq[String]): Unit

  //TODO
  def createOrReplacePrimaryKeyConstraint(): Unit = ???

}
