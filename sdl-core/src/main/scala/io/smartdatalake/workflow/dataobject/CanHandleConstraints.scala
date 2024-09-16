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

import io.smartdatalake.util.misc.SmartDataLakeLogger

import java.sql.SQLException


case class PrimaryKeyDefinition(pkColumns: Seq[String], pkName: Option[String] = None)

/**
 * This trait defines the general approach to handle constraints such as primary and foreign keys
 * within a TransactionalTableDataObject.
 */
trait CanHandleConstraints { self: TransactionalTableDataObject =>
  /**
   * @param pkColumns List of columns in a primary key constraint
   * @param pkName Primary Key constraint name. It can be null, since some databases have constraints without names.
   */
  def getExistingPKConstraint(catalog: String, schema: String, tableName: String): Option[PrimaryKeyDefinition]
  def dropPrimaryKeyConstraint(tableName: String, constraintName: String): Unit
  def createPrimaryKeyConstraint(tableName: String, constraintName: String, cols: Seq[String]): Unit

  private val pkConstraintName: String = table.primaryKeyConstraintName.getOrElse(f"sdlb_${table.name}")

  def createOrReplacePrimaryKeyConstraint(): Unit = {
    val definedPrimaryKeyOp: Option[Seq[String]] = table.primaryKey
    val existingPrimaryKeyOp: Option[PrimaryKeyDefinition] =  getExistingPKConstraint(table.catalog.getOrElse(""), table.db.getOrElse(""), table.name)
    (definedPrimaryKeyOp, existingPrimaryKeyOp) match {
      case (None, _) => logger.warn(f"$id parameter createAndReplacePrimaryKey not needed as there are no primary Key columns defined!")
      case (Some(pkcols), None) => createPrimaryKeyConstraint(table.fullName, pkConstraintName, pkcols)
      case (Some(definedPkCols), Some(existingPkCols)) if (definedPkCols.toSet.diff(existingPkCols.pkColumns.toSet).isEmpty) => {
        if (existingPkCols.pkName.isEmpty) throw new SQLException(f"$id: The Primary key in the database already has some columns, but the constraint name returned by the database is null. The PK cannot be updated!")
        dropPrimaryKeyConstraint(table.fullName, existingPkCols.pkName.get)
        createPrimaryKeyConstraint(table.fullName, pkConstraintName, definedPkCols)
      }
    }
  }

}
