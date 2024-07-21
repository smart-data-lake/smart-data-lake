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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.SchemaViolationException

/**
 * A [[DataObject]] that allows for optional schema validation on read and on write.
 */
trait SchemaValidation { this: DataObject =>

  /**
   * An optional, minimal schema that a [[DataObject]] schema must have to pass schema validation.
   *
   * The schema validation semantics are:
   * - Schema A is valid in respect to a minimal schema B when B is a subset of A. This means: the whole column set of B is contained in the column set of A.
   *  - A column of B is contained in A when A contains a column with equal name and data type.
   *  - Column order is ignored.
   *  - Column nullability is ignored.
   *  - Duplicate columns in terms of name and data type are eliminated (set semantics).
   *
   * Note: This is mainly used by the functionality defined in [[CanCreateDataFrame]] and [[CanWriteDataFrame]], that is,
   * when reading or writing Spark data frames from/to the underlying data container.
   * [[io.smartdatalake.workflow.action.Action]]s that work with files ignore the `schemaMin` attribute
   * if it is defined.
   * Additionally schemaMin can be used to define the schema used if there is no data or table doesn't yet exist.
   */
  def schemaMin: Option[GenericSchema]

  /**
   * Validate the schema of a given Data Frame `df` against `schemaMin`.
   *
   * @param schema The schema to validate.
   * @param role role used in exception message. Set to read or write.
   * @throws SchemaViolationException is the `schemaMin` does not validate.
   */
  def validateSchemaMin(schema: GenericSchema, role: String): Unit = {
    schemaMin.foreach { schemaExpected =>
      val missingCols = schemaExpected.diffSchema(schema)
      missingCols.foreach { missing =>
        throw new SchemaViolationException(
          s"""($id) DataFrame does not fulfil schemaMin on $role:
             | - missingCols=${missing.columns.mkString(", ")}
             | - schemaMin: ${schemaExpected.sql}
             | - schema: ${schema.sql}""".stripMargin)
      }
    }
  }

  /**
   * Validate the schema of a given Spark Data Frame `df` against a given expected schema.
   *
   * @param schema The schema to validate.
   * @param schemaExpected The expected schema to validate against.
   * @param role role used in exception message. Set to read or write.
   * @throws SchemaViolationException if the `schema` does not validate.
   */
  def validateSchema(schema: GenericSchema, schemaExpected: GenericSchema, role: String): Unit = {
    val missingCols = schemaExpected.diffSchema(schema)
    val superfluousCols = schema.diffSchema(schemaExpected)
    if (missingCols.isDefined || superfluousCols.isDefined) {
      throw new SchemaViolationException(
        s"""($id) Schema does not match schema defined on $role:
           | - missingCols=${missingCols.map(_.columns).getOrElse(Seq()).mkString(", ")}
           | - superfluousCols=${superfluousCols.map(_.columns).getOrElse(Seq()).mkString(", ")}
           | - schemaExpected: ${schemaExpected.sql}
           | - schema: ${schema.sql}""".stripMargin)
    }
  }
}
