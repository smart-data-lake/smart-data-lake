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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext

import java.sql.SQLException


/**
 * @param pkColumns List of columns in a primary key constraint
 * @param pkName    Primary Key constraint name. It can be null, since some databases have constraints without names.
 */
case class PrimaryKeyDefinition(pkColumns: Seq[String], pkName: Option[String] = None)

/**
 * @param constraintName the name of the foreign key constraint in the database
 * @param localColumns   local columns participating in the FK, ordered by KEY_SEQ / ORDINAL_POSITION
 * @param refCatalog     catalog of the referenced table, if any
 * @param refSchema      schema/database of the referenced table, if any
 * @param refTable       name of the referenced table
 * @param refColumns     columns in the referenced table, parallel to localColumns
 */
case class ForeignKeyDefinition(
  constraintName: String,
  localColumns: Seq[String],
  refCatalog: Option[String],
  refSchema: Option[String],
  refTable: String,
  refColumns: Seq[String]
)

/**
 * This trait defines the general approach to handle referential keys (primary keys and foreign keys)
 * within a TransactionalTableDataObject.
 *
 * Provisioning is triggered when `table.createAndReplaceReferentialKeys = true`.
 * Call `createOrReplaceReferentialKeys` from `postWrite` (or `prepare` for JDBC).
 */
trait CanHandleReferentialKeys extends SmartDataLakeLogger { self: TransactionalTableDataObject =>

  // ── Abstract: Primary Key ──────────────────────────────────────────────

  def getExistingPKConstraint(catalog: Option[String], schema: Option[String], tableName: String)
      (implicit context: ActionPipelineContext): Option[PrimaryKeyDefinition]

  def dropPrimaryKeyConstraint(tableName: String, constraintName: String)
      (implicit context: ActionPipelineContext): Unit

  def createPrimaryKeyConstraint(tableName: String, constraintName: String, cols: Seq[String])
      (implicit context: ActionPipelineContext): Unit

  // ── Abstract: NOT NULL enforcement ────────────────────────────────────

  /**
   * Ensures the given columns are NOT NULL in the target table.
   * Called automatically before any PK constraint is created or recreated.
   */
  def ensureColumnsNotNull(tableName: String, columns: Seq[String])
      (implicit context: ActionPipelineContext): Unit

  // ── Abstract: Foreign Key ─────────────────────────────────────────────

  def getExistingFKConstraints(catalog: Option[String], schema: Option[String], tableName: String)
      (implicit context: ActionPipelineContext): Seq[ForeignKeyDefinition]

  def dropForeignKeyConstraint(tableName: String, constraintName: String)
      (implicit context: ActionPipelineContext): Unit

  def createForeignKeyConstraint(tableName: String, constraintName: String,
      localColumns: Seq[String], refFullTableName: String, refColumns: Seq[String])
      (implicit context: ActionPipelineContext): Unit

  // ── Public entry-point ────────────────────────────────────────────────

  /**
   * Creates or replaces both the primary key and all foreign key constraints according to the
   * current `table` configuration.  Calls `ensureColumnsNotNull` before any PK DDL.
   */
  def createOrReplaceReferentialKeys(implicit context: ActionPipelineContext): Unit = {
    if (table.primaryKey.isDefined) createOrReplacePrimaryKeyConstraint
    table.foreignKeys.filter(_.nonEmpty).foreach { _ => createOrReplaceForeignKeyConstraints }
  }

  // ── Private helpers ───────────────────────────────────────────────────

  private val pkConstraintName: String =
    table.primaryKeyConstraintName.getOrElse(s"sdlb_${table.name}_pk")

  private def createOrReplacePrimaryKeyConstraint(implicit context: ActionPipelineContext): Unit = {
    val definedPk: Option[Seq[String]] = table.primaryKey
    val existingPk: Option[PrimaryKeyDefinition] = getExistingPKConstraint(table.catalog, table.db, table.name)
    (definedPk, existingPk) match {
      case (None, _) =>
        logger.warn(s"$id: createAndReplaceReferentialKeys=true but no primaryKey columns defined — skipping PK")
      case (Some(pkcols), None) =>
        ensureColumnsNotNull(table.fullName, pkcols)
        createPrimaryKeyConstraint(table.fullName, pkConstraintName, pkcols)
      case (Some(definedPkCols), Some(existingPkDef))
          if !definedPkCols.map(_.toLowerCase).toSet.diff(existingPkDef.pkColumns.map(_.toLowerCase).toSet).isEmpty =>
        if (existingPkDef.pkName.isEmpty)
          throw new SQLException(s"$id: Existing primary key on ${table.fullName} has no constraint name — cannot update it automatically")
        dropPrimaryKeyConstraint(table.fullName, existingPkDef.pkName.get)
        ensureColumnsNotNull(table.fullName, definedPkCols)
        createPrimaryKeyConstraint(table.fullName, pkConstraintName, definedPkCols)
      case _ => // columns match — no-op
    }
  }

  private def createOrReplaceForeignKeyConstraints(implicit context: ActionPipelineContext): Unit = {
    val definedFKs: Seq[ForeignKey] = table.foreignKeys.getOrElse(Seq.empty)
    if (definedFKs.isEmpty) {
      logger.warn(s"$id: createAndReplaceReferentialKeys=true but no foreignKeys defined — skipping FK")
      return
    }

    // Validate uniqueness of resolved constraint names
    val resolvedNames = definedFKs.map(fk => fkConstraintName(fk))
    val duplicates = resolvedNames.groupBy(identity).filter(_._2.size > 1).keys.toSeq
    if (duplicates.nonEmpty)
      throw new ConfigurationException(
        s"$id: Duplicate foreign key constraint names: ${duplicates.mkString(", ")}. " +
        s"Set explicit 'name' on each ForeignKey when multiple FKs reference the same table.")

    val existingFKs: Seq[ForeignKeyDefinition] =
      getExistingFKConstraints(table.catalog, table.db, table.name)

    // Drop FKs that are absent or have changed
    existingFKs.foreach { existing =>
      val stillDefined = definedFKs.find(fk => fkConstraintName(fk).equalsIgnoreCase(existing.constraintName))
      val changed = stillDefined.exists(fk => !fkMatchesDefinition(existing, fk))
      if (stillDefined.isEmpty || changed) {
        logger.info(s"$id: Dropping FK constraint '${existing.constraintName}' (removed or changed)")
        dropForeignKeyConstraint(table.fullName, existing.constraintName)
      }
    }

    // Create FKs that are missing or were just dropped
    definedFKs.foreach { fk =>
      val alreadyExists = existingFKs.exists { existing =>
        fkConstraintName(fk).equalsIgnoreCase(existing.constraintName) && fkMatchesDefinition(existing, fk)
      }
      if (!alreadyExists) {
        val name = fkConstraintName(fk)
        val localCols = fk.columns.keys.toSeq.sorted
        val refCols = localCols.map(fk.columns)
        val refFullName = buildRefTableFullName(fk)
        logger.info(s"$id: Creating FK constraint '$name' referencing $refFullName")
        createForeignKeyConstraint(table.fullName, name, localCols, refFullName, refCols)
      }
    }
  }

  private def fkConstraintName(fk: ForeignKey): String =
    fk.name.getOrElse(s"sdlb_${table.name}_fk_${fk.table}")

  private def buildRefTableFullName(fk: ForeignKey): String =
    Seq(fk.catalog.orElse(table.catalog), fk.db.orElse(table.db), Some(fk.table)).flatten.mkString(".")

  private def fkMatchesDefinition(existing: ForeignKeyDefinition, fk: ForeignKey): Boolean = {
    val localCols = fk.columns.keys.toSeq.sorted
    val refCols = localCols.map(fk.columns)
    val refCatalog = fk.catalog.orElse(table.catalog)
    val refSchema = fk.db.orElse(table.db)
    existing.localColumns.map(_.toLowerCase).sorted == localCols.map(_.toLowerCase).sorted &&
    existing.refColumns.map(_.toLowerCase).sorted == refCols.map(_.toLowerCase).sorted &&
    existing.refTable.equalsIgnoreCase(fk.table) &&
    existing.refCatalog.map(_.toLowerCase) == refCatalog.map(_.toLowerCase) &&
    existing.refSchema.map(_.toLowerCase) == refSchema.map(_.toLowerCase)
  }
}
