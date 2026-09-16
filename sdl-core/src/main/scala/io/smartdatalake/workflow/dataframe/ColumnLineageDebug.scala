/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.workflow.dataframe

import io.smartdatalake.config.SdlConfigObject.DataObjectId

/**
 * Why the lineage of the columns listed in [[ColumnLineage.unresolvedColumns]] could not be traced back,
 * collected if `Environment.columnLineageDebug` is enabled and exported as an additional debug file, see
 * [[io.smartdatalake.app.TestMode.DryRunWithLineageExport]].
 *
 * This is diagnostic output for developing the lineage extraction itself and not part of the exported
 * lineage: it describes engine internals, e.g. Sparks logical plan, and its format is not stable.
 *
 * @param engine            name of the engine which analyzed the DataFrame, e.g. `Spark`.
 * @param inputs            the columns of the input DataObjects the lineage is traced back to.
 * @param unresolvedColumns the columns which could not be traced back completely, and where they dead-ended.
 * @param plan              the plan the lineage was read from, line by line, if the engine has one.
 */
case class ColumnLineageDebug(
    engine: String,
    inputs: Seq[ColumnLineageDebugInput],
    unresolvedColumns: Seq[ColumnLineageDebugColumn],
    plan: Seq[String] = Seq()
)

/**
 * The columns of an input DataObject the lineage of an output column can be traced back to.
 *
 * @param dataObjectId     id of the input DataObject.
 * @param columns          its columns, with the id identifying them inside the engine, e.g. `city#123`.
 * @param columnsNotInPlan the columns which do not occur in the plan of the output DataFrame at all. If a
 *                         column is listed here although it is read by the Action, the engine has replaced
 *                         its id on the way to the output DataFrame, which makes it impossible to trace back.
 */
case class ColumnLineageDebugInput(dataObjectId: DataObjectId, columns: Seq[String], columnsNotInPlan: Seq[String] = Seq())

/**
 * An output column which could not be traced back completely.
 *
 * @param column   name of the column.
 * @param deadEnds the places where following its lineage stopped. A column can dead-end more than once, as it
 *                 can be calculated from several columns.
 */
case class ColumnLineageDebugColumn(column: String, deadEnds: Seq[ColumnLineageDebugDeadEnd])

/**
 * A column which could neither be attributed to an input DataObject nor traced back any further.
 *
 * @param attribute      the column where following the lineage stopped, e.g. `city#123`.
 * @param path           the columns followed to get there, starting at the output column.
 * @param producedBy     type of the plan node creating `attribute`, if it could be located. This is the node
 *                       type the lineage extraction does not handle yet.
 * @param producedByNode description of that plan node.
 */
case class ColumnLineageDebugDeadEnd(
    attribute: String,
    path: Seq[String],
    producedBy: Option[String] = None,
    producedByNode: Option[String] = None
)
