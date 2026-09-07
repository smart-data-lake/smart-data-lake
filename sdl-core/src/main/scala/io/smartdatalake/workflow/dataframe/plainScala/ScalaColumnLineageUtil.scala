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
package io.smartdatalake.workflow.dataframe.plainScala

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataframe.{ColumnLineage, ColumnLineageField, ColumnLineageInputField, ColumnTransformation}

import scala.collection.mutable

/**
 * Extract the column level lineage of a [[ScalaDataFrame]], see issue #867.
 * This is the plain-Scala engines counterpart of `SparkColumnLineageUtil`, which does the same for Spark.
 *
 * The plain-Scala engine evaluates every expression immediately, so there is no logical plan to read the
 * lineage from afterwards. Instead every column carries the provenance of its values, which is recorded while
 * the column is created, see [[ScalaColumnProvenance]]. Following the provenance of the output columns of the
 * DataFrame down to the columns of the given input DataFrames therefore gives the column level lineage.
 *
 * As in the Spark implementation, SDLB does not need to recognize the data sources: an Action knows the
 * DataFrames of its input DataObjects, so the columns to trace back to are known upfront. A column of an input
 * DataObject is a leaf of the lineage of this Action, even if it carries the provenance of a previous Action
 * whose output was passed on as a DataFrame.
 *
 * A column which is not created from an input column at all, e.g. a constant, is reported without input
 * columns. A column which could not be traced back completely is reported as unresolved instead, so that a
 * consumer of the lineage can tell the two apart.
 *
 * Limitations, next to the ones of the engine itself (see [[ScalaSubFeed]]):
 * - Only DIRECT lineage is detected, as in the Spark implementation. Columns used in join, filter, group by
 *   and sort conditions influence the output without being part of its value, and are reported as INDIRECT by
 *   OpenLineage. The same holds for an aggregation over all rows, e.g. count(*), which depends on the input
 *   dataset but not on one of its columns.
 * - A column of a DataFrame that is created from data inside a transformation, e.g. by
 *   `ScalaDataFrame.fromData`, has no provenance and is therefore reported as unresolved.
 */
private[smartdatalake] object ScalaColumnLineageUtil extends SmartDataLakeLogger {

  /**
   * Maximum length of the description of a transformation, which is the expression defining a column.
   * Expressions can be arbitrarily large, so they are cut off to keep the export readable.
   */
  private val maxDescriptionLength = 200

  /**
   * Extract the column level lineage of `df` with respect to the given input DataObjects.
   *
   * @param df     the DataFrame to analyze, e.g. the DataFrame written to an output DataObject.
   * @param inputs the DataFrames read from the input DataObjects this Action is processing.
   * @return the columns of `df` which could be traced back to a column of an input DataObject.
   */
  def extractColumnLineage(df: ScalaDataFrame, inputs: Seq[(DataObjectId, ScalaDataFrame)]): ColumnLineage = {
    val sources = collectSources(inputs)
    if (sources.isEmpty) return ColumnLineage.empty
    val resolutions = df.cols
      .groupBy(_.definition.name)
      .toSeq
      .map {
        case (name, columns) => (name, columns.map(c => resolve(c.definition.provenance, sources)).reduce(_ combineWith _))
      }
      .sortBy(_._1)
    val fields = resolutions.collect {
      // a column which is traced back completely, or which is created without reading any input column
      case (name, resolution) if resolution.isComplete =>
        ColumnLineageField(name, resolution.inputFields, if (resolution.inputFields.isEmpty) resolution.expression else None)
    }
    val unresolvedColumns = resolutions.collect { case (name, resolution) if !resolution.isComplete => name }
    if (unresolvedColumns.nonEmpty) {
      logger.debug(s"Could not trace back all columns of the DataFrame, lineage is incomplete for ${unresolvedColumns.mkString(", ")}")
    }
    ColumnLineage(fields, unresolvedColumns)
  }

  /**
   * Map the provenance of every column of an input DataFrame to the DataObject and column it belongs to.
   * The map is keyed by identity, as a [[ScalaColumnProvenance]] is compared by reference.
   *
   * A column can belong to more than one input DataObject: an Action which passes its input through unchanged
   * hands the very same columns to the next Action, so if that Action reads the original DataObject as well,
   * both inputs share the provenance. Such a column is reported for every input it belongs to, as there is no
   * way to tell which one it was read from - and both are true.
   */
  private def collectSources(inputs: Seq[(DataObjectId, ScalaDataFrame)]): Map[ScalaColumnProvenance, Seq[(DataObjectId, String)]] = {
    inputs
      .flatMap {
        case (dataObjectId, df) => df.cols.map(c => c.definition.provenance -> (dataObjectId, c.definition.name))
      }
      .groupMap(_._1)(_._2)
  }

  /**
   * Follow the provenance of a column down to the columns of the input DataObjects.
   *
   * A column can depend on the same input column over multiple paths, e.g. `concat(a, upper(a))`. Such paths
   * are combined into one input field, keeping the transformation of the first path which is not an identity.
   */
  private def resolve(
      provenance: ScalaColumnProvenance,
      sources: Map[ScalaColumnProvenance, Seq[(DataObjectId, String)]]
  ): Resolution = {
    val inputFields = mutable.LinkedHashMap[(DataObjectId, String), ColumnTransformation]()
    var expression: Option[String] = None
    var isComplete = true
    def go(provenance: ScalaColumnProvenance, isIdentity: Boolean, description: Option[String], visited: Set[ScalaColumnProvenance]): Unit = {
      if (visited.contains(provenance)) return
      val source = sources.get(provenance)
      source.foreach(_.foreach {
        case (dataObjectId, column) =>
          val transformation = ColumnTransformation.direct(isIdentity, description.map(truncate))
          inputFields.updateWith((dataObjectId, column)) {
            case Some(existing) if existing.subtype != ColumnTransformation.Identity => Some(existing)
            case _ => Some(transformation)
          }
      })
      // A column of an input DataObject is a leaf of the lineage of this Action. Its own provenance describes
      // how the input DataObject created it and belongs to the Action which wrote it.
      if (source.isEmpty) {
        if (provenance.references.nonEmpty) {
          provenance.references.foreach { reference =>
            go(reference, isIdentity && provenance.isIdentity, description.orElse(provenance.description), visited + provenance)
          }
        // a column of a DataFrame which is neither an input DataObject nor calculated, so its source is unknown
        } else if (provenance.isRoot) {
          isComplete = false
        // the column is calculated without reading any column, e.g. from a literal
        } else if (expression.isEmpty) {
          expression = description.orElse(provenance.description).map(truncate)
        }
      }
    }
    go(provenance, isIdentity = true, None, Set())
    val sortedInputFields = inputFields.toSeq
      .map { case ((dataObjectId, column), transformation) => ColumnLineageInputField(dataObjectId, column, transformation) }
      .sortBy(f => (f.dataObjectId.id, f.column))
    Resolution(sortedInputFields, expression, isComplete)
  }

  private def truncate(description: String): String = {
    if (description.length > maxDescriptionLength) description.take(maxDescriptionLength - 3) + "..." else description
  }

  /**
   * The result of tracing one column back to the columns of the input DataObjects.
   *
   * @param inputFields the input columns found, empty if the column is not created from an input column.
   * @param expression  the expression defining the column, if it is not created from an input column.
   * @param isComplete  false if a column was reached which could neither be traced back further nor attributed
   *                    to an input DataObject. The lineage of such a column is unknown, not empty.
   */
  private case class Resolution(inputFields: Seq[ColumnLineageInputField], expression: Option[String], isComplete: Boolean) {
    // several columns of a DataFrame can have the same name, in which case their lineage is combined
    def combineWith(other: Resolution): Resolution = Resolution(
      (inputFields ++ other.inputFields).distinctBy(f => (f.dataObjectId, f.column)),
      expression.orElse(other.expression),
      isComplete && other.isComplete
    )
  }
}
