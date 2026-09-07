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
package io.smartdatalake.util.spark

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataframe.{ColumnLineage, ColumnLineageField, ColumnLineageInputField, ColumnTransformation}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, Expression, ScalarSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Expand, Generate, LogicalPlan, ObjectProducer, SerializeFromObject, Union}

import scala.collection.mutable
import scala.util.Try

/**
 * Extract the column level lineage of a Spark DataFrame, see issue #867.
 *
 * The lineage is read from the analyzed logical plan of the DataFrame. Spark identifies every column of a plan
 * by an [[ExprId]], which is kept while a column is passed through the plan and is created anew whenever a column
 * is defined, e.g. by an [[Alias]]. Following these ids from the output columns of the DataFrame down to the
 * columns of the given input DataFrames therefore gives the column level lineage.
 *
 * Note that SDLB does not need to recognize the data sources in the plan, which is the difficult part of the
 * generic solutions like OpenLineage or Kyuubi: an Action knows the DataFrames of its input DataObjects, so the
 * columns to trace back to are known upfront.
 *
 * A column which is not created from an input column at all, e.g. a constant, is reported without input
 * columns. A column which could not be traced back completely is reported as unresolved instead, so that a
 * consumer of the lineage can tell the two apart.
 *
 * Limitations, to be addressed in a later step:
 * - Only DIRECT lineage is detected. Columns used in join, filter, group by, sort and window conditions
 *   influence the output without being part of its value, and are reported as INDIRECT by OpenLineage.
 *   The same holds for an aggregation over all rows, e.g. count(*), which depends on the input dataset but
 *   not on one of its columns.
 * - A column read from the same DataObject twice, e.g. in a self-join, is only traced back for one of the two
 *   occurrences, as Spark's analyzer replaces the duplicated expression ids. This also happens if an Action
 *   reads both the cached output of a previous Action (cacheOutput=true) and a DataObject which that Action
 *   passed through unchanged, as both inputs then share the same columns.
 * - Columns of a typed Dataset transformation, e.g. a map over a case class, are traced back to all columns
 *   read by that transformation, as the Scala function transforming them is opaque.
 */
private[smartdatalake] object SparkColumnLineageUtil extends SmartDataLakeLogger {

  /**
   * Maximum length of the description of a transformation, which is the SQL representation of the expression
   * defining a column. Expressions can be arbitrarily large, so they are cut off to keep the export readable.
   */
  private val maxDescriptionLength = 200

  /**
   * Extract the column level lineage of `df` with respect to the given input DataObjects.
   *
   * @param df     the DataFrame to analyze, e.g. the DataFrame written to an output DataObject.
   * @param inputs the DataFrames read from the input DataObjects this Action is processing.
   * @return the columns of `df` which could be traced back to a column of an input DataObject.
   */
  def extractColumnLineage(df: DataFrame, inputs: Seq[(DataObjectId, DataFrame)]): ColumnLineage = {
    val sources = collectSources(inputs)
    if (sources.isEmpty) return ColumnLineage.empty
    val plan = df.queryExecution.analyzed
    val definitions = collectDefinitions(plan)
    val resolutions = plan.output
      .groupBy(_.name)
      .toSeq
      .map {
        case (name, attributes) => (name, attributes.map(a => resolve(a.exprId, sources, definitions)).reduce(_ combineWith _))
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
   * Map the expression id of every column of an input DataFrame to the DataObject and column it belongs to.
   *
   * A column can belong to more than one input DataObject: an Action which passes its input through unchanged
   * and caches its output (cacheOutput=true) hands the very same columns to the next Action, so if that Action
   * reads the original DataObject as well, both inputs share the expression id. Such a column is reported for
   * every input it belongs to, as there is no way to tell which one it was read from - and both are true.
   */
  private def collectSources(inputs: Seq[(DataObjectId, DataFrame)]): Map[ExprId, Seq[(DataObjectId, String)]] = {
    inputs
      .flatMap {
        case (dataObjectId, df) => df.queryExecution.analyzed.output.map(a => a.exprId -> (dataObjectId, a.name))
      }
      .groupMap(_._1)(_._2)
  }

  /**
   * Collect how the columns created inside the plan are defined from the columns further down in the plan.
   *
   * The order of the traversal does not matter, as every column is defined only once. Note that the plan of a
   * subquery expression is not a child of the plan node holding it, so `foreachWithSubqueries` is needed to
   * reach the columns defined inside a subquery.
   */
  private def collectDefinitions(plan: LogicalPlan): Definitions = {
    val definitions = mutable.Map[ExprId, Definition]()
    val unionDependencies = mutable.Map[ExprId, Seq[ExprId]]()
    plan.foreachWithSubqueries { node =>
      // columns defined by an Alias, e.g. by select, withColumn, groupBy/agg or a window function
      node.expressions.foreach(_.foreach {
        case alias: Alias => definitions.put(alias.exprId, definitionOf(alias.child))
        case _ => ()
      })
      // columns which a plan node creates from the columns of its children without an Alias
      node match {
        // Union takes over the expression ids of its first child, so the columns of the other children have to
        // be added as additional dependencies of the output column at the same position. They are kept apart
        // from the definitions, as the column they belong to can be a column of an input DataObject itself.
        case union: Union =>
          union.output.zipWithIndex.foreach {
            case (attribute, idx) =>
              val dependencies = union.children.flatMap(_.output.lift(idx)).map(_.exprId)
              unionDependencies.updateWith(attribute.exprId) {
                case Some(existing) => Some((existing ++ dependencies).distinct)
                case None => Some(dependencies)
              }
          }
        // Expand creates a new column per position of its projections, e.g. for grouping sets, cube or rollup
        case expand: Expand =>
          expand.output.zipWithIndex.foreach {
            case (attribute, idx) =>
              val expressions = expand.projections.flatMap(_.lift(idx))
              definitions.put(attribute.exprId, Definition(references(expressions), isIdentity = false, None))
          }
        // Generate creates columns from the columns the generator reads, e.g. explode
        case generate: Generate =>
          val definition = Definition(references(Seq(generate.generator)), isIdentity = false, describe(generate.generator))
          generate.generatorOutput.foreach(attribute => definitions.put(attribute.exprId, definition))
        // A typed Dataset transformation, e.g. a map over a case class, deserializes the columns it reads into
        // an object, transforms it with an opaque Scala function and serializes it back to columns. The
        // expressions doing so address the object by its position and not by an attribute reference, so the
        // columns have to be linked to the object over the output of the plan node.
        case serialize: SerializeFromObject =>
          val dependencies = serialize.child.output.map(_.exprId)
          serialize.output.foreach(attribute => definitions.put(attribute.exprId, Definition(dependencies, isIdentity = false, None)))
        case producer: ObjectProducer =>
          val dependencies = producer.children.flatMap(_.output).map(_.exprId)
          definitions.put(producer.outputObjAttr.exprId, Definition(dependencies, isIdentity = false, None))
        case _ => ()
      }
    }
    Definitions(definitions.toMap, unionDependencies.toMap)
  }

  /**
   * Get the definition of a column from the expression defining it.
   */
  private def definitionOf(expression: Expression): Definition = expression match {
    // the column is taken over unchanged, e.g. by a select or a rename
    case attribute: AttributeReference => Definition(Seq(attribute.exprId), isIdentity = true, None)
    case _ => Definition(references(Seq(expression)), isIdentity = false, describe(expression))
  }

  /**
   * The columns an expression reads to create its value.
   */
  private def references(expressions: Seq[Expression]): Seq[ExprId] = expressions.flatMap(directReferences).distinct

  /**
   * The columns an expression reads, stopping at the boundary of a subquery expression.
   *
   * A subquery expression holds its plan in a field and not as a child, so its result column is not one of the
   * columns it references. The columns correlating a subquery with the outer query on the other hand are
   * referenced, but they influence the value only indirectly and are therefore left out here.
   */
  private def directReferences(expression: Expression): Seq[ExprId] = expression match {
    case attribute: AttributeReference => Seq(attribute.exprId)
    // a scalar subquery evaluates to the single column its plan returns
    case subquery: ScalarSubquery => subquery.plan.output.map(_.exprId)
    // other subquery expressions, e.g. IN or EXISTS, are conditions and therefore INDIRECT lineage
    case _: SubqueryExpression => Seq()
    case other => other.children.flatMap(directReferences)
  }

  /**
   * Describe an expression by its SQL representation, cut off if it is too long.
   */
  private def describe(expression: Expression): Option[String] = {
    Try(expression.sql).toOption
      .map(sql => if (sql.length > maxDescriptionLength) sql.take(maxDescriptionLength - 3) + "..." else sql)
  }

  /**
   * Follow the definitions of a column down to the columns of the input DataObjects.
   *
   * A column can depend on the same input column over multiple paths, e.g. `concat(a, upper(a))`. Such paths are
   * combined into one input field, keeping the transformation of the first path which is not an identity.
   */
  private def resolve(
      exprId: ExprId,
      sources: Map[ExprId, Seq[(DataObjectId, String)]],
      definitions: Definitions
  ): Resolution = {
    val inputFields = mutable.LinkedHashMap[(DataObjectId, String), ColumnTransformation]()
    var expression: Option[String] = None
    var isComplete = true
    def go(exprId: ExprId, isIdentity: Boolean, description: Option[String], visited: Set[ExprId]): Unit = {
      if (visited.contains(exprId)) return
      val source = sources.get(exprId)
      source.foreach(_.foreach {
        case (dataObjectId, column) =>
          val transformation = ColumnTransformation.direct(isIdentity, description)
          inputFields.updateWith((dataObjectId, column)) {
            case Some(existing) if existing.subtype != ColumnTransformation.Identity => Some(existing)
            case _ => Some(transformation)
          }
      })
      // a column of a Union can be a column of an input DataObject and take the columns of the other children
      // of the Union at the same time, see the Union case in collectDefinitions
      definitions.unionDependencies.getOrElse(exprId, Seq()).foreach { dependency =>
        go(dependency, isIdentity, description, visited + exprId)
      }
      // A column of an input DataObject is a leaf of the lineage of this Action. Its own definition describes
      // how the input DataObject created it and belongs to the Action which wrote it.
      if (source.isEmpty) {
        definitions.byExprId.get(exprId) match {
          case Some(definition) if definition.dependencies.nonEmpty =>
            definition.dependencies.foreach { dependency =>
              go(dependency, isIdentity && definition.isIdentity, description.orElse(definition.description), visited + exprId)
            }
          // the column is defined without reading any column, e.g. from a literal or by count(*)
          case Some(definition) =>
            if (expression.isEmpty) expression = description.orElse(definition.description)
          // the column is neither a column of an input DataObject nor defined inside the plan
          case None => isComplete = false
        }
      }
    }
    go(exprId, isIdentity = true, None, Set())
    val sortedInputFields = inputFields.toSeq
      .map { case ((dataObjectId, column), transformation) => ColumnLineageInputField(dataObjectId, column, transformation) }
      .sortBy(f => (f.dataObjectId.id, f.column))
    Resolution(sortedInputFields, expression, isComplete)
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

  /**
   * How the columns created inside a plan are defined.
   *
   * @param byExprId          the definition of a column, by the expression id of the column it defines.
   * @param unionDependencies additional dependencies of a column which is combined from several columns while
   *                          keeping its expression id, which is what a Union does with its first child.
   */
  private case class Definitions(byExprId: Map[ExprId, Definition], unionDependencies: Map[ExprId, Seq[ExprId]])

  /**
   * How a column created inside the plan is defined.
   *
   * @param dependencies the columns it is created from.
   * @param isIdentity   true if the value of the column is taken over unchanged from its dependency.
   * @param description  the expression defining the column, if it is not an identity.
   */
  private case class Definition(dependencies: Seq[ExprId], isIdentity: Boolean, description: Option[String])
}
