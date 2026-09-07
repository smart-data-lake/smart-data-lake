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
import org.json4s.JsonAST.{JArray, JBool, JField, JObject, JString, JValue}

/**
 * Column level lineage of a DataFrame written to an output DataObject, e.g. which columns of which input
 * DataObjects the column `city` of the output DataObject is created from, and how.
 *
 * A column which is not created from any input column, e.g. a constant, is listed in `fields` without input
 * fields. A column whose lineage could not be traced completely is listed in `unresolvedColumns` instead, see
 * [[io.smartdatalake.util.spark.SparkColumnLineageUtil]] for the cases where this happens. Keeping the two
 * apart tells a consumer of the export whether a column has no source or whether SDLB could not find it.
 *
 * The Json representation is the `columnLineage` dataset facet of the OpenLineage standard, see
 * https://openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet. Using an established format
 * makes the exported lineage consumable by existing tools, e.g. Marquez or DataHub, without a converter.
 *
 * @param fields            lineage per column of the DataFrame, ordered by column name to keep the export stable.
 * @param unresolvedColumns columns whose lineage could not be traced back completely, ordered by name.
 */
case class ColumnLineage(fields: Seq[ColumnLineageField], unresolvedColumns: Seq[String] = Seq()) {

  def isEmpty: Boolean = fields.isEmpty && unresolvedColumns.isEmpty

  def nonEmpty: Boolean = !isEmpty

  def get(column: String): Option[ColumnLineageField] = fields.find(_.column == column)

  /**
   * The `columnLineage` facet as defined by the OpenLineage standard.
   */
  def toJson: JObject = JObject(
    "fields" -> JObject(fields.map(f => JField(f.column, f.toJson)).toList)
  )
}

/**
 * Lineage of one column of a DataFrame.
 *
 * @param column      name of the column.
 * @param inputFields columns of input DataObjects this column is created from, ordered to keep the export stable.
 *                    Empty if the column is not created from an input column at all, e.g. a constant.
 * @param expression  the expression creating the column, if it has no input fields. For a column with input
 *                    fields the expression is exported as description of its transformations instead.
 */
case class ColumnLineageField(column: String, inputFields: Seq[ColumnLineageInputField], expression: Option[String] = None) {
  def toJson: JObject = JObject(
    Seq[Option[JField]](
      Some("inputFields" -> JArray(inputFields.map(_.toJson).toList)),
      expression.map(e => "expression" -> (JString(e): JValue))
    ).flatten.toList
  )
}

/**
 * A column of an input DataObject an output column is created from.
 *
 * Note that `namespace` and `name` of the OpenLineage input field are filled with SDLB's namespace and the
 * DataObjectId. They do not identify the physical dataset, as the same DataObject can be backed by different
 * storage locations in different environments.
 *
 * @param dataObjectId   id of the input DataObject.
 * @param column         name of the column of the input DataObject.
 * @param transformation how the output column is created from this input column.
 */
case class ColumnLineageInputField(dataObjectId: DataObjectId, column: String, transformation: ColumnTransformation) {
  def toJson: JObject = JObject(
    "namespace" -> JString(ColumnLineage.namespace),
    "name" -> JString(dataObjectId.id),
    "field" -> JString(column),
    "transformations" -> JArray(List(transformation.toJson))
  )
}

/**
 * How an output column is created from an input column, see
 * https://openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet#transformation-type.
 *
 * @param tpe         `DIRECT` if the value of the output column is derived from the input column, `INDIRECT`
 *                    if the input column influences the output column without being part of its value, e.g. a
 *                    join or filter condition. Only `DIRECT` transformations are detected so far.
 * @param subtype     `IDENTITY` if the value is taken over unchanged, `TRANSFORMATION` if it is modified,
 *                    `AGGREGATION` if it is aggregated.
 * @param description optional description of the transformation, e.g. the SQL expression creating the column.
 * @param masking     true if the transformation obfuscates the value of the input column. Not detected so far.
 */
case class ColumnTransformation(tpe: String, subtype: String, description: Option[String] = None, masking: Boolean = false) {
  def toJson: JObject = JObject(
    Seq[Option[JField]](
      Some("type" -> JString(tpe)),
      Some("subtype" -> JString(subtype)),
      description.map(d => "description" -> (JString(d): JValue)),
      Some("masking" -> JBool(masking))
    ).flatten.toList
  )
}

object ColumnTransformation {
  val Direct = "DIRECT"
  val Indirect = "INDIRECT"
  val Identity = "IDENTITY"
  val Transformation = "TRANSFORMATION"

  /**
   * A direct transformation, which is an identity if the value of the input column is taken over unchanged.
   */
  def direct(isIdentity: Boolean, description: Option[String] = None): ColumnTransformation = {
    if (isIdentity) ColumnTransformation(Direct, Identity)
    else ColumnTransformation(Direct, Transformation, description)
  }
}

object ColumnLineage {

  /**
   * OpenLineage namespace used for input fields. The `name` of an input field is a DataObjectId, which is
   * unique within an SDLB configuration but says nothing about the physical location of the data.
   */
  val namespace = "sdlb"

  val empty: ColumnLineage = ColumnLineage(Seq(), Seq())
}
