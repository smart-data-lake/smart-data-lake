/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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

import ScalaDataTypeEnum._
import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericDataType, GenericField, GenericSchema}

import scala.reflect.runtime.universe
case class ScalaSchema(_fields: List[ScalaColumnDefinition], isInferred: Boolean = false) extends GenericSchema {

  //only ignores upper / lower case difference
  override def diffSchema(schema: GenericSchema): Option[GenericSchema] = schema match {
    case scalaSchema: ScalaSchema => {
      val (thisFieldsSet, otherFieldsSet) = (this.toLowerCase.fields.toSet, scalaSchema.toLowerCase.fields.toSet)
      Some(ScalaSchema(thisFieldsSet.diff(otherFieldsSet).toList))
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(schema)
  }

  override def fields: Seq[ScalaColumnDefinition] = _fields.toSeq

  override def columns: Seq[String] = fields.map(_.name)

  //not really relevant...
  override def sql: String = fields.map(sc => s"${sc.name} ${sc.dataType.inner} ${if (sc.nullable) "" else "not null"}").mkString(",\n")

  override def add(colName: String, dataType: GenericDataType): GenericSchema = dataType match {
    case scalaType: ScalaDataType => add(ScalaColumnDefinition(colName, dataType.asInstanceOf[ScalaDataType]))
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
  }

  override def add(field: GenericField): ScalaSchema = field match {
    case scalaCol: ScalaColumnDefinition => ScalaSchema(_fields :+ field.asInstanceOf[ScalaColumnDefinition])
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(field)
  }

  override def remove(colName: String): ScalaSchema = ScalaSchema(_fields.filterNot(_.name == colName))

  override def filter(func: GenericField => Boolean): ScalaSchema = ScalaSchema(_fields.filter(func))


  override def getDataType(colName: String): ScalaDataType = {
    require(fields.map(_.name).contains(colName), s"The column $colName does not exist in the ScalaSchema")
    fields.collectFirst({case field if field.name == colName => field.dataType}).get
  }

  override def makeNullable: ScalaSchema = ScalaSchema(_fields.map(_.makeNullable))

  override def toLowerCase: ScalaSchema = ScalaSchema(_fields.map(_.makeNullable))

  override def removeMetadata: ScalaSchema = ScalaSchema(_fields.map(_.removeMetadata))

  override def getEmptyDataFrame(dataObjectId: SdlConfigObject.DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame = ???

  override def treeString(level: Int): String = fields.map(f => f"${f.name} (${f.dataType})").mkString("  |  "); //only flat structure as of now

  override def subFeedType: universe.Type = universe.typeOf[ScalaSubFeed]
}

object ScalaSchema {

  def apply(pairs: Seq[(String, ScalaDataTypeEnum)]): ScalaSchema = {
    val fields = pairs.map(p => ScalaColumnDefinition(p._1, ScalaDataType(p._2))).toList
    ScalaSchema(fields)
  }

  def inferredFromFields(pairs: Seq[(String, ScalaDataTypeEnum)]): ScalaSchema = {
    val fields = pairs.map(p => ScalaColumnDefinition(p._1, ScalaDataType(p._2))).toList
    ScalaSchema(fields, true)
  }
}
