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

import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericDataType, GenericField, GenericSchema}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe

case class ScalaSchema(override val fields: Seq[ScalaColumnDefinition[_]], isInferred: Boolean = false) extends GenericSchema {

  //only ignores upper / lower case difference
  override def diffSchema(schema: GenericSchema): Option[GenericSchema] = schema match {
    case scalaSchema: ScalaSchema => {
      val (thisFieldsSet, otherFieldsSet) = (this.toLowerCase.fields.toSet, scalaSchema.toLowerCase.fields.toSet)
      Some(ScalaSchema(thisFieldsSet.diff(otherFieldsSet).toList))
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(schema)
  }

  override def columns: Seq[String] = fields.map(_.name)

  override def sql: String = fields.map(sc => s"${sc.name} ${sc.dataType.sql}${if (sc.nullable) "" else " not null"}").mkString(", ")

  override def add(colName: String, dataType: GenericDataType): GenericSchema = dataType match {
    case scalaType: ScalaDataType[_] => add(scalaType.createColumnDefinition(colName))
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
  }

  override def add(field: GenericField): ScalaSchema = field match {
    case scalaCol: ScalaColumnDefinition[_] => copy(fields = fields :+ scalaCol)
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(field)
  }

  override def remove(colName: String): ScalaSchema = copy(fields = fields.filterNot(_.name == colName))

  override def filter(func: GenericField => Boolean): ScalaSchema = copy(fields = fields.filter(func))

  override def getDataType(colName: String): ScalaDataType[_] = {
    fields.collectFirst({ case field if field.name == colName => field.dataType })
      .getOrElse(throw new IllegalArgumentException(s"The column $colName does not exist in the ScalaSchema"))
  }

  override def makeNullable: ScalaSchema = copy(fields = fields.map(_.makeNullable))

  override def toLowerCase: ScalaSchema = copy(fields = fields.map(_.makeNullable))

  override def removeMetadata: ScalaSchema = copy(fields = fields.map(_.removeMetadata))

  override def getEmptyDataFrame(dataObjectId: SdlConfigObject.DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame = {
    // TODO
    null
  }

  override def treeString(level: Int): String = fields.map(f => f"${f.name} (${f.dataType})").mkString("  |  "); //only flat structure as of now

  override def subFeedType: universe.Type = universe.typeOf[ScalaSubFeed]
}

