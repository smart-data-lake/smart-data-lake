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

import io.smartdatalake.workflow.dataframe.GenericField

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Definition of a column in a ScalaDataFrame
 * The data type is deduced from the generic type A if not explicitly provided through dataType
 *
 * @param provenance which columns the values of this column are calculated from, see [[ScalaColumnProvenance]].
 *                   It is the identity of the column and not part of its definition, see `equals`.
 */
case class ScalaColumnDefinition[A: ClassTag](name: String,
                                              dataFrameAlias: Option[String] = None,
                                              nullable: Boolean = true,
                                              comment: Option[String] = None,
                                              dataTypeOverride: Option[ScalaDataType[A]] = None,
                                              provenance: ScalaColumnProvenance = ScalaColumnProvenance.root()
                                             ) extends GenericField {

  // datatype is deduced from generic type A if not explicitly provided
  val dataType: ScalaDataType[A] = dataTypeOverride.getOrElse(ScalaDataType.getFor[A])

  def makeNullable: ScalaColumnDefinition[A] = copy(nullable = true)

  def toLowerCase: ScalaColumnDefinition[A] = copy(name = name.toLowerCase)

  def removeMetadata: ScalaColumnDefinition[A] = copy(comment = None)

  def createColumn(data: IndexedSeq[Option[_]]): ScalaColumn[A] = {
    ScalaColumn(this, data.asInstanceOf[IndexedSeq[Option[A]]])
  }

  def withDataFrameAlias(alias: Option[String]): ScalaColumnDefinition[A] = copy(dataFrameAlias = alias)

  def getFullName() = dataFrameAlias.map(a => s"$a.$name").getOrElse(name)

  def withProvenance(provenance: ScalaColumnProvenance): ScalaColumnDefinition[A] = copy(provenance = provenance)

  /**
   * The provenance is excluded from equality and from the string representation, as it is the identity of a
   * column and not part of its definition: two columns with the same name, data type and metadata are the same
   * field of a schema, no matter how their values were calculated. Schemas are compared as sets of fields, see
   * [[ScalaSchema.diffSchema]], so `hashCode` has to leave out the provenance as well.
   */
  override def equals(other: Any): Boolean = other match {
    case that: ScalaColumnDefinition[_] =>
      name == that.name && dataFrameAlias == that.dataFrameAlias && nullable == that.nullable &&
        comment == that.comment && dataTypeOverride == that.dataTypeOverride
    case _ => false
  }

  override def hashCode(): Int = (name, dataFrameAlias, nullable, comment, dataTypeOverride).hashCode()

  override def toString: String = s"ScalaColumnDefinition($name,$dataFrameAlias,$nullable,$comment,$dataTypeOverride)"

  override def subFeedType: Type = typeOf[ScalaSubFeed]
}