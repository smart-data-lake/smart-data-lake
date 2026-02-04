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

import io.smartdatalake.workflow.dataframe.GenericField

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

case class ScalaColumnDefinition[A: ClassTag](name: String,
                                              nullable: Boolean = false,
                                              comment: Option[String] = None,
                                             _dataType: Option[ScalaDataType[A]] = None) extends GenericField {

  // datatype is deduced from generic type A if not explicitly provided
  val dataType: ScalaDataType[A] = _dataType.getOrElse(ScalaDataType.getFor[A])

  def makeNullable: ScalaColumnDefinition[A] = copy(nullable = true)

  def toLowerCase: ScalaColumnDefinition[A] = copy(name = name.toLowerCase)

  def removeMetadata: ScalaColumnDefinition[A] = copy(comment = None)

  def createColumn(data: IndexedSeq[_]): ScalaColumn[A] = {
    ScalaColumn(this, data.asInstanceOf[IndexedSeq[A]])
  }

  override def subFeedType: Type = typeOf[ScalaSubFeed]
}
