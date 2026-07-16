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

import io.smartdatalake.workflow.dataframe.GenericRow

import scala.reflect.ClassTag
import scala.reflect.runtime.universe

/**
 * A row in a ScalaDataFrame.
 * The data is stored as an IndexedSeq.
 */
case class ScalaRow(values: IndexedSeq[Option[Any]]) extends GenericRow {
  // Every entry is stored as Option, null is represented as None (plain-Scala null handling).
  // Note that the GenericRow interface functions (get, getAs, toSeq) resolve the Option and return plain values
  // with null for missing values, as defined by the generic contract. Use apply or values for Option-based access.
  def apply(ix: Int): Option[Any] = values(ix)

  override def get(index: Int): Any = values(index).orNull

  override def getStruct(index: Int): GenericRow = throw new NotImplementedError("getStruct is not implemented for ScalaRow")

  override def getAs[T: ClassTag](index: Int): T = {
    val v = values(index)
    val cls = implicitly[ClassTag[T]].runtimeClass
    // return the value as Option only if T is of type Option, e.g. getAs[Option[Int]].
    // note that for unspecific types as getAs[Any], the plain value must be returned.
    if (classOf[Option[_]].isAssignableFrom(cls)) v.asInstanceOf[T]
    else v.orNull.asInstanceOf[T]
  }

  override def toSeq: Seq[Any] = values.map(_.orNull)

  override def subFeedType: universe.Type =  universe.typeOf[ScalaSubFeed]
}