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

import io.smartdatalake.workflow.dataframe.GenericRow
import scala.reflect.runtime.universe

case class ScalaRow(value: Seq[Any]) extends GenericRow {
  def apply(ix: Int) = value(ix)

  override def get(index: Int): Any = value(index)

  override def getStruct(index: Int): GenericRow = this //not relevant for our tests

  override def getAs[T](index: Int): T = value(index).asInstanceOf[T]

  override def toSeq: Seq[Any] = value

  override def subFeedType: universe.Type =  ???//universe.typeOf[ScalaSubFeed]
}
