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

import io.smartdatalake.util.misc.SmartDataLakeLogger

import scala.reflect.ClassTag

/**
 * A column in a ScalaDataFrame
 * The data is stored as an IndexedSeq
 *
 * @param definition definition of the column including name and data type
 * @param data       the actual data of the column
 * @tparam A the Scala type of the column
 */
case class ScalaColumn[A: ClassTag](definition: ScalaColumnDefinition[A], data: IndexedSeq[A]) extends ScalaAbstractColumn with SmartDataLakeLogger {

  override def dataType: ScalaDataType[A] = definition.dataType

  override def getName: Option[String] = Some(definition.name)

  override def toScalaColumn(df: ScalaDataFrame): ScalaColumn[_] = this

  override def apply(extraction: Any): ScalaColumn[A] = throw new NotImplementedError("The 'apply' method is not applicable for a ScalaColumn instance")

  // Support methods for DataFrame operations
  def append(other: ScalaColumn[A]): ScalaColumn[A] = copy(data = data ++ other.data)

  def unsafeAppend(other: ScalaColumn[_]): ScalaColumn[A] = append(other.asInstanceOf[ScalaColumn[A]])

  def limit(n: Int): ScalaColumn[A] = copy(data = data.take(n))

}


object ScalaColumn {

  def apply[A: ClassTag](name: String, data: Seq[A]) = new ScalaColumn[A](ScalaColumnDefinition[A](name = name), data = data.toIndexedSeq)

  private val colCounter = new java.util.concurrent.atomic.AtomicLong(0)

  def nextColName = s"col${colCounter.incrementAndGet()}"

}

