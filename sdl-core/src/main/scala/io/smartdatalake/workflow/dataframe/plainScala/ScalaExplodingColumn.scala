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
 * A column in a ScalaDataFrame that is created by the explode() function
 * The data and the index of the original row are stored an IndexedSeq[A, Int]
 *
 * @param definition definition of the column including name and data type
 * @param data       the actual data of the column and the index of the original row
 * @tparam A the Scala type of the column
 */
case class ScalaExplodingColumn[A: ClassTag](definition: ScalaColumnDefinition[A], data: IndexedSeq[(A, Int)]) extends ScalaAbstractColumn with SmartDataLakeLogger {

  override def dataType: ScalaDataType[A] = definition.dataType

  override def getName: Option[String] = Some(definition.name)

  override def apply(extraction: Any): ScalaColumn[A] = throw new NotImplementedError("The 'apply' method is not applicable for a ScalaExplodingColumn instance")

  def mergeWithScalaDataFrame(df: ScalaDataFrame): ScalaDataFrame = {
    val explodedRows: Seq[ScalaRow] = for (row <- df.rows.zipWithIndex;
         (field, ix) <- this.data
         if row._2 == ix) yield ScalaRow(row._1.values :+ field)
    val newSchema: ScalaSchema = df.schema.add(this.definition)
    ScalaDataFrame.fromScalaRows(explodedRows, schemaIn = Some(newSchema))
  }

  def changeColumnName(newName: String): ScalaExplodingColumn[A] = {
    val definition: ScalaColumnDefinition[A] = this.definition.copy(name = newName, _dataType = Some(dataType))
    this.copy(definition = definition)
  }
}


object ScalaExplodingColumn {

  private val colCounter = new java.util.concurrent.atomic.AtomicLong(0)

  def nextColName = s"col${colCounter.incrementAndGet()}"

}

