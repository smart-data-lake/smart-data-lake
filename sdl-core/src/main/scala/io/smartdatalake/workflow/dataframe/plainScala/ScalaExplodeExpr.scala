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
 */
case class ScalaExplodeExpr(in: ScalaAbstractColumn, fixedDataType: Option[ScalaDataType[_]] = None) extends ScalaAbstractColumn with SmartDataLakeLogger {

  override def dataType: ScalaDataType[_] = throw new IllegalStateException("Cannot get dataType for explode() expression, because it needs DataFrame to be evaluated.")

  override def data: Seq[_] = throw new IllegalStateException("Cannot get data for explode() expression, because it changes DataFrame granularity. Make sure explode is used as top-level expression in a withColumn statement.")

  override def visit[X](visitorFunc: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = {
    aggregator(visitorFunc(this), in.visit(visitorFunc, aggregator))
  }

  def explodeDataFrame(colName: String, df: ScalaDataFrame): ScalaDataFrame = {
    val inResolved = in.toScalaColumn(df)
    assert(inResolved.dataType.isInstanceOf[ScalaArrayDataType], s"Input column for explode() must be of type array, but is ${in.dataType}")
    val inData = inResolved.data.asInstanceOf[Seq[Seq[_]]]
    val dataType = fixedDataType
      .orElse(inResolved.dataType.asInstanceOf[ScalaArrayDataType].elementType)
      .getOrElse{
      // infer the data type by looking at the first non-null value in the input column
      ScalaDataType.getFor(inData.flatten.find(_ != null).getOrElse(throw new RuntimeException("Cannot infer data type for explode() column, because all values are null. Use fixedDataType.")).getClass)
    }
    assert(df.nrRows == inData.size)
    val explodedRows = df.rows.zip(inData).flatMap {
      case (row, seq) => seq.map(x => (row.values :+ x))
    }
    ScalaDataFrame.fromData(explodedRows, Some(ScalaSchema(df.cols.map(_.definition) :+ dataType.createColumnDefinition(colName))))
  }

}

