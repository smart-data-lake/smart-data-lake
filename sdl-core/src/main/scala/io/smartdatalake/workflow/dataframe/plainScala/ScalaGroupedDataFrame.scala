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

import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame, GenericGroupedDataFrame}

import scala.reflect.runtime.universe

case class ScalaGroupedDataFrame(keyCols: Seq[ScalaAbstractColumn], df: ScalaDataFrame) extends GenericGroupedDataFrame {
  private val functions = ScalaSubFeed.asInstanceOf[DataFrameFunctions]
  import functions._

  override def agg(columns: Seq[GenericColumn]): GenericDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val aggCols = columns.map {
      case c: ScalaAbstractColumn =>
        assert(c.getName.isDefined, s"Aggregate column has no name. Use 'col.as(...)' to give the aggregates a name!")
        c
      case c => DataFrameSubFeed.throwIllegalSubFeedTypeException(c)
    }
    // make sure keyCols are evaluated
    val scalaKeyCols = keyCols.map(_.toScalaColumn(df))
    // calculate aggregated values for every group
    val groups = scalaKeyCols.map(_.data).transpose.distinct
    val dfAgg = groups.map { group =>
      val groupCondition = scalaKeyCols.zip(group).map{ case (col, value) => col === lit(value) }.reduce(_ and _)
      val groupDf = df.where(groupCondition)
      val aggKeyCols = keyCols.zip(group).map{ case (col, v) => col.toScalaColumn(IndexedSeq(v)) }
      groupDf.agg(aggKeyCols ++ aggCols)
    }.reduceLeft(_.unionByName(_))

    dfAgg
  }

  override def subFeedType: universe.Type = universe.typeOf[ScalaSubFeed]
}
