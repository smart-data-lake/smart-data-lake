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

import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame, GenericGroupedDataFrame}

import scala.reflect.runtime.universe

/**
 * A grouped DataFrame in plain Scala implementation.
 * It is created by calling groupBy() on a ScalaDataFrame.
 */
case class ScalaGroupedDataFrame(keyCols: Seq[ScalaAbstractColumn], df: ScalaDataFrame) extends GenericGroupedDataFrame {
  private val functions = ScalaSubFeed.asInstanceOf[DataFrameFunctions]
  import functions._

  override def agg(columns: Seq[GenericColumn]): ScalaDataFrame = {
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
    // there is no group to aggregate if the DataFrame has no rows, e.g. in init phase
    if (groups.isEmpty) return emptyAggDataFrame(scalaKeyCols, aggCols)
    val dfAgg = groups.map { group =>
      val groupCondition = scalaKeyCols.zip(group).map{ case (col, value) => col === lit(value) }.reduce(_ and _)
      val groupDf = df.where(groupCondition)
      val aggKeyCols = keyCols.zip(group).map{ case (col, v) => col.toScalaColumn(IndexedSeq(v)) }
      groupDf.agg(aggKeyCols ++ aggCols)
    }.reduceLeft(_.unionByName(_))

    dfAgg.asInstanceOf[ScalaDataFrame]
  }

  /**
   * Empty result of an aggregation, having the schema of the key and aggregate columns.
   */
  private def emptyAggDataFrame(scalaKeyCols: Seq[ScalaColumn[_]], aggCols: Seq[ScalaAbstractColumn]): ScalaDataFrame = {
    val aggColDefs = aggCols.map(_.toScalaColumn(df).definition)
    ScalaDataFrame.returnEmpty(ScalaSchema(scalaKeyCols.map(_.definition) ++ aggColDefs))
  }

  override def subFeedType: universe.Type = universe.typeOf[ScalaSubFeed]
}
