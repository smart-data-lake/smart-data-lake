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
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataFrame, GenericGroupedDataFrame}

import scala.reflect.runtime.universe


case class ScalaGroupedDataframe(keyColNames: Seq[String], df: ScalaDataFrame) extends GenericGroupedDataFrame {

  override def agg(columns: Seq[GenericColumn]): GenericDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val scalaCols = columns.map(_.asInstanceOf[ScalaColumn[_]])
    throw new NotImplementedError("agg not implemented yet for ScalaGroupedDataframe")
  }

  override def subFeedType: universe.Type = universe.typeOf[ScalaSubFeed]
}
