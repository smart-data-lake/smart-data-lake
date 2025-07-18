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

import ScalaDataTypeEnum.STRING
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataFrame, GenericGroupedDataFrame}

import scala.reflect.runtime.universe


case class ScalaGroupedDataframe(keyColNames: Seq[String], groups: Map[Seq[Any], Seq[Int]], df: ScalaDataframe) extends GenericGroupedDataFrame {

  override def agg(columns: Seq[GenericColumn]): GenericDataFrame = throw new NotImplementedError("For aggregations please use another signature providing an aggregation expression")

  override def subFeedType: universe.Type = universe.typeOf[ScalaSubFeed]

  def agg[A >: Any](colName: String, aggExpr: (A, A) => A, newColName: String = "groupbyExpr"): ScalaDataframe = {
    val colIx: Int = df.cols.indexWhere(_.name == colName)
    val rows = groups.map(kv => {
      val (key, rowIndices) = (kv._1, kv._2)
      key ++ Seq((for (i <- rowIndices) yield df(i)(colIx)).reduce(aggExpr))
    }).toSeq
    val newType = if (rows.isEmpty || rows(0).isEmpty) ScalaDataType(STRING) else ScalaDataType.fromValue(rows.head.last)
    val newSchema: ScalaSchema = ScalaSchema(df.schema._fields.filter(field => keyColNames.contains(field.name))).add(newColName, newType).asInstanceOf[ScalaSchema]
    ScalaDataframe(schema = Some(newSchema), rows = rows)
  }
}
