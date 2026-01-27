/*
 * Smart Data Lake - Build your data lake the smart way.
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

package io.smartdatalake.util.spark

import org.apache.spark.sql.types.{DataType, DecimalType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.jdk.CollectionConverters._

package object dataset {
  /**
   * Create empty DataFrame with defined Schema
   */
  def getEmptyDataFrame(scheme: StructType)(implicit ss: SparkSession): DataFrame = ss
    .createDataFrame(List[Row]().asJava, scheme)

  def getDecimalPrecisionScale(t: DataType): Option[(Int, Int)] = t match {
    case DecimalType() => Some((t.asInstanceOf[DecimalType].precision,
      t.asInstanceOf[DecimalType].scale))
    case _ => None
  }

}
