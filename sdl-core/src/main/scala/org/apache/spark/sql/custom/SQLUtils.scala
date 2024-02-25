/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package org.apache.spark.sql.custom

import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{BlockId, TempLocalBlockId}

import java.util.UUID
import scala.reflect.ClassTag

object SQLUtils {
  def internalCreateDataFrame(catalystRows: RDD[InternalRow], schema: StructType, isStreaming: Boolean = false)(implicit session: SparkSession): DataFrame = {
    session.internalCreateDataFrame(catalystRows, schema, isStreaming)
  }
  def createBlockRDD[T: ClassTag](blockIds: Seq[BlockId])(implicit session: SparkSession) = {
    new BlockRDD[T](session.sparkContext, blockIds.toArray)
  }
  def getNewBlockId = TempLocalBlockId(UUID.randomUUID())



}
