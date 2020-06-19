/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.streaming

import java.util.Optional

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * Dummy spark streaming source
 * It is used to create a streaming DataFrame to transport the schema between spark streaming queries in init-phase.
 */
private[smartdatalake] class DummyStreamProvider extends DataSourceV2
  with MicroBatchReadSupport with DataSourceRegister {
  override def createMicroBatchReader( schema: Optional[StructType], checkpointLocation: String, options: DataSourceOptions): MicroBatchReader = {
    assert(schema.isPresent, "DummyStreamProvider must be initialized with a schema.")
    new DummyMicroBatchReader(schema.get)
  }
  override def shortName(): String = "dummy"

  class DummyMicroBatchReader(schema: StructType)
    extends MicroBatchReader with Logging {
    override def readSchema(): StructType = schema
    override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = Unit
    override def getStartOffset(): Offset = LongOffset(0)
    override def getEndOffset(): Offset = LongOffset(0)
    override def deserializeOffset(json: String): Offset = LongOffset(json.toLong)
    override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = throw new NotImplementedError("DummyMicroBatchReader cannot deliver data")
    override def commit(end: Offset): Unit = {}
    override def stop(): Unit = {}
    override def toString: String = s"DummyStreamV2"
  }
}

object DummyStreamProvider extends SmartDataLakeLogger {
  def getDummyDf(schema: StructType)(implicit session: SparkSession): DataFrame = {
    logger.debug("creating dummy streaming df")
    session.readStream.schema(schema).format(classOf[DummyStreamProvider].getName).load
  }
}


