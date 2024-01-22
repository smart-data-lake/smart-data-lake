/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util
import scala.jdk.CollectionConverters._

/**
 * Dummy spark streaming source
 * It is used to create a streaming DataFrame to transport the schema between spark streaming queries in init-phase.
 */
private[smartdatalake] class DummyStream(schema: StructType) extends MicroBatchStream {

  def getSchema: StructType = schema

  override def deserializeOffset(json: String): Offset = LongOffset(json.toLong)
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = throw new NotImplementedError("DummyStream cannot deliver data")
  override def commit(end: Offset): Unit = ()
  override def stop(): Unit = ()
  override def toString: String = s"DummyStream"
  override def initialOffset(): Offset = LongOffset(0)
  override def latestOffset(): Offset = LongOffset(0)
  override def createReaderFactory(): PartitionReaderFactory = DummyStreamReaderFactory

  private object DummyStreamReaderFactory extends PartitionReaderFactory {
    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
      new PartitionReader[InternalRow] {
        override def next(): Boolean = false
        override def get(): UnsafeRow = throw new NotImplementedError("DummyStream cannot deliver data")
        override def close(): Unit = ()
      }
    }
  }
}

class MemoryStreamTable(val stream: DummyStream) extends Table with SupportsRead {
  override def name(): String = "DummyStreamDataSource"
  override def schema(): StructType = stream.getSchema
  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.MICRO_BATCH_READ).asJava
  }
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new DummyStreamScanBuilder(stream)
  }
}

class DummyStreamScanBuilder(stream: DummyStream) extends ScanBuilder with Scan {
  override def build(): Scan = this
  override def description(): String = "DummyStreamDataSource"
  override def readSchema(): StructType = stream.getSchema
  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = stream
}

class DummyStreamProvider extends TableProvider {
  override def supportsExternalMetadata() = true
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = throw new NotImplementedError("DummyStreamProvider cannot infer schema")
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new MemoryStreamTable( new DummyStream(schema))
  }
}

object DummyStreamProvider extends SmartDataLakeLogger {
  def getDummyDf(schema: StructType)(implicit session: SparkSession): DataFrame = {
    logger.debug("creating dummy streaming df")
    session.readStream.schema(schema).format(classOf[DummyStreamProvider].getName).load()
  }
}