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

package io.smartdatalake.debezium

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataobject.DebeziumCdcDataObject
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.storage.OffsetBackingStore
import org.apache.kafka.connect.util.Callback

import java.nio.ByteBuffer
import java.util
import java.util.Base64
import java.util.concurrent.{CompletableFuture, Future}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Custom offset storage that leverage the sdlb state mechanism to save the offset
 */
class SDLBDebeziumOffsetStorage() extends OffsetBackingStore with SmartDataLakeLogger {

  private val SDLB_DATA_OBJECT_ID_CONFIG = "offset.storage.sdlb.data.object.id"
  private var dataObjectId: String = ""

  private val instanceRegistry = Environment.instanceRegistry

  private val data: mutable.Map[ByteBuffer, ByteBuffer] = mutable.Map()

  override def start(): Unit = {
    logger.info(s"Start SDLBDebeziumOffsetStorage for data object DebeziumCdcDataObject($dataObjectId)")
    instanceRegistry.get[DebeziumCdcDataObject](DataObjectId(dataObjectId)).incrementalState.foreach(state => {
      val key = stringToByteBuffer(state._1)
      val value = stringToByteBuffer(state._2)

      data.put(key, value)

    })

  }

  // Helper function to convert Base64 string back to ByteBuffer
  private def stringToByteBuffer(str: String): ByteBuffer = {
    val bytes = Base64.getDecoder.decode(str)
    ByteBuffer.wrap(bytes)
  }

  override def stop(): Unit = {
    logger.info(s"Stop SDLBDebeziumOffsetStorage for data object DebeziumCdcDataObject($dataObjectId)")
    data.clear()
  }

  override def get(keys: util.Collection[ByteBuffer]): Future[util.Map[ByteBuffer, ByteBuffer]] = {
    CompletableFuture.completedFuture(data.filterKeys(k => keys.contains(k)).toMap.asJava)
  }

  override def set(values: util.Map[ByteBuffer, ByteBuffer], callback: Callback[Void]): Future[Void] = {
    values.asScala.foreach(state => {
      val key = byteBufferToString(state._1)
      val value = byteBufferToString(state._2)
      instanceRegistry.get[DebeziumCdcDataObject](DataObjectId(dataObjectId)).incrementalState.put(key, value)
    })

    if(callback != null) {
      callback.onCompletion(null, null)
    }

    CompletableFuture.completedFuture(null)
  }

  // Helper function to convert ByteBuffer to Base64 string
  private def byteBufferToString(buffer: ByteBuffer): String = {
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    Base64.getEncoder.encodeToString(bytes)
  }

  override def configure(config: WorkerConfig): Unit = {
    dataObjectId = config.originalsStrings().get(SDLB_DATA_OBJECT_ID_CONFIG)
  }

  override def connectorPartitions(s: String): util.Set[util.Map[String, AnyRef]] = {
    // Not used
    Set.empty[util.Map[String, AnyRef]].asJava
  }
}

