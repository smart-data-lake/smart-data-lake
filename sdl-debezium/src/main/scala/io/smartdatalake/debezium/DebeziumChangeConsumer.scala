/*
 * sdl-debezium - Build your data lake the smart way.
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

import io.debezium.engine.{ChangeEvent, DebeziumEngine}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.kafka.connect.source.SourceRecord

import java.time.ZonedDateTime
import java.util

/**
 * Custom change consumer that stores the resulting change events as a list of [SourceRecord].
 */
private[smartdatalake] class DebeziumChangeConsumer extends DebeziumEngine.ChangeConsumer[ChangeEvent[SourceRecord, SourceRecord]] with SdlbDebeziumChangeConsumerState with SmartDataLakeLogger {

  private var _records: List[SourceRecord] = List()
  private var _isSnapshotting: Boolean = false
  private var _lastRecordTimestamp: ZonedDateTime = ZonedDateTime.now()

  override def handleBatch(batch: util.List[ChangeEvent[SourceRecord, SourceRecord]], recordCommitter: DebeziumEngine.RecordCommitter[ChangeEvent[SourceRecord, SourceRecord]]): Unit = {

    logger.debug("handleBatch size=" + batch.size())

    _lastRecordTimestamp = ZonedDateTime.now()

    batch.forEach(record => {

      val r = record.value()

      if (r.sourceOffset().containsKey("snapshot") && r.sourceOffset().containsKey("snapshot_completed") && r.sourceOffset().get("snapshot_completed").equals(true.toString)) {
        _isSnapshotting = true
      } else {
        _isSnapshotting = false
      }

      _records = _records :+ r

      recordCommitter.markProcessed(record)
    })

    recordCommitter.markBatchFinished()

  }

  override def records: Seq[SourceRecord] = _records

  override def isSnapshotting: Boolean = _isSnapshotting

  override def lastRecordTimestamp: ZonedDateTime = _lastRecordTimestamp
}

