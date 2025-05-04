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

package io.smartdatalake.debezium

import io.debezium.engine.{ChangeEvent, DebeziumEngine}
import org.apache.kafka.connect.source.SourceRecord

import java.util

/**
 * Custom change consumer that returns only the first change event record for schema extraction.
 */
private[smartdatalake] class DebeziumSchemaConsumer extends DebeziumEngine.ChangeConsumer[ChangeEvent[SourceRecord, SourceRecord]] with HasRecords[SourceRecord] {


  var records: List[SourceRecord] = List()

  override def handleBatch(batch: util.List[ChangeEvent[SourceRecord, SourceRecord]], recordCommitter: DebeziumEngine.RecordCommitter[ChangeEvent[SourceRecord, SourceRecord]]): Unit = {

    if(records.isEmpty) {
      records  = records :+ batch.get(0).value() // read only the first record
    }

    recordCommitter.markBatchFinished()

  }
}
