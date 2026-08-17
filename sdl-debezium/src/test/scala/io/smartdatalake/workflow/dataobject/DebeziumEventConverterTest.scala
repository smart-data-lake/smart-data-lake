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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.definitions.{CdcChangeType, Environment}
import io.smartdatalake.testutils.spark.SparkTestUtil
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.Collections

/**
 * Unit tests for the conversion of debezium change events into a DataFrame with SDLBs standard CDC columns.
 * Note that reading change events from a real database is covered by the integration tests, see DebeziumCdcDataObject*IT.
 */
class DebeziumEventConverterTest extends AnyFunSuite with Matchers {

  private implicit val session: SparkSession = SparkTestUtil.session

  private val rowSchema = SchemaBuilder.struct().name("row").optional()
    .field("id", Schema.OPTIONAL_INT32_SCHEMA)
    .field("value", Schema.OPTIONAL_STRING_SCHEMA)
    .build()
  private val sourceSchema = SchemaBuilder.struct().name("source")
    .field("ts_ms", Schema.INT64_SCHEMA)
    .build()
  private val envelopeSchema = SchemaBuilder.struct().name("envelope")
    .field("before", rowSchema)
    .field("after", rowSchema)
    .field("source", sourceSchema)
    .field("op", Schema.STRING_SCHEMA)
    .build()

  private val commitTs = Timestamp.valueOf("2026-08-17 10:11:12.345")

  private def row(id: Int, value: String) = new Struct(rowSchema).put("id", id).put("value", value)

  private def changeEvent(op: String, before: Option[Struct], after: Option[Struct], ts: Timestamp = commitTs) = {
    val envelope = new Struct(envelopeSchema)
      .put("source", new Struct(sourceSchema).put("ts_ms", ts.getTime))
      .put("op", op)
    before.foreach(envelope.put("before", _))
    after.foreach(envelope.put("after", _))
    new SourceRecord(Collections.emptyMap[String, AnyRef](), Collections.emptyMap[String, AnyRef](), "topic", null,
      envelopeSchema, envelope)
  }

  test("change events are converted to the standard CDC columns") {
    val records = Seq(
      changeEvent("r", None, Some(row(1, "a"))), // snapshot read
      changeEvent("c", None, Some(row(2, "b"))), // create
      changeEvent("d", Some(row(3, "c")), None) // delete
    )

    val df = DebeziumEventConverter.convert(records)

    // data columns first, then the CDC metadata columns
    df.columns shouldBe Array("id", "value", Environment.cdcChangeTypeColumnName,
      Environment.cdcCommitTimestampColumnName, Environment.cdcChangeOrdinalColumnName)
    df.collect().toSeq should contain theSameElementsAs Seq(
      Row(1, "a", CdcChangeType.read, commitTs, 0L),
      Row(2, "b", CdcChangeType.insert, commitTs, 1L),
      Row(3, "c", CdcChangeType.delete, commitTs, 2L)
    )
  }

  test("an update is converted to a preimage and a postimage event") {
    val records = Seq(changeEvent("u", Some(row(1, "before")), Some(row(1, "after"))))

    val df = DebeziumEventConverter.convert(records)

    // preimage and postimage stem from the same change event and therefore share the change ordinal
    df.collect().toSeq should contain theSameElementsAs Seq(
      Row(1, "before", CdcChangeType.updatePreimage, commitTs, 0L),
      Row(1, "after", CdcChangeType.updatePostimage, commitTs, 0L)
    )
  }

  test("the change ordinal preserves the order of the delivered events") {
    // all events share the same commit timestamp, so only the ordinal tells them apart
    val records = (1 to 5).map(i => changeEvent("c", None, Some(row(i, s"v$i"))))

    val df = DebeziumEventConverter.convert(records)

    val ordinalsById = df.collect()
      .map(r => r.getAs[Int]("id") -> r.getAs[Long](Environment.cdcChangeOrdinalColumnName)).toMap
    ordinalsById shouldBe (1 to 5).map(i => i -> (i - 1L)).toMap
  }

  test("the commit timestamp keeps its milliseconds") {
    val records = Seq(changeEvent("c", None, Some(row(1, "a")), ts = Timestamp.valueOf("2026-08-17 10:11:12.987")))

    val df = DebeziumEventConverter.convert(records)

    df.collect().head.getAs[Timestamp](Environment.cdcCommitTimestampColumnName) shouldBe
      Timestamp.valueOf("2026-08-17 10:11:12.987")
  }
}
