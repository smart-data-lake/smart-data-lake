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
package io.smartdatalake.util.json

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DateUtil
import io.smartdatalake.workflow.action.RuntimeEventState
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import org.json4s.Extraction.decompose
import org.json4s.JsonAST._
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization
import org.json4s.{CustomKeySerializer, CustomSerializer, DefaultFormats, Formats, TypeHints}

import java.time.{Duration, LocalDateTime}

/**
 * Core JSON utilities using json4s directly without Spark dependencies.
 * Only contains what sdl-core itself needs. The full sdl-spark JsonUtils
 * (with Spark-specific type converters) lives in sdl-spark.
 */
private[smartdatalake] object SdlJsonUtils {

  def caseClassToJsonString(instance: AnyRef)(implicit formats: Formats): String =
    Serialization.write(instance)

  private val durationSerializer = new CustomSerializer[Duration](_ =>
    (
      { case json: JString => Duration.parse(json.s) },
      { case obj: Duration => JString(obj.toString) }
    )
  )

  private val localDateTimeToUtcSerializer = new CustomSerializer[LocalDateTime](_ =>
    (
      { case json: JString => DateUtil.parseDateTimeToLocalDateTime(json.s) },
      { case obj: LocalDateTime => JString(DateUtil.convertLocalDateTimeToUtcISOString(obj)) }
    )
  )

  private val actionIdKeySerializer = new CustomKeySerializer[ActionId](_ =>
    (
      { case s: String => ActionId(s) },
      { case obj: ActionId => obj.id }
    )
  )

  private val dataObjectIdKeySerializer = new CustomKeySerializer[DataObjectId](_ =>
    (
      { case s: String => DataObjectId(s) },
      { case obj: DataObjectId => obj.id }
    )
  )

  private val dataObjectIdSerializer = new CustomSerializer[DataObjectId](_ =>
    (
      { case json: JString => DataObjectId(json.s) },
      { case obj: DataObjectId => JString(obj.id) }
    )
  )

  private val runtimeEventStateKeySerializer = new CustomKeySerializer[RuntimeEventState](_ =>
    (
      { case s: String => RuntimeEventState.withName(s) },
      { case obj: RuntimeEventState => obj.toString }
    )
  )

  private val partitionValuesSerializer = new CustomSerializer[PartitionValues](_ =>
    (
      { case json: JObject => PartitionValues(json.values) },
      { case obj: PartitionValues => JObject(obj.elements.map(e => JField(e._1, decompose(e._2)(DefaultFormats))).toList) }
    )
  )

  def getFormats(typeHints: TypeHints): Formats =
    Serialization.formats(typeHints) +
      new EnumNameSerializer(RuntimeEventState) +
      actionIdKeySerializer +
      dataObjectIdKeySerializer +
      dataObjectIdSerializer +
      durationSerializer +
      localDateTimeToUtcSerializer +
      runtimeEventStateKeySerializer +
      partitionValuesSerializer
}
