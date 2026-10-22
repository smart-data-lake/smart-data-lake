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
package io.smartdatalake.util.misc

import java.time.format.DateTimeParseException
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{LocalDateTime, ZoneId, ZoneOffset, ZonedDateTime}

object DateUtil {

  def parseDateTimeToLocalDateTime(dateTime: String): LocalDateTime = {
    try {
      // try parsing dateTime with Timezone information, and convert to local timezone and LocalDateTime.
      ZonedDateTime.parse(dateTime).withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime
    } catch {
      case _: DateTimeParseException => LocalDateTime.parse(dateTime)
    }
  }

  def convertLocalDateTimeToUtcISOString(dateTime: LocalDateTime, precision: TemporalUnit = ChronoUnit.MILLIS): String = {
    dateTime.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneOffset.UTC).truncatedTo(precision).toString
  }

}
