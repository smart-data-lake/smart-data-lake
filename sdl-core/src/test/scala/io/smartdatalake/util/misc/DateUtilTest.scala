/*
 * sdl-core - Build your data lake the smart way.
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

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneId, ZoneOffset, ZonedDateTime}

class DateUtilTest extends AnyFunSpec with Matchers {

  private val currentTime = LocalDateTime.now.truncatedTo(ChronoUnit.MILLIS)
  private val currentTimeAtUtc = currentTime.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneOffset.UTC)

  describe("Flexible LocalDateParser") {

    it("should parse a date-time string without timezone") {
      val result = DateUtil.parseDateTimeToLocalDateTime(currentTime.toString)
      result shouldBe currentTime
    }

    it("should parse a date-time string with UTC timezone to local timezone") {
      val result = DateUtil.parseDateTimeToLocalDateTime(currentTimeAtUtc.toString)
      result shouldBe currentTime
    }
  }

  describe("LocalDate To UTC converter") {

    it("should create an ISO date-time string with UTC timezone") {
      val result = DateUtil.convertLocalDateTimeToUtcISOString(currentTime)
      result shouldBe currentTime.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneOffset.UTC).toString
      result should endWith("Z")
    }

    it("should be an ISO date-time string with UTC timezone") {
      val result = DateUtil.convertLocalDateTimeToUtcISOString(currentTime)
      result shouldBe currentTime.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneOffset.UTC).toString
      result should endWith("Z")
    }

    it("should be parsable by ZonedDateTime") {
      val result = DateUtil.convertLocalDateTimeToUtcISOString(currentTime)
      val parsed = ZonedDateTime.parse(result)
      parsed shouldBe currentTime.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneOffset.UTC)
    }
  }
}