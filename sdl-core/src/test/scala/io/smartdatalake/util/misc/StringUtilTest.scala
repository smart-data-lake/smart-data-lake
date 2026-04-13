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

import io.smartdatalake.testutils.TestTool
import io.smartdatalake.util.misc.StringUtil._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

class StringUtilTest extends AnyFlatSpec with Matchers
  with TestTool {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  "strCamelCase2LowerCaseWithUnderscores" should "transform camelCase to lower_case_with_underscore" in {
    val argExpMap = Map(
      "abc0" -> "abc0", "aBc_d0" -> "a_bc_d0", "aBC0" -> "a_bc0", "AbcABc_aBC0" -> "abc_abc_a_bc0",
      "_AbcABc_aBC0" -> "_abc_abc_a_bc0", "__abcABc_aBC0" -> "__abc_abc_a_bc0")
    testArgumentExpectedMap[String, String](strCamelCase2LowerCaseWithUnderscores, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

  "strToLowerCamelCase" should "transform string to lowerCamelCase" in {
    val argExpMap = Map("abc0" -> "abc0", "aBc_d0" -> "aBcD0", "aBC0" -> "aBC0",
      "Abc-ABc_aBC0" -> "abcABcABC0", "_AbcABc aBC0" -> "abcABcABC0")
    testArgumentExpectedMap[String, String](strToLowerCamelCase, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

  "normalizeToAscii" should "transform UTF8 to ASCII" in {
    val argExpMap = Map("abc Äöü_éà" -> "abc Aeoeue_ea",
      "un peu de maths: ω² + ω^ω = ω^ω" -> "un peu de maths:  + ^ = ^")
    testArgumentExpectedMap[String, String](normalizeToAscii, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

  "removeNonStandardSQLNameChars" should "remove non standard SQL name chars" in {
    val argExpMap = Map("a-!$* A" -> "aa",
      "un peu de maths: ω² + ω^ω = ω^ω" -> "unpeudemaths")
    testArgumentExpectedMap[String, String](removeNonStandardSQLNameChars, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

}
