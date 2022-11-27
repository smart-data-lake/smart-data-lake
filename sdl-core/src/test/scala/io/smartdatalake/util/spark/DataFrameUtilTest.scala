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

import io.smartdatalake.util.spark.DataFrameUtil.{normalizeToAscii, removeNonStandardSQLNameChars, strCamelCase2LowerCaseWithUnderscores}
import org.scalatest.FunSuite

class DataFrameUtilTest extends FunSuite {


  test("camelCase to lower_case_with_underscore") {
    assert(strCamelCase2LowerCaseWithUnderscores("abc0") == "abc0")
    assert(strCamelCase2LowerCaseWithUnderscores("aBc_d0") == "a_bc_d0")
    assert(strCamelCase2LowerCaseWithUnderscores("aBC0") == "a_bc0")
    assert(strCamelCase2LowerCaseWithUnderscores("AbcABc_aBC0") == "abc_abc_a_bc0")
    assert(strCamelCase2LowerCaseWithUnderscores("_AbcABc_aBC0") == "_abc_abc_a_bc0")
    assert(strCamelCase2LowerCaseWithUnderscores("__abcABc_aBC0") == "__abc_abc_a_bc0")
  }

  test("normalize UTF8 to ASCII") {
    assert(normalizeToAscii("Äöü_éà") == "Aeoeue_ea")
  }

  test("remove non standard SQL name chars") {
    assert(removeNonStandardSQLNameChars("a-!$* A") == "aa")
  }

}
