/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

import org.scalatest.FunSuite

class ProductUtilTest extends FunSuite {

  test("get case class field value") {
    val p1 = XyzProduct("test", 1, true)
    val v = ProductUtil.getFieldData[Int](p1, "y").get
    assert(v == 1)
  }

  test("dynamic copy constructor") {
    val p1 = XyzProduct("test", 1, true)
    val p2 = ProductUtil.dynamicCopy(p1, "y", 2)
    assert(p2 == p1.copy(y = 2))
  }

}

case class XyzProduct(x: String, y: Int, z: Boolean)
