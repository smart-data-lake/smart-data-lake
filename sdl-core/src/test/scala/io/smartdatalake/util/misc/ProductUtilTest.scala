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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProductUtilTest extends AnyFlatSpec with Matchers {

  val xyProd = XyzProduct(x = "test", y = 1, z = true)

  "getFieldData" should "get case class field value" in {
    val v = ProductUtil.getFieldData[Int](xyProd, "y").get
    v should be(1)
  }

  "dynamicCopy" should "dynamic copy constructor" in {
    val p2 = ProductUtil.dynamicCopy(xyProd, "y", 2)
    p2 should be(xyProd.copy(y = 2))
  }

  "toDebugString" should "return string for debugging" in {
    ProductUtil.toDebugString(xyProd) should be("io.smartdatalake.util.misc.XyzProduct(x=test, y=1, z=true)")
  }

  "toDebugString" should "succeed even if a field is null" in {
    ProductUtil.toDebugString(XyzProduct(x = null.asInstanceOf[String], y = 1, z = true)) should
      be("io.smartdatalake.util.misc.XyzProduct(x=null, y=1, z=true)")
  }
}

case class XyzProduct(x: String, y: Int, z: Boolean)
