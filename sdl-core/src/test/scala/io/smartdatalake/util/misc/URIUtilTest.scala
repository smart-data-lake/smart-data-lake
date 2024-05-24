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

import java.net.URI

class URIUtilTest extends FunSuite {
  test("add path should append path to existing URI path, without changing the rest of the URI") {
    {
      val uri = new URI("https://test.com/abc?name=123")
      val result = URIUtil.appendPath(uri, "def")
      assert(result == new URI("https://test.com/abc/def?name=123"))
    }
  }

  test("add path should append path to existing URI without path, without changing the rest of the URI") {
    {
      val uri = new URI("https://test.com/?name=123")
      val result = URIUtil.appendPath(uri, "def")
      assert(result == new URI("https://test.com/def?name=123"))
    }
    {
      val uri = new URI("https://test.com?name=123")
      val result = URIUtil.appendPath(uri, "def")
      assert(result == new URI("https://test.com/def?name=123"))
    }
  }
}
