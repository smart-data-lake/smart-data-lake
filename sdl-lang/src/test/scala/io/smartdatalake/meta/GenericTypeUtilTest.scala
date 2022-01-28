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

package io.smartdatalake.meta

import org.scalatest.FunSuite

import scala.reflect.runtime.universe.typeOf

class GenericTypeUtilTest extends FunSuite {

  test ("handle attributes - required, deprecated, override") {
    val testObjectTypeDef = GenericTypeUtil.typeDefForClass(typeOf[TestObject])
    assert(testObjectTypeDef.name == "TestObject")
    val attrsExpected = Seq(
      ("id",true,false,false),
      ("javaDeprecated",true,true,false),
      ("scalaDeprecated",true,false,false), // scala annotations are *not* kept for runtime and will not be reflected in type def...
      ("optional",false,false,false),
      ("default",false,false,false),
      ("overrideTest",true,false,true)
    )
    assert(testObjectTypeDef.attributes.map(a => (a.name,a.isRequired,a.isDeprecated,a.isOverride)) == attrsExpected)
  }

  test("scala doc from case class attribute and from overridden method as fallback") {
    val testObjectTypeDef = GenericTypeUtil.typeDefForClass(typeOf[TestObject])
    assert(testObjectTypeDef.name == "TestObject")
    assert(testObjectTypeDef.attributes.find(_.name == "id").get.description.contains("parameter test"))
    assert(testObjectTypeDef.attributes.find(_.name == "overrideTest").get.description.contains("method override test"))
  }

}

/**
 * ScalaDoc on case class
 * @param id parameter test
 */
case class TestObject (
                       id: String,
                       @Deprecated javaDeprecated: String,
                       @deprecated scalaDeprecated: String, // scala annotations are *not* kept for runtime and will not be reflected in type def...
                       optional: Option[String], default: String = "",
                       override val overrideTest: String
                     ) extends OverrideTest

trait OverrideTest {
  /**
   * method override test
   */
  def overrideTest: String
  def methodTest(): String = ""
}