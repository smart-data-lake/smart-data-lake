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

package io.smartdatalake.meta.jsonschema

import io.smartdatalake.meta.GenericTypeDef
import io.smartdatalake.meta.GenericTypeUtil.{attributesForCaseClass}
import org.reflections.Reflections
import org.scalatest.FunSuite

import scala.reflect.runtime.universe.typeOf

class JsonTypeConverterTest extends FunSuite {
  private val registry = new DefinitionRegistry
  private val reflections = new Reflections
  private val jsonTypeConverter = new JsonSchemaUtil.JsonTypeConverter(reflections, registry)

  trait BaseType
  case class TestCaseClass(name: String, age: Option[Int]) extends BaseType

  test("convert types to json types") {
    val attributes = attributesForCaseClass(typeOf[TestCaseClass], Map())
    val typeDef = GenericTypeDef("testTypeDef", None, typeOf[TestCaseClass], None, true, Set(), attributes)

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties("name").isInstanceOf[JsonStringDef])
    assert(jsonTypeDef.properties("age").isInstanceOf[JsonIntegerDef])
    assert(jsonTypeDef.required == List("name"))
  }

  test("required type attribute is added if base type is present") {
    val attributes = attributesForCaseClass(typeOf[TestCaseClass], Map())
    val typeDef = GenericTypeDef("testTypeDef", Some(typeOf[BaseType]), typeOf[TestCaseClass], None, true, Set(), attributes)

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties.contains("type"))
    assert(jsonTypeDef.required == List("type", "name"))
  }

  test("type attribute is not added without base type") {
    val attributes = attributesForCaseClass(typeOf[TestCaseClass], Map())
    val typeDef = GenericTypeDef("testTypeDef", None, typeOf[TestCaseClass], None, true, Set(), attributes)

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(!jsonTypeDef.properties.contains("type"))
    assert(jsonTypeDef.required == List("name"))
  }
}
