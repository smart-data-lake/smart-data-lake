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

import io.smartdatalake.config.{FromConfigFactory, SdlConfigObject}
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConfigObjectId, ConnectionId, DataObjectId}
import io.smartdatalake.meta.GenericTypeDef
import io.smartdatalake.meta.GenericTypeUtil.attributesForCaseClass
import io.smartdatalake.meta.jsonschema.TestEnum.TestEnum
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.connection.{Connection, ConnectionMetadata}
import io.smartdatalake.workflow.dataobject.{DataObject, DataObjectMetadata}
import org.reflections.Reflections
import org.scalatest.FunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

class JsonTypeConverterTest extends FunSuite {
  private val registry = new DefinitionRegistry
  private val reflections = new Reflections
  private val jsonTypeConverter = new JsonSchemaUtil.JsonTypeConverter(reflections, registry)

  trait BaseType
  case class TestCaseClass(name: String, age: Option[Int]) extends BaseType

  test("convert types to json types") {
    val typeDef = getGenericTypeDef(typeOf[TestCaseClass])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties("name").isInstanceOf[JsonStringDef])
    assert(jsonTypeDef.properties("age").isInstanceOf[JsonIntegerDef])
    assert(jsonTypeDef.required == List("name"))
  }

  test("required type attribute is added if base type is present") {
    val typeDef = getGenericTypeDef(typeOf[TestCaseClass], Some(typeOf[BaseType]))

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties.contains("type"))
    assert(jsonTypeDef.required == List("type", "name"))
  }

  test("type attribute is not added without base type") {
    val typeDef = getGenericTypeDef(typeOf[TestCaseClass])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(!jsonTypeDef.properties.contains("type"))
    assert(jsonTypeDef.required == List("name"))
  }

  case class TestConfigObjectWithId(id: ConfigObjectId) extends SdlConfigObject
  test("do not add id in subtype of SdlConfigObject to json schema") {
    val typeDef = getGenericTypeDef(typeOf[TestConfigObjectWithId])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(!jsonTypeDef.properties.contains("id"))
  }

  case class TestConfigObjectWithActionId(id: ActionId) extends SdlConfigObject
  test("do not add id of type ActionId in subtype of SdlConfigObject to json schema") {
    val typeDef = getGenericTypeDef(typeOf[TestConfigObjectWithActionId])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(!jsonTypeDef.properties.contains("id"))
  }

  case class TestDataObjectWithDataObjectId(id: DataObjectId) extends DataObject {
    override def metadata: Option[DataObjectMetadata] = None
    override def factory: FromConfigFactory[DataObject] = null
  }
  test("do not add id in subtype of DataObject to json schema") {
    val typeDef = getGenericTypeDef(typeOf[TestDataObjectWithDataObjectId])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(!jsonTypeDef.properties.contains("id"))
  }

  case class TestConnectionWithConnectionId(id: ConnectionId) extends Connection {
    override def metadata: Option[ConnectionMetadata] = None
    override def factory: FromConfigFactory[Connection] = null
  }
  test("do not add id in subtype of ConnectionId to json schema") {
    val typeDef = getGenericTypeDef(typeOf[TestConnectionWithConnectionId])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(!jsonTypeDef.properties.contains("id"))
  }

  case class TestClassWithStringId(id: String)
  test("show id of type String in json schema") {
    val typeDef = getGenericTypeDef(typeOf[TestClassWithStringId])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties.contains("id"))
  }

  case class TestClassWithIntId(id: Int)
  test("show id of type Int in json schema") {
    val typeDef = getGenericTypeDef(typeOf[TestClassWithIntId])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties.contains("id"))
  }

  case class TestClassWithReferenceToId(id: ConfigObjectId, referenceId: ConfigObjectId) extends SdlConfigObject
  test("show reference to id in subtype of SdlConfigObject in json schema") {
    val typeDef = getGenericTypeDef(typeOf[TestClassWithReferenceToId])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(!jsonTypeDef.properties.contains("id"))
    assert(jsonTypeDef.properties.contains("referenceId"))
  }

  case class TestClassWithBoolean(flag: Boolean)
  test("Boolean case class attributes are converted to boolean in JSON schema") {
    val typeDef = getGenericTypeDef(typeOf[TestClassWithBoolean])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties.contains("flag"))
    assert(jsonTypeDef.properties("flag").isInstanceOf[JsonBooleanDef])
  }

  case class TestClassWithSecret(secret: StringOrSecret)
  test("StringOrSecret is converted to string with additional description") {
    val typeDef = getGenericTypeDef(typeOf[TestClassWithSecret])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties("secret").isInstanceOf[JsonStringDef])
    assert(jsonTypeDef.properties("secret").asInstanceOf[JsonStringDef].description.get.contains("```###<PROVIDERID>#<SECRETNAME>###```"))
  }

  case class TestClassWithSecretsInOptions(options: Map[String, StringOrSecret])
  test("Map with StringOrSecret values is has additional description") {
    val typeDef = getGenericTypeDef(typeOf[TestClassWithSecretsInOptions])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties("options").isInstanceOf[JsonMapDef])
    assert(jsonTypeDef.properties("options").asInstanceOf[JsonMapDef].description.get.contains("```###<PROVIDERID>#<SECRETNAME>###```"))
  }

  case class TestClassWithoutSecretsInOptions(options: Map[String, String])
  test("Map without StringOrSecret values has no additional description") {
    val typeDef = getGenericTypeDef(typeOf[TestClassWithoutSecretsInOptions])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties("options").isInstanceOf[JsonMapDef])
    assert(jsonTypeDef.properties("options").asInstanceOf[JsonMapDef].description.isEmpty)
  }

  private def getGenericTypeDef(tpe: Type, baseType: Option[Type] = None): GenericTypeDef = {
    val attributes = attributesForCaseClass(tpe, Map())
    GenericTypeDef("testTypeDef", baseType, tpe, None, true, Set(), attributes)
  }

  case class TestClassWithEnum(testEnum: TestEnum)
  test("convert scala enum to json enum") {
    val typeDef = getGenericTypeDef(typeOf[TestClassWithEnum])

    val jsonTypeDef = jsonTypeConverter.fromGenericTypeDef(typeDef)

    assert(jsonTypeDef.properties("testEnum").isInstanceOf[JsonStringDef])
    val jsonEnum = jsonTypeDef.properties("testEnum").asInstanceOf[JsonStringDef]
    assert(jsonEnum.enum.get.toSet == Set("firstValue", "secondValue"))
   }
}

object TestEnum extends Enumeration {
  type TestEnum = Value
  val firstValue = Value("firstValue")
  val secondValue = Value("secondValue")
}
