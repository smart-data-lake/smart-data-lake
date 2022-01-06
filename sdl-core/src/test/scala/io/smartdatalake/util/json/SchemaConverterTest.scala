/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.json

import io.smartdatalake.util.misc.TryWithRessource
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.io.Source

/**
 * This code originates from https://github.com/zalando-incubator/spark-json-schema and is protected by its corresponding MIT license.
 */
class SchemaConverterTest extends FunSuite with Matchers with BeforeAndAfter {

  val expectedStruct = StructType(Array(
    StructField("object", StructType(Array(
      StructField("item1", StringType, nullable = false),
      StructField("item2", StringType, nullable = false)
    )), nullable = false),
    StructField("array", ArrayType(StructType(Array(
      StructField("itemProperty1", StringType, nullable = false),
      StructField("itemProperty2", DoubleType, nullable = false)
    )), containsNull = false), nullable = false),
    StructField("structure", StructType(Array(
      StructField("nestedArray", ArrayType(StructType(Array(
        StructField("key", StringType, nullable = false),
        StructField("value", LongType, nullable = false)
      )), containsNull = false), nullable = false)
    )), nullable = false),
    StructField("integer", LongType, nullable = false),
    StructField("string", StringType, nullable = false),
    StructField("number", DoubleType, nullable = false),
    StructField("float", FloatType, nullable = false),
    StructField("nullable", DoubleType, nullable = true),
    StructField("boolean", BooleanType, nullable = false),
    StructField("additionalProperty", StringType, nullable = false)
  ))

  test("should convert schema.json into spark StructType") {
    val testSchema = SchemaConverter.convertStrict(getTestResourceContent("/jsonSchema/testJsonSchema.json"))
    assert(testSchema === expectedStruct)
  }

  test("should convert schema.json content into spark StructType") {
    val testSchema = SchemaConverter.convertStrict(getTestResourceContent("/jsonSchema/testJsonSchema.json"))
    assert(testSchema === expectedStruct)
  }

  // 'id' and 'name' are optional according to http://json-schema.org/latest/json-schema-core.html
  test("should support optional 'id' and 'name' properties") {
    val testSchema = SchemaConverter.convertStrict(getTestResourceContent("/jsonSchema/testJsonSchema3.json"))
    assert(testSchema === expectedStruct)
  }

  test("schema should support references") {
    val schema = SchemaConverter.convertStrict(getTestResourceContent("/jsonSchema/testJsonSchema4.json"))

    val expected = StructType(Array(
      StructField("name", StringType, nullable = false),
      StructField("addressA", StructType(Array(
        StructField("zip", StringType, nullable = true)
      )), nullable = false),
      StructField("addressB", StructType(Array(
        StructField("zip", StringType, nullable = true)
      )), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Empty object should be possible") {
    val schema = SchemaConverter.convertStrict(
      """
        {
          "$schema": "smallTestSchema",
          "type": "object",
          "properties": {
            "address": {
              "type": "object"
            }
          }
        }
      """
    )
    val expected = StructType(Array(
      StructField("address", StructType(Seq.empty), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Known primitive type array should be an array of this type") {
    val typeMap = Map(
      "string" -> StringType,
      "number" -> DoubleType,
      "float" -> FloatType,
      "integer" -> LongType,
      "boolean" -> BooleanType
    )
    typeMap.foreach {
      case p @ (key, datatype) =>
        val schema = SchemaConverter.convertStrict(
          s"""
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "$key"
                }
              }
            }
          }
        """
        )
        val expected = StructType(Array(
          StructField("array", ArrayType(datatype, containsNull = false), nullable = false)
        ))

        assert(schema === expected)
    }
  }

  test("Array of array should be an array of array") {

    val schema = SchemaConverter.convertStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "array",
                  "items": {
                    "type": "string"
                  }
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(ArrayType(StringType, containsNull = false), containsNull = false), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Array of object should be an array of object") {

    val schema = SchemaConverter.convertStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "object"
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(StructType(Seq.empty), containsNull = false), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Array of object with properties should be an array of object with these properties") {

    val schema = SchemaConverter.convertStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items": {
                  "type": "object",
                  "properties": {
                    "name" : {
                      "type" : "string"
                    }
                  }
                }
              }
            }
          }
        """
    )
    val expected = StructType(Array(
      StructField("array", ArrayType(StructType(Seq(StructField("name", StringType, nullable = false))), containsNull = false), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Array of unknown type should fail") {

    assertThrows[IllegalArgumentException] {
      val schema = SchemaConverter.convertStrict(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {}
              }
            }
          }
        """
      )
    }
  }

  test("Array of various type should fail") {
    assertThrows[IllegalArgumentException] {
      val schema = SchemaConverter.convertStrict(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["string", "integer"]
                }
              }
            }
          }
        """
      )
    }
  }

  test("Array of nullable type should be an array of nullable type") {
    val typeMap = Map(
      "string" -> StringType,
      "number" -> DoubleType,
      "float" -> FloatType,
      "integer" -> LongType,
      "boolean" -> BooleanType
    )
    typeMap.foreach {
      case (name, atype) =>
        val schema = SchemaConverter.convertStrict(
          s"""
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["$name", "null"]
                }
              }
            }
          }
        """
        )

        val expected = StructType(Array(
          StructField("array", ArrayType(atype, containsNull = true), nullable = false)
        ))

        assert(schema === expected)
    }
  }

  test("Array of non-nullable type should be an array of non-nullable type") {
    val schema = SchemaConverter.convertStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["string"]
                }
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("array", ArrayType(StringType, containsNull = false), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Array of nullable object should be an array of nullable object") {
    val schema = SchemaConverter.convertStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : "array",
                "items" : {
                  "type" : ["object", "null"],
                  "properties" : {
                    "prop" : {
                      "type" : "string"
                    }
                  }
                }
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("array", ArrayType(
        StructType(Seq(StructField("prop", StringType, nullable = false))), containsNull = true
      ), nullable = false)
    ))

    assert(schema === expected)
  }

  test("Nullable array should be an array or a null value") {
    val schema = SchemaConverter.convertStrict(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "array" : {
                "type" : ["array", "null"],
                "items" : {
                  "type" : "string"
                }
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("array", ArrayType(StringType, containsNull = false), nullable = true)
    ))

    assert(schema === expected)
  }

  test("Multiple types should fail with strict typing") {
    assertThrows[IllegalArgumentException] {
      val schema = SchemaConverter.convertStrict(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : ["integer", "float"]
              }
            }
          }
        """
      )

    }
  }

  test("Multiple types should default to string without strict typing") {
    val schema = SchemaConverter.convert(
      """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : ["integer", "float"]
              }
            }
          }
        """
    )

    val expected = StructType(Array(
      StructField("prop", StringType, nullable = false)
    ))

    assert(schema === expected)
  }

  test("null type only should fail") {
    assertThrows[NoSuchElementException] {
      val schema = SchemaConverter.convertStrict(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : "null"
              }
            }
          }
        """
      )
    }
  }

  test("null type only should fail event as a single array element") {
    assertThrows[IllegalArgumentException] {
      val schema = SchemaConverter.convertStrict(
        """
          {
            "$$schema": "smallTestSchema",
            "type": "object",
            "properties": {
              "prop" : {
                "type" : ["null"]
              }
            }
          }
        """
      )
    }
  }

  def getTestResourceContent(relativePath: String): String = {
    Option(getClass.getResource(relativePath)) match {
      case Some(relPath) => TryWithRessource.exec(Source.fromURL(relPath))(_.mkString)
      case None => throw new IllegalArgumentException(s"Path can not be reached: $relativePath")
    }
  }

}