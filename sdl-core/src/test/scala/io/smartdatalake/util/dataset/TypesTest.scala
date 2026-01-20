package io.smartdatalake.util.dataset

import io.smartdatalake.util.spark.dataset.Types
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypesTest extends AnyFlatSpec with Matchers with Types {

  "createStruct" should "created a struct" in {
    val argument = Array[(String, DataType, Boolean)](("id", IntegerType, false),
      ("name", StringType, false), ("birthdate", DateType, true))
    val actual = createStruct(argument)
    val expected = StructType(Array(StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("birthdate", DateType, nullable = true)))
    actual shouldEqual expected
  }

}
