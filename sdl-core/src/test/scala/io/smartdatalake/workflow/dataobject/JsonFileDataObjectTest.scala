/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.dataobject

import java.io._
import java.nio.file.Files

import com.typesafe.config.ConfigFactory
import io.smartdatalake.testutils.DataObjectTestSuite
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

class JsonFileDataObjectTest extends DataObjectTestSuite with SparkFileDataObjectSchemaBehavior {

  val hdfs = FileSystem.get(session.sparkContext.hadoopConfiguration)

  case class Data(name: String, age: Int)

  case class DataObj(id: String, seq: Seq[Data])

  test("test stringify") {

    val tempDir = Files.createTempDirectory("jsonTest").toString

    val config = ConfigFactory.parseString( s"""
         | id = src1
         | path = "${escapedFilePath(tempDir)}"
         | json-options { multiLine = false }
         | stringify = true
         """.stripMargin)

    val jsonStr =
      """
        |{"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}}
        |{"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}}
        |{"string":"string3","int":3,"array":[3,6,9],"dict": {"key": "value3", "extra_key": "extra_value3"}}
      """.stripMargin

    val output = hdfs.create(new Path(tempDir+"/test.json"), true)
    val os = new BufferedOutputStream(output)
    os.write(jsonStr.getBytes("UTF-8"))
    os.close()

    val aj = JsonFileDataObject.fromConfig(config)
    val result = aj.getDataFrame()
    result.show
    assert(result.count() == 3)

    val expectedSchema = StructType(List(
      StructField("array", StringType),
      StructField("dict", StringType),
      StructField("int", StringType),
      StructField("string", StringType)))

    val testResult = result.schema == expectedSchema

    if (!testResult) {
      println("Schemata differ:")
      println(s"result.schema  = ${result.schema.simpleString}")
      println(s"expectedSchema = ${expectedSchema.simpleString}")
    }
    assert(testResult)
  }


  test("testDefaultParsing") {

    val tempDir = Files.createTempDirectory("jsonTest").toString

    val config = ConfigFactory.parseString( s"""
         | id = src1
         | path = "${escapedFilePath(tempDir)}"
         """.stripMargin)

    val jsonStr =
      """{
        |  "a_string": "string3",
        |  "an_int": 3,
        |  "array": [
        |    3,
        |    6,
        |    9
        |  ],
        |  "dict": {
        |    "key": "value3",
        |    "extra_key": "extra_value3"
        |  }
        |}""".stripMargin

    val output = hdfs.create(new Path(tempDir+"/test.json"), true)
    val os = new BufferedOutputStream(output)
    os.write(jsonStr.getBytes("UTF-8"))
    os.close()

    val aj = JsonFileDataObject.fromConfig(config)
    val result = aj.getDataFrame()
    result.show()
    assert(result.count() == 1)

    val expectedSchema = StructType(List(
      StructField("a_string", StringType),
      StructField("an_int", LongType),
      StructField("array", ArrayType(LongType)),
      StructField("dict",
        StructType(List(
          StructField("extra_key", StringType),
          StructField("key", StringType))))
    ))
    result.printSchema()
    assert(result.schema == expectedSchema)

    FileUtils.deleteDirectory(new File(tempDir))
  }

  test("jsonLinesParsing") {

    val tempDir = Files.createTempDirectory("jsonTest").toString

    val config = ConfigFactory.parseString( s"""
         | id = src1
         | path = "${escapedFilePath(tempDir)}"
         | json-options { multiLine = false }
         """.stripMargin)

    val jsonStr =
      """
        |{"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}}
        |{"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}}
        |{"string":"string3","int":3,"array":[3,6,9],"dict": {"key": "value3", "extra_key": "extra_value3"}}
      """.stripMargin

    val output = hdfs.create(new Path(tempDir+"/test.json"), true)
    val os = new BufferedOutputStream(output)
    os.write(jsonStr.getBytes("UTF-8"))
    os.close()

    val aj = JsonFileDataObject.fromConfig(config)
    val result = aj.getDataFrame()
    assert(result.count() == 3)

    val expectedSchema = StructType(List(
      StructField("array", ArrayType(LongType)),
      StructField("dict",
        StructType(List(StructField("extra_key", StringType),
          StructField("key", StringType)))),
      StructField("int", LongType),
      StructField("string", StringType)))
    assert(result.schema == expectedSchema)

    FileUtils.deleteDirectory(new File(tempDir))
  }

  testsFor(readNonExistingSources(createDataObject, ".json"))
  testsFor(readEmptySources(createDataObject, ".json"))
  testsFor(validateSchemaMinOnWrite(createDataObjectWithSchemaMin, ".json"))
  testsFor(validateSchemaMinOnRead(createDataObjectWithSchemaMin, ".json"))

  private def createDataObject(path: String, schemaOpt: Option[StructType]): JsonFileDataObject = {
    val dataObj = JsonFileDataObject(id = "schemaTestJsonDO", path = path, schema = schemaOpt)
    instanceRegistry.register(dataObj)
    dataObj
  }

  private def createDataObjectWithSchemaMin(path: String, schemaOpt: Option[StructType], schemaMinOpt: Option[StructType]): JsonFileDataObject = {
    val dataObj = JsonFileDataObject(id = "schemaTestJsonDO", path = path, schema = schemaOpt, schemaMin = schemaMinOpt)
    instanceRegistry.register(dataObj)
    dataObj
  }

  override def createFile(path: String, data: DataFrame): Unit = {
    data.write.format("json").save(path)
  }
}
