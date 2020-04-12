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

import java.io.File

import com.typesafe.config.ConfigFactory
import io.smartdatalake.util.hdfs.SparkRepartitionDef
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
 * Unit tests for [[CsvFileDataObject]].
 */
class CsvFileDataObjectTest extends DataObjectTestSuite with SparkFileDataObjectSchemaBehavior {

  import testSession.implicits._

  test("Reading from an empty file with header=true and inferSchema=false results in an empty, schema-less data frame.") {
    val tempFile = File.createTempFile("temp", "csv")
    tempFile.deleteOnExit()
    try {
      val config = ConfigFactory.parseString(
        s"""
           |{
           | id = src1
           | csv-options = {
           |  header = true
           |  inferSchema = false
           | }
           | path = "${escapedFilePath(tempFile.getPath)}"
           |}
         """.stripMargin)
      val dataObj = CsvFileDataObject.fromConfig(config, instanceRegistry)

      val df = dataObj.getDataFrame()
      df.schema shouldBe empty
      df shouldBe empty
    } finally {
      FileUtils.forceDelete(tempFile)
    }
  }

  test("Reading from an empty file with header=true and inferSchema=true results in an empty, schema-less data frame.") {
    val tempFile = File.createTempFile("temp", "csv")
    tempFile.deleteOnExit()
    try {
      val config = ConfigFactory.parseString(
        s"""
           |{
           | id = src1
           | csv-options = {
           |  header = true
           |  inferSchema = true
           | }
           | path = "${escapedFilePath(tempFile.getPath)}"
           |}
         """.stripMargin)
      val dataObj = CsvFileDataObject.fromConfig(config, instanceRegistry)

      val df = dataObj.getDataFrame()
      df.schema shouldBe empty
      df shouldBe empty
    } finally {
      FileUtils.forceDelete(tempFile)
    }
  }

  test("Reading from an empty file with header=false and inferSchema=true results in an empty, schema-less data frame.") {
    val tempFile = File.createTempFile("temp", "csv")
    tempFile.deleteOnExit()
    try {
      val config = ConfigFactory.parseString(
        s"""
           |{
           | id = src1
           | csv-options = {
           |  header = false
           |  inferSchema = true
           | }
           | path = "${escapedFilePath(tempFile.getPath)}"
           |}
         """.stripMargin)
      val dataObj = CsvFileDataObject.fromConfig(config, instanceRegistry)

      val df = dataObj.getDataFrame()
      df.schema shouldBe empty
      df shouldBe empty
    } finally {
      FileUtils.forceDelete(tempFile)
    }
  }

  testsFor(readEmptySources(createDataObject(Map("inferSchema" -> "false", "header" -> "false")), ".csv"))

  test("User-defined schema takes precedence over schema inference from header.") {
    val tempFile = File.createTempFile("temp", "csv")
    tempFile.deleteOnExit()


    testSession.createDataFrame(testSession.sparkContext.makeRDD(Seq(
      Row.fromTuple("A", "B"),
      Row.fromTuple("B", "1")
    )),
      StructType.fromDDL("h1 STRING, h2 STRING"))
      .write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
      .save(tempFile.getPath)

    try {
      val config = ConfigFactory.parseString(
        s"""
           |{
           | id = src1
           | csv-options = {
           |  header = true
           |  inferSchema = false
           |  delimiter = ","
           | }
           | schema = "header1 STRING, header2 INT"
           | path = "${escapedFilePath(tempFile.getPath)}"
           |}
         """.stripMargin)

      val dataObj = CsvFileDataObject.fromConfig(config, instanceRegistry)

      val df = dataObj.getDataFrame()

      df.schema should contain theSameElementsInOrderAs Seq(
        StructField("header1", StringType, nullable = true),
        StructField("header2", IntegerType, nullable = true)
      )
      df.count() shouldBe 1
      df.collect().foreach { row =>
        row(0).isInstanceOf[String] shouldBe true
        row(0) should equal ("B")
        row(1).isInstanceOf[Int] shouldBe true
        row(1) should equal (1)
      }

    } finally {
      FileUtils.forceDelete(tempFile)
    }
  }

  test("User-defined schema takes precedence over schema inference.") {
    val tempFile = File.createTempFile("temp", "csv")
    tempFile.deleteOnExit()


    testSession.createDataFrame(testSession.sparkContext.makeRDD(Seq(
      Row.fromTuple("A", "B"),
      Row.fromTuple("B", "1")
    )),
      StructType.fromDDL("h1 STRING, h2 STRING"))
      .write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
      .save(tempFile.getPath)

    try {
      val config = ConfigFactory.parseString(
        s"""
           |{
           | id = src1
           | csv-options = {
           |  header = false
           |  inferSchema = true
           |  delimiter = ","
           | }
           | schema = "header1 STRING, header2 INT"
           | path = "${escapedFilePath(tempFile.getPath)}"
           |}
         """.stripMargin)

      val dataObj = CsvFileDataObject.fromConfig(config, instanceRegistry)

      val df = dataObj.getDataFrame()

      df.schema should contain theSameElementsInOrderAs Seq(
        StructField("header1", StringType, nullable = true),
        StructField("header2", IntegerType, nullable = true)
      )
      df.count() shouldBe 2

    } finally {
      FileUtils.forceDelete(tempFile)
    }
  }

  testsFor(readNonExistingSources(createDataObject(Map("inferSchema" -> "true")), ".csv"))

  testsFor(validateSchemaMinOnWrite(createDataObjectWithSchemaMin(Map("header" -> "false", "inferSchema" -> "true")), fileExtension = ".csv"))
  testsFor(validateSchemaMinOnRead(createDataObjectWithSchemaMin(Map("header" -> "false", "inferSchema" -> "true")), fileExtension = ".csv"))

  test("Writing file with numberOfTasksPerPartition=1 results in 1 file written") {
    val tempFile = File.createTempFile("temp", "csv")
    tempFile.deleteOnExit()

    val dfInit = (1 to 1000).map( i => ("test", i)).toDF("name", "cnt")
      .repartition(10)
    val tgtDO = CsvFileDataObject(id="test1", path=escapedFilePath(tempFile.getPath), sparkRepartition=Some(SparkRepartitionDef(numberOfTasksPerPartition=1)))
    tgtDO.writeDataFrame(dfInit, Seq())
    val resultFileRefs = tgtDO.getFileRefs(Seq())
    resultFileRefs.size shouldBe 1
  }

  test("Writing file with numberOfTasksPerPartition=2 results in 2 files written") {
    val tempFile = File.createTempFile("temp", "csv")
    tempFile.deleteOnExit()

    val dfInit = (1 to 1000).map( i => ("test", i)).toDF("name", "cnt")
      .repartition(10)
    val tgtDO = CsvFileDataObject(id="test1", path=escapedFilePath(tempFile.getPath), sparkRepartition=Some(SparkRepartitionDef(numberOfTasksPerPartition=2)))
    tgtDO.writeDataFrame(dfInit, Seq())
    val resultFileRefs = tgtDO.getFileRefs(Seq())
    resultFileRefs.size shouldBe 2
  }

  def createDataObject(options: Map[String, String])(path: String, schemaOpt: Option[StructType]): CsvFileDataObject = {
    val dataObj = CsvFileDataObject(id = "schemaTestCsvDO", path = path, schema = schemaOpt, csvOptions = options)
    instanceRegistry.register(dataObj)
    dataObj
  }

  def createDataObjectWithSchemaMin(options: Map[String, String])(path: String, schemaOpt: Option[StructType], schemaMinOpt: Option[StructType]): CsvFileDataObject = {
    val dataObj = CsvFileDataObject(id = "schemaTestCsvDO", path = path, schema = schemaOpt, schemaMin = schemaMinOpt, csvOptions = options)
    instanceRegistry.register(dataObj)
    dataObj
  }

  override def createFile(path: String, data: DataFrame): Unit = {
    data.write.format("com.databricks.spark.csv").save(path)
  }
}
