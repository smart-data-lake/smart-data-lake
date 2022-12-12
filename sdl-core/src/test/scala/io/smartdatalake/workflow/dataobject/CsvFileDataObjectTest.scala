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

import com.typesafe.config.ConfigFactory
import io.smartdatalake.testutils.{DataObjectTestSuite, TestUtil}
import io.smartdatalake.util.hdfs.{PartitionValues, SparkRepartitionDef}
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import java.io.FileInputStream
import java.nio.file.Files
import java.util.zip.ZipInputStream
import scala.util.Random

/**
 * Unit tests for [[CsvFileDataObject]].
 */
class CsvFileDataObjectTest extends DataObjectTestSuite with SparkFileDataObjectSchemaBehavior {

  import session.implicits._

  test("Reading from an empty file with header=true and inferSchema=false results in an empty, schema-less data frame.") {
    val tempDir = Files.createTempDirectory("csv")
    val tempFile = Files.createTempFile(tempDir, "temp", "csv") // attention, this creates not a proper .csv file type, but just appends <filename>csv...
    try {
      val config = ConfigFactory.parseString(
        s"""
           |{
           | id = src1
           | csv-options = {
           |  header = true
           |  inferSchema = false
           | }
           | path = "${escapedFilePath(tempFile.toString)}"
           |}
         """.stripMargin)
      val dataObj = CsvFileDataObject.fromConfig(config)

      val df = dataObj.getSparkDataFrame()
      df.schema shouldBe empty
      df shouldBe empty
    } finally {
      FileUtils.forceDelete(tempDir.toFile)
    }
  }

  test("Reading from an empty file with header=true and inferSchema=true results in an empty, schema-less data frame.") {
    val tempDir = Files.createTempDirectory("csv")
    val tempFile = Files.createTempFile(tempDir, "temp", "csv")
    try {
      val config = ConfigFactory.parseString(
        s"""
           |{
           | id = src1
           | csv-options = {
           |  header = true
           |  inferSchema = true
           | }
           | path = "${escapedFilePath(tempFile.toString)}"
           |}
         """.stripMargin)
      val dataObj = CsvFileDataObject.fromConfig(config)

      val df = dataObj.getSparkDataFrame()
      df.schema shouldBe empty
      df shouldBe empty
    } finally {
      FileUtils.forceDelete(tempDir.toFile)
    }
  }

  test("Reading from an empty file with header=false and inferSchema=true results in an empty, schema-less data frame.") {
    val tempDir = Files.createTempDirectory("csv")
    val tempFile = Files.createTempFile(tempDir, "temp", "csv")
    try {
      val config = ConfigFactory.parseString(
        s"""
           |{
           | id = src1
           | csv-options = {
           |  header = false
           |  inferSchema = true
           | }
           | path = "${escapedFilePath(tempFile.toString)}"
           |}
         """.stripMargin)
      val dataObj = CsvFileDataObject.fromConfig(config)

      val df = dataObj.getSparkDataFrame()
      df.schema shouldBe empty
      df shouldBe empty
    } finally {
      FileUtils.forceDelete(tempDir.toFile)
    }
  }

  testsFor(readEmptySources(createDataObject(Map("inferSchema" -> "false", "header" -> "false")), ".csv"))

  test("User-defined schema takes precedence over schema inference from header.") {
    val tempDir = Files.createTempDirectory("csv")

    session.createDataFrame(session.sparkContext.makeRDD(Seq(
      Row.fromTuple("A", "B"),
      Row.fromTuple("B", "1")
    )),
      StructType.fromDDL("h1 STRING, h2 STRING"))
      .write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
      .save(tempDir.toFile.getPath)

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
           | path = "${escapedFilePath(tempDir.toFile.getPath)}"
           |}
         """.stripMargin)

      val dataObj = CsvFileDataObject.fromConfig(config)

      val df = dataObj.getSparkDataFrame()

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
      FileUtils.forceDelete(tempDir.toFile)
    }
  }

  test("User-defined schema takes precedence over schema inference.") {
    val tempDir = Files.createTempDirectory("csv")

    session.createDataFrame(session.sparkContext.makeRDD(Seq(
      Row.fromTuple("A", "B"),
      Row.fromTuple("B", "1")
    )),
      StructType.fromDDL("h1 STRING, h2 STRING"))
      .write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
      .save(tempDir.toFile.getPath)

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
           | path = "${escapedFilePath(tempDir.toFile.getPath)}"
           |}
         """.stripMargin)

      val dataObj = CsvFileDataObject.fromConfig(config)

      val df = dataObj.getSparkDataFrame()

      df.schema should contain theSameElementsInOrderAs Seq(
        StructField("header1", StringType, nullable = true),
        StructField("header2", IntegerType, nullable = true)
      )
      df.count() shouldBe 2

    } finally {
      FileUtils.forceDelete(tempDir.toFile)
    }
  }

  testsFor(readNonExistingSources(createDataObject(Map("inferSchema" -> "true")), ".csv"))

  testsFor(validateSchemaMinOnWrite(createDataObjectWithSchemaMin(Map("header" -> "false", "inferSchema" -> "true")), fileExtension = ".csv"))
  testsFor(validateSchemaMinOnRead(createDataObjectWithSchemaMin(Map("header" -> "false", "inferSchema" -> "true")), fileExtension = ".csv"))

  test("Writing file with numberOfTasksPerPartition=1 results in 1 file written, incl. rename") {
    val tempDir = Files.createTempDirectory("csv")

    val dfInit = (1 to 1000).map( i => ("test", i)).toDF("name", "cnt")
      .repartition(10)
    val tgtDO = CsvFileDataObject(id="test1", path=escapedFilePath(tempDir.toFile.getPath), sparkRepartition=Some(SparkRepartitionDef(numberOfTasksPerPartition=1, filename=Some("data.csv"))))
    tgtDO.writeSparkDataFrame(dfInit, Seq())
    val resultFileRefs = tgtDO.getFileRefs(Seq())
    resultFileRefs.map(_.fileName).sorted shouldBe Seq("data.csv")
  }

  test("Writing file with numberOfTasksPerPartition=5 results in 5 files written, incl. rename") {
    val tempDir = Files.createTempDirectory("csv")

    val dfInit = (1 to 1000).map( i => ("test", i)).toDF("name", "cnt")
      .repartition(10)
    val tgtDO = CsvFileDataObject(id="test1", path=escapedFilePath(tempDir.toFile.getPath), sparkRepartition=Some(SparkRepartitionDef(numberOfTasksPerPartition=5, filename=Some("data.csv"))))
    tgtDO.writeSparkDataFrame(dfInit, Seq())
    val resultFileRefs = tgtDO.getFileRefs(Seq())
    resultFileRefs.map(_.fileName).sorted shouldBe Seq("data.1.csv","data.2.csv","data.3.csv","data.4.csv","data.5.csv")
  }


  test("Writing file with numberOfTasksPerPartition=2 and partitions results in 2 files written") {
    val tempDir = Files.createTempDirectory("csv")

    val dfInit = (1 to 1000).map( i => ("test"+Random.nextInt(2), i)).toDF("name", "cnt")
      .repartition(10)
    val tgtDO = CsvFileDataObject(id="test1", path=escapedFilePath(tempDir.toFile.getPath), partitions = Seq("name"), sparkRepartition=Some(SparkRepartitionDef(numberOfTasksPerPartition=1, keyCols = Seq("name"), filename = Some("data.csv"))))
    tgtDO.writeSparkDataFrame(dfInit, Seq(PartitionValues(Map("name"->"test0")), PartitionValues(Map("name"->"test1"))))
    val resultFileRefs = tgtDO.getFileRefs(Seq())
    resultFileRefs.map(_.fileName).sorted shouldBe Seq("data.csv","data.csv")
  }

  test("Writing and reading zip file") {
    val tempDir = Files.createTempDirectory("csv")
    val testFilename = "data.csv.zip"
    val df = Seq(("A", "B"), ("B", "1")).toDF("a", "b")

    // write
    val dataObject = CsvFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), csvOptions = Map("compression" -> classOf[ZipCsvCodec].getName), sparkRepartition=Some(SparkRepartitionDef(numberOfTasksPerPartition=1, filename = Some(testFilename))))
    dataObject.writeSparkDataFrame(df)

    // verify file
    val zipInputStream = new ZipInputStream(new FileInputStream(tempDir.resolve(testFilename).toFile))
    assert(zipInputStream.getNextEntry != null)
    val readString = new String(IOUtils.toByteArray(zipInputStream))
    zipInputStream.close()

    // reading with custom codec is not implemented in Spark for now.
    /**
    val dfRead = dataObject.getSparkDataFrame()
    val result = df.isEqual(dfRead)
    if (!result) TestUtil.printFailedTestResult("")(dfRead)(df)
    assert(result)
    **/
  }

  test("rename file, handle already existing") {
    val tempDir = Files.createTempDirectory("csv")
    val resourceFile = "AB_NYC_2019.csv"

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(resourceFile).toFile)

    // setup DataObject
    val csvDO = CsvFileDataObject( "src1", path = escapedFilePath(tempDir.toFile.getPath))
    val fileRefs = csvDO.getFileRefs(Seq())
    assert(fileRefs.map(_.fileName) == Seq(resourceFile))

    // rename 1
    csvDO.renameFileHandleAlreadyExisting(
      tempDir.resolve(resourceFile).toString.replace('\\','/'),
      tempDir.resolve(resourceFile+".temp").toString.replace('\\','/')
    )
    val fileRefs1 = csvDO.getFileRefs(Seq())
    assert(fileRefs1.map(_.fileName) == Seq(resourceFile+".temp"))

    // copy data file again to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(resourceFile).toFile)

    // rename 2 -> handle already existing
    csvDO.renameFileHandleAlreadyExisting(
      tempDir.resolve(resourceFile).toString.replace('\\','/'),
      tempDir.resolve(resourceFile+".temp").toString.replace('\\','/')
    )
    val fileRefs2 = csvDO.getFileRefs(Seq())
    assert(fileRefs2.size == 2 && fileRefs2.map(_.fileName).forall(_.startsWith(resourceFile+".temp")))
  }


  def createDataObject(options: Map[String, String])(path: String, schemaOpt: Option[StructType]): CsvFileDataObject = {
    val dataObj = CsvFileDataObject(id = "schemaTestCsvDO", path = path, schema = schemaOpt.map(SparkSchema), csvOptions = options)
    instanceRegistry.register(dataObj)
    dataObj
  }

  def createDataObjectWithSchemaMin(options: Map[String, String])(path: String, schemaOpt: Option[StructType], schemaMinOpt: Option[StructType]): CsvFileDataObject = {
    val dataObj = CsvFileDataObject(id = "schemaTestCsvDO", path = path, schema = schemaOpt.map(SparkSchema), schemaMin = schemaMinOpt.map(SparkSchema), csvOptions = options)
    instanceRegistry.register(dataObj)
    dataObj
  }

  override def createFile(path: String, data: DataFrame): Unit = {
    data.write.format("com.databricks.spark.csv").save(path)
  }
}
