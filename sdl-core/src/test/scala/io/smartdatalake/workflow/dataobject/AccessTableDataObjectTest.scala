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
import java.nio.file.Paths
import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row}


class AccessTableDataObjectTest extends DataObjectTestSuite {

  private val access2016dbPath = Paths.get(this.getClass.getResource("/test_database.accdb").toURI).toAbsolutePath.toString
  private val access2016SampleConfig = ConfigFactory.parseString(
    s"""
       |{
       | id = src1
       | path = "${escapedFilePath(access2016dbPath)}"
       | table = { db = test, name = "tableNameOne" }
       |}
         """.stripMargin)

  test("it should be possible to read a table from an access 2016 database") {
    // prepare
    val dataObj = AccessTableDataObject.fromConfig(access2016SampleConfig, instanceRegistry)

    // run
    val df: DataFrame = dataObj.getDataFrame()

    // check
    val result: Array[Row] = df.collect()

    result.length shouldEqual 1
    val firstRow = result.head
    firstRow.getAs[Int]("ID") shouldEqual 2
    firstRow.getAs[String]("field2") shouldEqual "test"
    firstRow.getAs[Int]("field1") shouldEqual 10000
    firstRow.getAs[Boolean]("field3") shouldEqual true
    firstRow.getAs[Long]("field4") shouldEqual 8888888888888888888L
    firstRow.getAs[Timestamp]("field5").toString shouldEqual "2018-05-24 00:00:00.0"
  }

  test("it should be possible to read a table from an access 2000 database") {
    // prepare
    val access2000dbPath = Paths.get(this.getClass.getResource("/test_database.mdb").toURI).toAbsolutePath.toString
    val access2000SampleConfig = ConfigFactory.parseString(
      s"""
         |{
         | id = src1
         | path = "${escapedFilePath(access2000dbPath)}"
         | table = { db = test, name = "tableName" }
         |}
         """.stripMargin)
    val dataObj = AccessTableDataObject.fromConfig(access2000SampleConfig, instanceRegistry)

    // run
    val df = dataObj.getDataFrame()

    // check
    val result = df.collect()
    result.length shouldEqual 1
    val firstRow = result.head
    firstRow.getAs[Int]("ID") shouldEqual 1
    firstRow.getAs[Int]("field1") shouldEqual 1111
    firstRow.getAs[String]("field2") shouldEqual "test"
    firstRow.getAs[Int]("field3") shouldEqual 9999
    firstRow.getAs[Boolean]("field4") shouldEqual true
    firstRow.getAs[Timestamp]("field5").toString shouldEqual "2018-05-24 00:00:00.0"
  }

  test("Reading an access 2016 database using the Ucanaccess driver directly throws an exception") {
    // prepare
    val dataObj = AccessTableDataObject.fromConfig(access2016SampleConfig, instanceRegistry)
    val executorLogLevel = Logger.getLogger("org.apache.spark.executor.Executor").getLevel
    val taskSetManagerLogLevel = Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").getLevel

    try {
      Logger.getLogger("org.apache.spark.executor.Executor").setLevel(Level.OFF)
      Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.OFF)

        // run
        val thrown = intercept[Exception] {
          val df = dataObj.getDataFrameByFramework(doPersist = false)
          df.collect()
        }

        // check
        thrown.getCause.getMessage shouldEqual "UCAExc:::4.0.4 incompatible data type in conversion: from SQL type CHARACTER to java.lang.Integer, value: ID"
    } finally {
        Logger.getLogger("org.apache.spark.executor.Executor").setLevel(executorLogLevel)
        Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(taskSetManagerLogLevel)
    }
  }

  test("It is not possible to read from an non-existing database file.") {

    // prepare
    val tempFile = File.createTempFile("temp", "accdb")
    val path = tempFile.getPath
    FileUtils.forceDelete(tempFile)
      val config = ConfigFactory.parseString(
        s"""
           |{
           | id = src1
           | path = "${escapedFilePath(path)}"
           | table = { db = test, name = "tableName" }
           |}
         """.stripMargin)
      val actionInput = AccessTableDataObject.fromConfig(config, instanceRegistry)

      // run
      an [Exception] should be thrownBy actionInput.getDataFrame()
  }

}
