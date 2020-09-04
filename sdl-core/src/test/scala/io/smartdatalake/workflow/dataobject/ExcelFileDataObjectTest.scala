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

import java.io.{File, FileOutputStream}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel
import org.apache.poi.ss.usermodel.{DateUtil, Sheet, Workbook}
import org.apache.poi.ss.util.DateFormatConverter
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterAll

import scala.reflect.runtime.universe._

/**
 * Unit tests for [[ExcelFileDataObject]].
 */
class ExcelFileDataObjectTest extends DataObjectTestSuite with BeforeAndAfterAll with SparkFileDataObjectSchemaBehavior {

  private val XslSuffix = ".xsl"
  private val XslxSuffix = ".xslx"

  private var xslTempFilePath: String = _
  private var xslxTempFilePath: String = _

  private final val sampleDate: Date = new Date(0L)

  private final val javaDateFormat: String = "yyyy-MM-dd HH:mm:ss.S"
  private final lazy val sampleDateFormatted : String = new SimpleDateFormat(javaDateFormat).format(sampleDate)

  private lazy val xslxSampleConfig = ConfigFactory.parseString(
    s"""
       |{
       | id = src1
       | type = excel
       | path = "${escapedFilePath(xslxTempFilePath)}"
       | excel-options {
       |   sheet-name = "sheet number 1"
       |   num-header-lines = 1
       |   num-lines-to-skip = 0
       |   start-column = 1
       |   end-column = 5
       | }
       |}
         """.stripMargin)

  test("reading an XSSF excel sheet with a date should yield a field of type date") {
    // prepare
    val actionInputExcel = ExcelFileDataObject.fromConfig(xslxSampleConfig, instanceRegistry)

    // run
    val df = actionInputExcel.getDataFrame()

    // check
    val data = df.collect().toList
    data should have size 3
    data.head.getAs[Int]("a_a") shouldEqual 42
    data.head.getAs[Boolean]("bb") shouldEqual true
    data.head.getAs[Double]("ccc_cd_c_dcc") shouldEqual DateUtil.getExcelDate(sampleDate)
    data.head.getAs[Timestamp]("dd").toString shouldEqual sampleDateFormatted
    data.head.getAs[String]("e") shouldEqual "Lorem Ipsum"
    data.head shouldEqual data.tail.head
  }

  test("reading an HSSF excel sheet with skipped/limited rows should only return the wanted rows") {
    // prepare
    val testConfig = ConfigFactory.parseString(
      s"""
         |{
         | id = src1
         | type = excel
         | path = "${escapedFilePath(xslTempFilePath)}"
         | excel-options {
         |   sheet-name = "sheet number 1"
         |   num-header-lines = 1
         |   num-lines-to-skip = 1
         |   start-column = 1
         |   end-column = 5
         |   row-limit = 1
         | }
         |}
         """.stripMargin)

    val actionInputExcel = ExcelFileDataObject.fromConfig(testConfig, instanceRegistry)

    // run
    val df = actionInputExcel.getDataFrame()

    // check
    val data: Array[Row] = df.collect()
    data should have size 1
    val datum: Row = data.head
    datum.getAs[Int]("a_a") shouldEqual 42
    datum.getAs[Boolean]("bb") shouldEqual true
    datum.getAs[Double]("ccc_cd_c_dcc") shouldEqual DateUtil.getExcelDate(sampleDate)
    datum.getAs[Timestamp]("dd").toString shouldEqual sampleDateFormatted
    datum.getAs[String]("e") shouldEqual "Lorem Ipsum"
  }

  testsFor(readNonExistingSources(createDataObject(ExcelOptions(sheetName = "testSheet")), ".xslx"))
  testsFor(readEmptySources(createDataObject(ExcelOptions(sheetName = "testSheet", useHeader = false)), ".xslx"))
  testsFor(validateSchemaMinOnWrite(createDataObjectWithSchemaMin(ExcelOptions(sheetName = "testSheet", useHeader = false)), ".xslx"))
  testsFor(validateSchemaMinOnRead(createDataObjectWithSchemaMin(ExcelOptions(sheetName = "testSheet", useHeader = false)), ".xslx"))

  override def beforeAll() {
    xslxTempFilePath = createTempFile(createXSSFWorkbook, XslxSuffix)
    xslTempFilePath = createTempFile(createHSSFWorkbook, XslSuffix)
  }

  override def afterAll(): Unit = {
    FileUtils.forceDelete(new File(xslTempFilePath))
    FileUtils.forceDelete(new File(xslxTempFilePath))
  }

  private def createTempFile(workbook: Workbook, suffix: String): String = {
    val tempFile = File.createTempFile("tmp_bd-util-etl_ActionInputExcelTest", suffix)
    tempFile.deleteOnExit()
    val tempOutputStream = new FileOutputStream(tempFile)
    try {
      workbook.write(tempOutputStream)
    } finally {
      IOUtils.closeQuietly(tempOutputStream)
    }
    tempFile.getPath
  }

  private def createHeaderRow(sheet: Sheet): Unit = {
    sheet.createRow(0)
    createValue(sheet, 0, 0, Some("A A"))
    createValue(sheet, 0, 1, Some("Bb"))
    createValue(sheet, 0, 2, Some("CCC -cd c - dcc"))
    createValue(sheet, 0, 3, Some("DD"))
    createValue(sheet, 0, 4, Some("E"))
  }

  private def createRow(rowNum: Int, sheet: usermodel.Sheet, workbook: Workbook): Unit = {
    val row = sheet.createRow(rowNum)
    createValue(sheet, rowNum, 0, Some(42))
    createValue(sheet, rowNum, 1, Some(true))
    createValue(sheet, rowNum, 2, Some(sampleDate))
    val cell3 = row.createCell(3)
    val cellStyle = workbook.createCellStyle
    cellStyle.setDataFormat(workbook.getCreationHelper.createDataFormat().getFormat(
        DateFormatConverter.convert(Locale.getDefault, javaDateFormat)))
    cell3.setCellStyle(cellStyle)
    cell3.setCellValue(sampleDate)
    createValue(sheet, rowNum, 4, Some("Lorem Ipsum"))
  }

  private def createValue[A: TypeTag](sheet: usermodel.Sheet, rowIdx: Int, cellIdx: Int, value: Option[A]): Unit = {
    if (value.isDefined) {
      val cell = sheet.getRow(rowIdx).createCell(cellIdx)
      typeOf[A] match {
        case tpe if tpe <:< typeOf[String] => cell.setCellValue(value.get.asInstanceOf[String])
        case tpe if tpe <:< typeOf[Int] => cell.setCellValue(value.get.asInstanceOf[Int])
        case tpe if tpe <:< typeOf[Float] => cell.setCellValue(value.get.asInstanceOf[Float])
        case tpe if tpe <:< typeOf[Double] => cell.setCellValue(value.get.asInstanceOf[Double])
        case tpe if tpe <:< typeOf[Boolean] => cell.setCellValue(value.get.asInstanceOf[Boolean])
        case tpe if tpe <:< typeOf[Date] => cell.setCellValue(value.get.asInstanceOf[Date])
      }
    }
  }

  private def createXSSFWorkbook: XSSFWorkbook = {
    val workbook = new XSSFWorkbook
    val sheet = workbook.createSheet("sheet number 1")
    createHeaderRow(sheet)
    createRow(1, sheet, workbook)
    createRow(2, sheet, workbook)
    createRow(3, sheet, workbook)
    workbook
  }

  private def createHSSFWorkbook: HSSFWorkbook = {
    val workbook = new HSSFWorkbook()
    val sheet = workbook.createSheet("sheet number 1")
    createHeaderRow(sheet)
    createRow(1, sheet, workbook)
    createRow(2, sheet, workbook)
    workbook
  }

  private def createDataObject(options: ExcelOptions)(path: String, schemaOpt: Option[StructType]): ExcelFileDataObject = {
    val dataObj = ExcelFileDataObject(id = "schemaTestExcelDO", path = path, schema = schemaOpt, excelOptions = options)
    instanceRegistry.register(dataObj)
    dataObj
  }

  private def createDataObjectWithSchemaMin(options: ExcelOptions)(path: String, schemaOpt: Option[StructType], schemaMinOpt: Option[StructType]): ExcelFileDataObject = {
    val dataObj = ExcelFileDataObject(id = "schemaTestExcelDO", path = path, schema = schemaOpt, schemaMin = schemaMinOpt, excelOptions = options)
    instanceRegistry.register(dataObj)
    dataObj
  }

  override def createFile(path: String, data: DataFrame): Unit = {
    data.write.options(Map("useHeader" -> "false", "sheetName" -> "testSheet"))
      .format("com.crealytics.spark.excel").save(path)
  }
}
