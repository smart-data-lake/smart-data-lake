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

import io.smartdatalake.testutils.{DataObjectTestSuite, TestUtil}
import io.smartdatalake.util.hdfs.PartitionValues
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Path}

/**
 * Unit tests for [[RawFileDataObject]].
 */
class RawFileDataObjectTest extends DataObjectTestSuite with BeforeAndAfterEach {

  var tempDir: Path = _
  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("raw")
  }

  override def afterEach(): Unit = {
    FileUtils.deleteDirectory(tempDir.toFile)
    tempDir = null
  }

  test("Schema is fixed if format=text") {
    val dataObj = RawFileDataObject(id = "schemaTestCsvDO", path = tempDir.toString, customFormat = Some("text"), filenameColumn = Some("_filename"))
    assert(dataObj.getSparkDataFrame().columns.toSet == Set("value","_filename"))
  }

  test("Schema is fixed if format=binaryFile") {
    val dataObj = RawFileDataObject(id = "schemaTestCsvDO", path = tempDir.toString, customFormat = Some("binaryFile"), partitions = Seq("a","b") )
    assert(dataObj.getSparkDataFrame().columns.toSet == Set("path","modificationTime","length","content", "a", "b"))
  }

  test("initialize") {
    // no partition
    RawFileDataObject( "src1", "test")

    // partitions with standard layout
    RawFileDataObject( "src1", "test", partitions = Seq("test"))

    // layout without partitions
    intercept[IllegalArgumentException](RawFileDataObject( "src1", "test", customPartitionLayout = Some("%test%")))

    // layout with incomplete partitions
    intercept[IllegalArgumentException](RawFileDataObject( "src1", "test", partitions = Seq("test1"), customPartitionLayout = Some("%test%")))

    // with partitions
    RawFileDataObject( "src1", "test", partitions = Seq("test"), customPartitionLayout = Some("%test%"))

    // with multiple partitions
    RawFileDataObject( "src1", "test", partitions = Seq("test1", "test2"), customPartitionLayout = Some("%test1%/abc/%test2%/def"))
  }

  test("get FileRef's with partitions in filename") {

    val resourceFile = "AB_NYC_2019.csv"

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(resourceFile).toFile)

    // setup DataObject
    val rawDO = RawFileDataObject( "src1"
      , tempDir.toString.replace('\\','/')
      , partitions = Seq("town", "year")
      , customPartitionLayout = Some("AB_%town%_%year:[0-9]+%" ))
    val partitionValuesExpected = Seq(PartitionValues(Map("town" -> "NYC", "year" -> "2019")))

    // list all files and extract partitions
    val fileRefsAll = rawDO.getFileRefs(Seq())
    assert(fileRefsAll.size == 1)
    assert(fileRefsAll.head.fileName == resourceFile)
    assert(fileRefsAll.head.partitionValues == partitionValuesExpected.head)

    // list with matched partition filter
    val fileRefsPartitionFilter = rawDO.getFileRefs(partitionValuesExpected)
    assert(fileRefsPartitionFilter.size == 1)
    assert(fileRefsPartitionFilter.head.fileName == resourceFile)

    // list with unmatched partition filter
    val fileRefsPartitionNoMatchFilter = rawDO.getFileRefs(Seq(PartitionValues(Map("town" -> "NYC", "year" -> "2020"))))
    assert(fileRefsPartitionNoMatchFilter.isEmpty)

    // check list partition values
    val partitionValuesListed = rawDO.listPartitions
    partitionValuesListed shouldEqual partitionValuesListed
  }

  test("get FileRef's with partitions as directories") {

    val partitionDir = "20190101"
    val resourceFile = "AB_NYC_2019.csv"

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(partitionDir).resolve(resourceFile).toFile)

    // setup DataObject
    val rawDO = RawFileDataObject( "src1"
      , tempDir.resolve(tempDir).toString.replace('\\','/')
      , partitions = Seq("date", "town", "year")
      , customPartitionLayout = Some("%date%/AB_%town%_%year:[0-9]+%" ))
    val partitionValuesExpected = Seq(PartitionValues(Map("date" -> "20190101", "town" -> "NYC", "year" -> "2019")))

    // list all files and extract partitions
    val fileRefsAll = rawDO.getFileRefs(Seq())
    assert(fileRefsAll.size == 1)
    assert(fileRefsAll.head.fileName == resourceFile)
    assert(fileRefsAll.head.partitionValues == partitionValuesExpected.head)

    // list with matched partition filter
    val fileRefsPartitionFilter = rawDO.getFileRefs(partitionValuesExpected)
    assert(fileRefsPartitionFilter.size == 1)
    assert(fileRefsPartitionFilter.head.fileName == resourceFile)

    // list with unmatched partition filter
    val fileRefsPartitionNoMatchFilter = rawDO.getFileRefs(Seq(PartitionValues(Map("date" -> "20190101", "town" -> "NYC", "year" -> "2020"))))
    assert(fileRefsPartitionNoMatchFilter.isEmpty)

    // check list partition values
    val partitionValuesListed = rawDO.listPartitions
    partitionValuesListed shouldEqual partitionValuesListed
  }

}
