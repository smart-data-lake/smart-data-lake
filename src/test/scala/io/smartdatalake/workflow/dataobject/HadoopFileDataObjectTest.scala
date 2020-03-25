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

import java.nio.file.Files

import io.smartdatalake.util.hdfs.PartitionValues
import org.apache.commons.io.FileUtils

/**
 * Unit tests for [[HadoopFileDataObjectTest]].
 */
class HadoopFileDataObjectTest extends DataObjectTestSuite {
  import testSession.implicits._

  test("overwrite only one partition") {

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, partitions = Seq("p"), csvOptions = Map("header" -> "true") )

    // write test data 1 - create partition A and B
    val partitionValuesCreated = Seq( PartitionValues(Map("p"->"A")), PartitionValues(Map("p"->"B")))
    val df1 = Seq(("A",1),("A",2),("B",3),("B",4)).toDF("p", "value")
    dataObject.writeDataFrame(df1, partitionValuesCreated )

    // test 1
    dataObject.getDataFrame().count shouldEqual 4 // four records should remain, 2 from partition A and 2 from partition B
    partitionValuesCreated.toSet shouldEqual dataObject.listPartitions.toSet

    // write test data 2 - overwrite partition B
    val df2 = Seq(("B",5)).toDF("p", "value")
    dataObject.writeDataFrame(df2, partitionValuesCreated )

    // test 2
    dataObject.getDataFrame().count shouldEqual 3 // three records should remain, 2 from partition A and 1 from partition B
    partitionValuesCreated.toSet shouldEqual dataObject.listPartitions.toSet

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("create and list partition one level") {

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, partitions = Seq("p"), csvOptions = Map("header" -> "true") )

    // write test files
    val partitionValuesCreated = Seq(PartitionValues(Map("p"->"A")), PartitionValues(Map("p"->"B")))
    val df = Seq(("A",1),("B",2)).toDF("p", "value")
    dataObject.writeDataFrame(df, partitionValuesCreated )

    val partitionValuesListed = dataObject.listPartitions
    partitionValuesCreated.toSet shouldEqual partitionValuesListed.toSet

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("create and list partition multi level") {

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, partitions = Seq("p1","p2"), csvOptions = Map("header" -> "true"))

    // write test files
    val partitionValuesCreated = Seq( PartitionValues(Map("p1"->"A","p2"->"L2A")), PartitionValues(Map("p1"->"A","p2"->"L2B"))
                                    , PartitionValues(Map("p1"->"B","p2"->"L2B")), PartitionValues(Map("p1"->"B","p2"->"L2C")))
    val df = Seq(("A","L2A",1),("A","L2B",2),("B","L2B",3),("B","L2C",4)).toDF("p1", "p2", "value")
    dataObject.writeDataFrame(df, partitionValuesCreated)

    val partitionValuesListed = dataObject.listPartitions
    partitionValuesCreated.toSet shouldEqual partitionValuesListed.toSet

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("create empty partition") {

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, partitions = Seq("p1","p2"), csvOptions = Map("header" -> "true"))

    // write test files
    val partitionValuesCreated = Seq( PartitionValues(Map("p1"->"A","p2"->"L2A")), PartitionValues(Map("p1"->"X","p2"->"L2X")))
    val df = Seq(("A","L2A",1)).toDF("p1", "p2", "value")
    dataObject.writeDataFrame(df, partitionValuesCreated)

    val partitionValuesListed = dataObject.listPartitions
    partitionValuesCreated.toSet shouldEqual partitionValuesListed.toSet

    Try(FileUtils.deleteDirectory(tempDir.toFile))
  }
}
