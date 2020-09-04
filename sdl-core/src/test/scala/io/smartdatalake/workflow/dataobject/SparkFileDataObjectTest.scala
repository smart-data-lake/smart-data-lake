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

import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.CustomFileActionTest
import org.apache.commons.io.FileUtils

class SparkFileDataObjectTest extends DataObjectTestSuite {

  test("read partitioned data and filter expected partitions") {
    import testSession.implicits._

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, partitions = Seq("p"), csvOptions = Map("header" -> "true"), expectedPartitionsCondition = Some("elements['p'] != 'A'"))

    // write test data - create partition A and B
    val partitionValuesCreated = Seq( PartitionValues(Map("p"->"A")), PartitionValues(Map("p"->"B")))
    val df1 = Seq(("A",1),("A",2),("B",3),("B",4)).toDF("p", "value")
    dataObject.writeDataFrame(df1, partitionValuesCreated )

    // test reading data
    dataObject.getDataFrame().count shouldEqual 4 // four records in total, 2 from partition A and 2 from partition B
    dataObject.getDataFrame(Seq(PartitionValues(Map("p"->"B")))).count shouldEqual 2 // two records in partition B
    dataObject.getDataFrame(Seq(PartitionValues(Map("p"->"A")),PartitionValues(Map("p"->"A","p"->"B")))).count shouldEqual 4

    // test expected partitions
    assert( dataObject.filterExpectedPartitionValues(partitionValuesCreated) == Seq(PartitionValues(Map("p"->"B"))))

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("append filename") {
    import testSession.implicits._

    // create data object

    val feed = "filenametest"
    val srcDir = "testSrc"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)
    val sourceFileColName = "sourcefile"

    // copy data file to test directory
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile).toFile)
    // setup DataObject
    val dataObject = CsvFileDataObject("src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter), filenameColumn = Some(sourceFileColName))

    // test
    val df = dataObject.getDataFrame()
    df.columns.contains(sourceFileColName) //retrieved Dataframe has sourcefile column appended
    df.select(sourceFileColName).collect().head.getAs[String](0).endsWith(resourceFile) //content of sourcefile column corresponds to sourcefile

    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
