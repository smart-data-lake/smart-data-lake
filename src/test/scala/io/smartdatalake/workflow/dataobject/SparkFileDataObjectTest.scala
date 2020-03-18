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

class SparkFileDataObjectTest extends DataObjectTestSuite {

  test("read partitioned data") {
    import testSession.implicits._

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, partitions = Seq("p"), csvOptions = Map("header" -> "true") )

    // write test data - create partition A and B
    val partitionValuesCreated = Seq( PartitionValues(Map("p"->"A")), PartitionValues(Map("p"->"B")))
    val df1 = Seq(("A",1),("A",2),("B",3),("B",4)).toDF("p", "value")
    dataObject.writeDataFrame(df1, partitionValuesCreated )

    // test
    dataObject.getDataFrame().count shouldEqual 4 // four records in total, 2 from partition A and 2 from partition B
    dataObject.getDataFrame(Seq(PartitionValues(Map("p"->"B")))).count shouldEqual 2 // two records in partition B
    dataObject.getDataFrame(Seq(PartitionValues(Map("p"->"A")),PartitionValues(Map("p"->"A","p"->"B")))).count shouldEqual 4

    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
