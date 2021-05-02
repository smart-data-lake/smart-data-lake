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

import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.testutils.{DataObjectTestSuite, TestUtil}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ProcessingLogicException
import io.smartdatalake.workflow.action.CustomFileActionTest
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import java.nio.file
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.util.Try

class SparkFileDataObjectTest extends DataObjectTestSuite with SmartDataLakeLogger {
  import session.implicits._

  test("overwrite only one partition") {

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, partitions = Seq("p"), csvOptions = Map("header" -> "true") )

    // write test data 1 - create partition A and B
    val partitionValuesCreated1 = Seq( PartitionValues(Map("p"->"A")), PartitionValues(Map("p"->"B")))
    val df1 = Seq(("A",1),("A",2),("B",3),("B",4)).toDF("p", "value")
    dataObject.writeDataFrame(df1, partitionValuesCreated1 )

    // test 1
    dataObject.getDataFrame().count shouldEqual 4 // four records should remain, 2 from partition A and 2 from partition B
    partitionValuesCreated1.toSet shouldEqual dataObject.listPartitions.toSet

    // write test data 2 - overwrite partition B
    val partitionValuesCreated2 = Seq(PartitionValues(Map("p"->"B")))
    val df2 = Seq(("B",5)).toDF("p", "value")
    dataObject.writeDataFrame(df2, partitionValuesCreated2 )

    // test 2
    dataObject.getDataFrame().count shouldEqual 3 // three records should remain, 2 from partition A and 1 from partition B
    partitionValuesCreated1.toSet shouldEqual dataObject.listPartitions.toSet

    Try(FileUtils.deleteDirectory(tempDir.toFile))
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

    Try(FileUtils.deleteDirectory(tempDir.toFile))
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

    Try(FileUtils.deleteDirectory(tempDir.toFile))
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

  test("read partitioned data and filter expected partitions") {

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

  test("overwrite partitioned data") {

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, partitions = Seq("p"), csvOptions = Map("header" -> "true"))

    // write test data - create partition A, B, C
    val partitionValuesCreated1 = Seq( PartitionValues(Map("p"->"A")), PartitionValues(Map("p"->"B")))
    val df1 = Seq(("A",1),("A",2),("B",3),("B",4),("C",5),("C",6)).toDF("p", "value")
    dataObject.writeDataFrame(df1, partitionValuesCreated1)

    // overwrite partition B with new data, overwrite partition C with no data
    val partitionValuesCreated2 = Seq( PartitionValues(Map("p"->"B")), PartitionValues(Map("p"->"C")))
    val df2 = Seq(("B",7),("B",8)).toDF("p", "value")
    dataObject.writeDataFrame(df2, partitionValuesCreated2)

    // test reading data
    val result = dataObject.getDataFrame()
      .select($"p",$"value".cast("int"))
      .as[(String,Int)].collect.toSeq.sorted
    assert( result == Seq(("A",1),("A",2),("B",7),("B",8)))
    assert( dataObject.listPartitions.map(pv => pv("p").toString).sorted == Seq("A","B","C"))

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("overwrite all") {

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, csvOptions = Map("header" -> "true"))

    // write test data
    val df1 = Seq(("A",1),("A",2)).toDF("p", "value")
    dataObject.writeDataFrame(df1)

    // overwrite with new data
    val df2 = Seq(("B",3),("B",4)).toDF("p", "value")
    dataObject.writeDataFrame(df2)

    // test reading data
    val result = dataObject.getDataFrame()
      .select($"p",$"value".cast("int"))
      .as[(String,Int)].collect.toSeq.sorted
    assert( result == Seq(("B",3),("B",4)))

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("overwrite all empty") {

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, csvOptions = Map("header" -> "true"))

    // write test data
    val df1 = Seq(("A",1),("A",2)).toDF("p", "value")
    dataObject.writeDataFrame(df1)

    // overwrite with no data
    val df2 = Seq[(String,Int)]().toDF("p", "value")
    dataObject.writeDataFrame(df2)

    // test reading data
    assert(dataObject.getDataFrame().isEmpty)

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("overwrite all preserve directory") {

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", path = tempDir.toString, csvOptions = Map("header" -> "true"), saveMode = SDLSaveMode.OverwritePreserveDirectories)

    // write test data
    val df1 = Seq(("A",1),("A",2)).toDF("p", "value")
    dataObject.writeDataFrame(df1)

    // overwrite with new data
    val df2 = Seq(("B",3),("B",4)).toDF("p", "value")
    dataObject.writeDataFrame(df2)

    // test reading data
    val result = dataObject.getDataFrame()
      .select($"p",$"value".cast("int"))
      .as[(String,Int)].collect.toSeq.sorted
    assert( result == Seq(("B",3),("B",4)))

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("append filename") {

    // create data object

    val feed = "filenametest"
    val srcDir = "testSrc"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)
    val sourceFileColName = "_sourcefile"

    // copy data file to test directory
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile).toFile)
    // setup DataObject
    val dataObject = CsvFileDataObject("src1", tempDir.resolve(srcDir).toString.replace('\\', '/'),
      csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter), filenameColumn = Some(sourceFileColName),
      schema = Some(StructType.fromDDL("id string,name string,host_id string,host_name string,neighbourhood_group string,neighbourhood string,latitude string,longitude string,room_type string,price string,minimum_nights string,number_of_reviews string,last_review string,reviews_per_month string,calculated_host_listings_count string,availability_365 string"))
    )

    // test received DataFrame
    val df = dataObject.getDataFrame()
    df.columns.contains(sourceFileColName) //retrieved Dataframe has sourcefile column appended
    df.select(sourceFileColName).collect().head.getAs[String](0).endsWith(resourceFile) //content of sourcefile column corresponds to sourcefile

    // test if it could be written again
    dataObject.init(df.drop(sourceFileColName), Seq())

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("get concrete paths") {
    val tempDir = Files.createTempDirectory("concretePaths")
    tempDir.resolve("a=1").resolve("b=1").resolve("c=1").toFile.mkdirs()
    tempDir.resolve("a=1").resolve("b=1").resolve("c=2").toFile.mkdirs()
    tempDir.resolve("a=1").resolve("b=2").resolve("c=1").toFile.mkdirs()
    tempDir.resolve("a=1").resolve("b=2").resolve("c=2").toFile.mkdirs()
    tempDir.resolve("a=1").resolve("b=3").resolve("c=1").toFile.mkdirs()
    tempDir.resolve("a=1").resolve("b=3").resolve("c=2").toFile.mkdirs()
    tempDir.resolve("a=2").resolve("b=1").resolve("c=1").toFile.mkdirs()
    tempDir.resolve("a=2").resolve("b=1").resolve("c=2").toFile.mkdirs()
    tempDir.resolve("a=2").resolve("b=2").resolve("c=1").toFile.mkdirs()
    tempDir.resolve("a=2").resolve("b=2").resolve("c=2").toFile.mkdirs()
    tempDir.resolve("a=2").resolve("b=3").resolve("c=1").toFile.mkdirs()
    tempDir.resolve("a=2").resolve("b=3").resolve("c=2").toFile.mkdirs()

    val do1 = RawFileDataObject("testDO", tempDir.toString, partitions = Seq("a","b","c"))
    def removeDOBase(p: String) = p.replaceFirst(".*concretePaths.*?/", "")
    def getPaths(pv: PartitionValues) = do1.getConcretePaths(pv).map(p => removeDOBase(p.toString))
    // inits
    assert(getPaths(PartitionValues(Map("a" -> 1))).sorted == Seq("a=1"))
    assert(getPaths(PartitionValues(Map("a" -> 1, "b" -> 1))).sorted == Seq("a=1/b=1"))
    assert(getPaths(PartitionValues(Map("a" -> 1, "b" -> 1, "c" -> 1))).sorted == Seq("a=1/b=1/c=1"))
    // no inits
    assert(getPaths(PartitionValues(Map("b" -> 1))).sorted == Seq("a=1/b=1","a=2/b=1"))
    assert(getPaths(PartitionValues(Map("c" -> 1))).sorted == Seq("a=1/b=1/c=1","a=1/b=2/c=1","a=1/b=3/c=1","a=2/b=1/c=1","a=2/b=2/c=1","a=2/b=3/c=1"))
    assert(getPaths(PartitionValues(Map("b" -> 1, "c" -> 1))).sorted == Seq("a=1/b=1/c=1","a=2/b=1/c=1"))
  }


  test("delete files only") {

    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", partitions = Seq("p"), path = tempDir.toString, csvOptions = Map("header" -> "true"))

    // write test data
    val df1 = Seq(("A",1),("A",2)).toDF("p", "value")
    dataObject.writeDataFrame(df1)

    // delete partition files
    val partitionValues = PartitionValues(Map("p"->"A"))
    val partitionPath = new Path(dataObject.hadoopPath, dataObject.getPartitionString(partitionValues).get)
    assert(dataObject.filesystem.isDirectory(partitionPath))
    assert(dataObject.filesystem.listStatus(partitionPath).nonEmpty)
    dataObject.deletePartitionsFiles(Seq(partitionValues))
    assert(dataObject.filesystem.listStatus(partitionPath).isEmpty)
    assert(dataObject.filesystem.isDirectory(partitionPath))

    // delete files in base dir
    dataObject.filesystem.createNewFile(new Path(dataObject.hadoopPath, "testFile"))
    assert(dataObject.filesystem.listStatus(dataObject.hadoopPath).exists(_.isFile))
    dataObject.deleteAllFiles(dataObject.hadoopPath)
    assert(!dataObject.filesystem.listStatus(dataObject.hadoopPath).exists(_.isFile))
    assert(dataObject.filesystem.isDirectory(dataObject.hadoopPath))
    assert(dataObject.filesystem.isDirectory(new Path(dataObject.hadoopPath,"p=A")))

    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("OverwriteOptimized without partition values not allowed for partitioned DataObject") {
    val df = Seq(("A", "2", 1), ("B", "1", 2), ("C", "X", 3)).toDF("p1", "p2", "value")
    // create data object
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = CsvFileDataObject(id = "partitionTestCsv", partitions = Seq("p1","p2"), path = tempDir.toString, saveMode = SDLSaveMode.OverwriteOptimized)
    a [ProcessingLogicException] should be thrownBy dataObject.writeDataFrame(df, partitionValues = Seq())
  }

  private def createJsonFiles(path: file.Path, nbOfFile: Int = 100, filenamePrefix: String = "test") = {
    Files.createDirectory(path)
    logger.info(s"creating test files in $path")
    (1 to nbOfFile).foreach { i =>
      val writer = Files.newBufferedWriter(path.resolve(s"$filenamePrefix$i.json"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      writer.write(s"""{"value": $i}""")
      writer.close()
    }
    assert(file.Files.list(path).count == nbOfFile)
  }

  // create data for 2 partitions and compact one of them
  test("move partition function") {
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = JsonFileDataObject(id = "movePartitionTestJson", partitions = Seq("p"), path = tempDir.toString, jsonOptions = Some(Map("multiLine"->"false")))

    // create 100 json files for partition p=A and p=B
    val partitionPathA = Paths.get(tempDir.toString, "p=A")
    val partitionPathB = Paths.get(tempDir.toString, "p=B")
    createJsonFiles(partitionPathA, 10, "testA")
    createJsonFiles(partitionPathB, 10, "testB")

    // move partition p=A to p=B
    val pvsToMove = Seq((PartitionValues(Map("p" -> "A")), PartitionValues(Map("p" -> "B"))))
    dataObject.movePartitions(pvsToMove)

    //check
    assert(!file.Files.exists(partitionPathA)) // p=A is deleted
    assert(file.Files.list(partitionPathB).count == 20) // check p=A moved to p=B
    assert(dataObject.getDataFrame(pvsToMove.map(_._2)).select(sum($"value")).as[Long].head == 110) // check completeness of p=A + p=B
  }

  // create data for 2 partitions and compact one of them
  test("compact partition function") {
    val tempDir = Files.createTempDirectory("tempHadoopDO")
    val dataObject = JsonFileDataObject(id = "compactionTestJson", partitions = Seq("p"), path = tempDir.toString, jsonOptions = Some(Map("multiLine"->"false")))

    // create 100 json files for partition p=A and p=B
    val partitionPathA = Paths.get(tempDir.toString, "p=A")
    val partitionPathB = Paths.get(tempDir.toString, "p=B")
    def createFiles(path: file.Path) = {
      Files.createDirectory(path)
      logger.info(s"creating test files in $path")
      (1 to 100).foreach { i =>
        val writer = Files.newBufferedWriter(path.resolve(s"$i.json"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        writer.write(s"""{"value": $i}""")
        writer.close()
      }
      assert(file.Files.list(path).count == 100)
    }
    createFiles(partitionPathA)
    createFiles(partitionPathB)

    // compact
    logger.info("compacting partition p=A")
    val pvsToCompact = Seq(PartitionValues(Map("p" -> "A")))
    dataObject.compactPartitions(pvsToCompact)

    //check
    assert(file.Files.list(partitionPathA).count < 100) // check less files in p=A
    assert(dataObject.getDataFrame(pvsToCompact).select(sum($"value")).as[Long].head == 5050) // check completeness of p=A
    assert(file.Files.list(partitionPathB).count == 100) // check p=B not changed
    val specialFiles = dataObject.filesystem.globStatus(new Path(tempDir.toString, s"*/_SDL*"))
    assert(specialFiles.count(_.getPath.getName.endsWith("COMPACTED")) == 1) // one partition marked as compacted
    assert(specialFiles.count(!_.getPath.getName.endsWith("COMPACTED")) == 0) // no other special files left

    // compact 2 - dont compact p=A again
    logger.info("compacting partition p=A 2nd time")
    val compactedTstmp = specialFiles.find(_.getPath.getName.endsWith("COMPACTED")).get.getModificationTime
    dataObject.compactPartitions(pvsToCompact)
    val specialFiles2 = dataObject.filesystem.globStatus(new Path(tempDir.toString, s"*/_SDL*"))
    val compactedTstmp2 = specialFiles.find(_.getPath.getName.endsWith("COMPACTED")).get.getModificationTime
    assert(compactedTstmp == compactedTstmp2)
  }

}
