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

import java.time.LocalDateTime

import com.typesafe.config.ConfigFactory
import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.workflow.ActionPipelineContext

class HiveTableDataObjectTest extends DataObjectTestSuite {

  private val tempDir = createTempDir
  private val tempPath = tempDir.toAbsolutePath.toString

  import session.implicits._

  test("write and analyze table without partitions") {
    val srcTable = Table(Some("default"), "input")
    val srcDO = HiveTableDataObject( "input", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1, analyzeTableAfterWrite = true)
    srcDO.dropTable
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq())
    // check table statistics
    val statsStr = session.sql(s"describe extended ${srcTable.fullName}")
      .where($"col_name"==="Statistics").head.getAs[String](1)
    assert(statsStr.contains("3 rows"))
    // check table contents
    assert(srcDO.getDataFrame().count==3)
  }

  test("write and analyze table with partitions and partition values") {
    val srcTable = Table(Some("default"), "input")
    val srcDO = HiveTableDataObject( "input", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("type"), numInitialHdfsPartitions = 1, analyzeTableAfterWrite = true)
    srcDO.dropTable
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq(PartitionValues(Map("type"->"ext")),PartitionValues(Map("type"->"int"))))
    // check table statistics
    val statsStr = session.sql(s"describe extended ${srcTable.fullName}")
      .where($"col_name"==="Statistics").head.getAs[String](1)
    assert(statsStr.contains("3 rows"))
    // check partition statistics
    val statsPart1Str = session.sql(s"describe extended ${srcTable.fullName} partition(type='ext')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart1Str.contains("2 rows"))
    val statsPart2Str = session.sql(s"describe extended ${srcTable.fullName} partition(type='int')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart2Str.contains("1 rows"))
    // check table contents
    assert(srcDO.getDataFrame().count==3)
  }

  test("write and analyze table with partitions without partition values") {
    val srcTable = Table(Some("default"), "input")
    val srcDO = HiveTableDataObject( "input", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("type"), numInitialHdfsPartitions = 1, analyzeTableAfterWrite = true)
    srcDO.dropTable
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq())
    // check table statistics
    val statsStr = session.sql(s"describe extended ${srcTable.fullName}")
      .where($"col_name"==="Statistics").head.getAs[String](1)
    assert(statsStr.contains("3 rows"))
    // check partition statistics
    val statsPart1Str = session.sql(s"describe extended ${srcTable.fullName} partition(type='ext')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart1Str.contains("2 rows"))
    val statsPart2Str = session.sql(s"describe extended ${srcTable.fullName} partition(type='int')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart2Str.contains("1 rows"))
    // check table contents
    assert(srcDO.getDataFrame().count==3)
  }

  test("write and analyze table with multi partition layout and partial partition values") {
    val srcTable = Table(Some("default"), "input")
    val srcDO = HiveTableDataObject( "input", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("type","lastname"), numInitialHdfsPartitions = 1, analyzeTableAfterWrite = true)
    srcDO.dropTable
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq(PartitionValues(Map("type"->"ext"))))
    // check table statistics
    val statsStr = session.sql(s"describe extended ${srcTable.fullName}")
      .where($"col_name"==="Statistics").head.getAs[String](1)
    assert(statsStr.contains("3 rows"))
    // check partition statistics -> only partition type=ext,name=doe and type=ext,lastname=smith should have been analyzed
    val statsPart1Str = session.sql(s"describe extended ${srcTable.fullName} partition(type='ext',lastname='doe')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart1Str.contains("1 rows"))
    val statsPart2Str = session.sql(s"describe extended ${srcTable.fullName} partition(type='ext',lastname='smith')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart2Str.contains("1 rows"))
    // check no partition statistics for type=int,lastname=emma
    val statsPart3Str = session.sql(s"describe extended ${srcTable.fullName} partition(type='int',lastname='emma')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(!statsPart3Str.contains("1 rows"))
    // check table contents
    assert(srcDO.getDataFrame().count==3)
  }

  test("write and analyze table with multi partition layout and full partition values") {
    val srcTable = Table(Some("default"), "input")
    val srcDO = HiveTableDataObject( "input", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("type","lastname"), numInitialHdfsPartitions = 1, analyzeTableAfterWrite = true)
    srcDO.dropTable
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq(PartitionValues(Map("type"->"ext", "lastname"->"doe")),PartitionValues(Map("type"->"ext", "lastname"->"smith"))))
    // check table statistics
    val statsStr = session.sql(s"describe extended ${srcTable.fullName}")
      .where($"col_name"==="Statistics").head.getAs[String](1)
    assert(statsStr.contains("3 rows"))
    // check partition statistics -> only partition type=ext,name=doe and type=ext,lastname=smith should have been analyzed
    val statsPart1Str = session.sql(s"describe extended ${srcTable.fullName} partition(type='ext',lastname='doe')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart1Str.contains("1 rows"))
    val statsPart2Str = session.sql(s"describe extended ${srcTable.fullName} partition(type='ext',lastname='smith')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart2Str.contains("1 rows"))
    // check no partition statistics for type=int,lastname=emma
    val statsPart3Str = session.sql(s"describe extended ${srcTable.fullName} partition(type='int',lastname='emma')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(!statsPart3Str.contains("1 rows"))
    // check table contents
    assert(srcDO.getDataFrame().count==3)
  }

  test("overwrite only one partition") {

    // create data object
    val srcTable = Table(Some("default"), "input")
    val srcDO = HiveTableDataObject( "input", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("p"), numInitialHdfsPartitions = 1)
    srcDO.dropTable

    // write test data 1 - create partition A and B
    val partitionValuesCreated = Seq( PartitionValues(Map("p"->"A")), PartitionValues(Map("p"->"B")))
    val df1 = Seq(("A",1),("A",2),("B",3),("B",4)).toDF("p", "value")
    srcDO.writeDataFrame(df1, partitionValuesCreated )

    // test 1
    srcDO.getDataFrame().count shouldEqual 4 // four records should remain, 2 from partition A and 2 from partition B
    partitionValuesCreated.toSet shouldEqual srcDO.listPartitions.toSet

    // write test data 2 - overwrite partition B
    val df2 = Seq(("B",5)).toDF("p", "value")
    srcDO.writeDataFrame(df2, partitionValuesCreated )

    // test 2
    srcDO.getDataFrame().count shouldEqual 3 // three records should remain, 2 from partition A and 1 from partition B
    partitionValuesCreated.toSet shouldEqual srcDO.listPartitions.toSet
  }

  test("create and list partition one level") {

    // create data object
    val srcTable = Table(Some("default"), "input")
    val srcDO = HiveTableDataObject( "input", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("p"), numInitialHdfsPartitions = 1)
    srcDO.dropTable

    // write test files
    val partitionValuesCreated = Seq(PartitionValues(Map("p"->"A")), PartitionValues(Map("p"->"B")))
    val df = Seq(("A",1),("B",2)).toDF("p", "value")
    srcDO.writeDataFrame(df, partitionValuesCreated )

    val partitionValuesListed = srcDO.listPartitions
    partitionValuesCreated.toSet shouldEqual partitionValuesListed.toSet
  }

  test("create and list partition multi level") {

    // create data object
    val srcTable = Table(Some("default"), "input")
    val srcDO = HiveTableDataObject( "input", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("p1","p2"), numInitialHdfsPartitions = 1)
    srcDO.dropTable

    // write test files
    val partitionValuesCreated = Seq( PartitionValues(Map("p1"->"A","p2"->"L2A")), PartitionValues(Map("p1"->"A","p2"->"L2B"))
      , PartitionValues(Map("p1"->"B","p2"->"L2B")), PartitionValues(Map("p1"->"B","p2"->"L2C")))
    val df = Seq(("A","L2A",1),("A","L2B",2),("B","L2B",3),("B","L2C",4)).toDF("p1", "p2", "value")
    srcDO.writeDataFrame(df, partitionValuesCreated )

    val partitionValuesListed = srcDO.listPartitions
    partitionValuesCreated.toSet shouldEqual partitionValuesListed.toSet
  }

  test("create empty partition") {

    // create data object
    val srcTable = Table(Some("default"), "input")
    val srcDO = HiveTableDataObject( "input", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("p1","p2"), numInitialHdfsPartitions = 1)
    srcDO.dropTable

    // write test files
    val partitionValuesCreated = Seq( PartitionValues(Map("p1"->"A","p2"->"L2A")), PartitionValues(Map("p1"->"X","p2"->"L2X")))
    val df = Seq(("A","L2A",1)).toDF("p1", "p2", "value")
    srcDO.writeDataFrame(df, partitionValuesCreated )

    val partitionValuesListed = srcDO.listPartitions
    partitionValuesCreated.toSet shouldEqual partitionValuesListed.toSet
  }

  test("Reading from an non-existing path is not possible.") {
    val srcTable = Table(Some("default"), "emptytesttable")

    val path = tempPath + s"/${srcTable.fullName}"
    val config = ConfigFactory.parseString(
      s"""
         |{
         | id = src1
         | path = "${escapedFilePath(path)}"
         | table = {
         |  name = ${srcTable.name}
         |  db = ${srcTable.db.get}
         | }
         |}
       """.stripMargin)
    val dataObj = HiveTableDataObject.fromConfig(config)

    an [Exception] should be thrownBy dataObj.getDataFrame()
  }

  test("writing not possible without ACLs if authority restricted") {

    // restrict default authority
    // remove 2 characters from the end to test that a substring is enough for restriction
    Environment.hadoopAuthoritiesWithAclsRequired = Seq(HdfsUtil.getHadoopDefaultSchemeAuthority().toString.reverse.drop(2).reverse)

    try {
      // create data object
      val srcTable = Table(Some("default"), "input")
      val srcDO = HiveTableDataObject("input", Some(tempPath + s"/${srcTable.fullName}"), table = srcTable, partitions = Seq("p1", "p2"), numInitialHdfsPartitions = 1)
      srcDO.dropTable

      // write test files
      val df = Seq(("A", "L2A", 1), ("A", "L2B", 2), ("B", "L2B", 3), ("B", "L2C", 4)).toDF("p1", "p2", "value")
      intercept[IllegalArgumentException](srcDO.preWrite)

    } finally {
      // reset restriction
      Environment.hadoopAuthoritiesWithAclsRequired = Seq()
    }

  }

  test("Path must be specified if table does not exist yet") {
    val df = Seq(("A", "2", 1), ("B", "1", 2), ("C", "X", 3)).toDF("p1", "p2", "value")
    val tgtTable = Table(Some("default"), "nonexistenttgttable")
    val tgtDO = HiveTableDataObject("tgtthatsurelydoesnotexistyet", path=None, table = tgtTable)
    an [Exception] should be thrownBy tgtDO.writeDataFrame(df, partitionValues = Seq())
  }
}
