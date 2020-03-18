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

import com.holdenkarau.spark.testing.Utils
import com.typesafe.config.ConfigFactory
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.hive.HiveUtil

class HiveTableDataObjectTest extends DataObjectTestSuite {

  val tempDir: File = Utils.createTempDir()
  val tempPath: String = tempDir.toPath.toAbsolutePath.toString

  import testSession.implicits._

  test("write and analyze table without partitions") {
    val srcTable = Table(Some("default"), "input")
    HiveUtil.dropTable(testSession, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "input", srcPath, table = srcTable, numInitialHdfsPartitions = 1, analyzeTableAfterWrite = true)
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq())
    // check table statistics
    val statsStr = testSession.sql(s"describe extended ${srcTable.fullName}")
      .where($"col_name"==="Statistics").head.getAs[String](1)
    assert(statsStr.contains("3 rows"))
    // check table contents
    assert(srcDO.getDataFrame.count==3)
  }

  test("write and analyze table with partitions and partition values") {
    val srcTable = Table(Some("default"), "input")
    HiveUtil.dropTable(testSession, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "input", srcPath, table = srcTable, partitions = Seq("type"), numInitialHdfsPartitions = 1, analyzeTableAfterWrite = true)
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq(PartitionValues(Map("type"->"ext")),PartitionValues(Map("type"->"int"))))
    // check table statistics
    val statsStr = testSession.sql(s"describe extended ${srcTable.fullName}")
      .where($"col_name"==="Statistics").head.getAs[String](1)
    assert(statsStr.contains("3 rows"))
    // check partition statistics
    val statsPart1Str = testSession.sql(s"describe extended ${srcTable.fullName} partition(type='ext')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart1Str.contains("2 rows"))
    val statsPart2Str = testSession.sql(s"describe extended ${srcTable.fullName} partition(type='int')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart2Str.contains("1 rows"))
    // check table contents
    assert(srcDO.getDataFrame.count==3)
  }

  test("write and analyze table with partitions without partition values") {
    val srcTable = Table(Some("default"), "input")
    HiveUtil.dropTable(testSession, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "input", srcPath, table = srcTable, partitions = Seq("type"), numInitialHdfsPartitions = 1, analyzeTableAfterWrite = true)
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq())
    // check table statistics
    val statsStr = testSession.sql(s"describe extended ${srcTable.fullName}")
      .where($"col_name"==="Statistics").head.getAs[String](1)
    assert(statsStr.contains("3 rows"))
    // check partition statistics
    val statsPart1Str = testSession.sql(s"describe extended ${srcTable.fullName} partition(type='ext')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart1Str.contains("2 rows"))
    val statsPart2Str = testSession.sql(s"describe extended ${srcTable.fullName} partition(type='int')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart2Str.contains("1 rows"))
    // check table contents
    assert(srcDO.getDataFrame.count==3)
  }

  test("write and analyze table with multi partition layout and partial partition values") {
    val srcTable = Table(Some("default"), "input")
    HiveUtil.dropTable(testSession, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "input", srcPath, table = srcTable, partitions = Seq("type","lastname"), numInitialHdfsPartitions = 1, analyzeTableAfterWrite = true)
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq(PartitionValues(Map("type"->"ext"))))
    // check table statistics
    val statsStr = testSession.sql(s"describe extended ${srcTable.fullName}")
      .where($"col_name"==="Statistics").head.getAs[String](1)
    assert(statsStr.contains("3 rows"))
    // check partition statistics -> only partition type=ext,name=doe and type=ext,lastname=smith should have been analyzed
    val statsPart1Str = testSession.sql(s"describe extended ${srcTable.fullName} partition(type='ext',lastname='doe')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart1Str.contains("1 rows"))
    val statsPart2Str = testSession.sql(s"describe extended ${srcTable.fullName} partition(type='ext',lastname='smith')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart2Str.contains("1 rows"))
    // check no partition statistics for type=int,lastname=emma
    val statsPart3Str = testSession.sql(s"describe extended ${srcTable.fullName} partition(type='int',lastname='emma')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(!statsPart3Str.contains("1 rows"))
    // check table contents
    assert(srcDO.getDataFrame.count==3)
  }

  test("write and analyze table with multi partition layout and full partition values") {
    val srcTable = Table(Some("default"), "input")
    HiveUtil.dropTable(testSession, srcTable.db.get, srcTable.name )
    val srcPath = tempPath+s"/${srcTable.fullName}"
    val srcDO = HiveTableDataObject( "input", srcPath, table = srcTable, partitions = Seq("type","lastname"), numInitialHdfsPartitions = 1, analyzeTableAfterWrite = true)
    val df = Seq(("ext","doe","john",5),("ext","smith","peter",3),("int","emma","brown",7)).toDF("type", "lastname", "firstname", "rating")
    srcDO.writeDataFrame(df, Seq(PartitionValues(Map("type"->"ext", "lastname"->"doe")),PartitionValues(Map("type"->"ext", "lastname"->"smith"))))
    // check table statistics
    val statsStr = testSession.sql(s"describe extended ${srcTable.fullName}")
      .where($"col_name"==="Statistics").head.getAs[String](1)
    assert(statsStr.contains("3 rows"))
    // check partition statistics -> only partition type=ext,name=doe and type=ext,lastname=smith should have been analyzed
    val statsPart1Str = testSession.sql(s"describe extended ${srcTable.fullName} partition(type='ext',lastname='doe')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart1Str.contains("1 rows"))
    val statsPart2Str = testSession.sql(s"describe extended ${srcTable.fullName} partition(type='ext',lastname='smith')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(statsPart2Str.contains("1 rows"))
    // check no partition statistics for type=int,lastname=emma
    val statsPart3Str = testSession.sql(s"describe extended ${srcTable.fullName} partition(type='int',lastname='emma')")
      .where($"col_name"==="Partition Statistics").head.getAs[String](1)
    assert(!statsPart3Str.contains("1 rows"))
    // check table contents
    assert(srcDO.getDataFrame.count==3)
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
    val dataObj = HiveTableDataObject.fromConfig(config, instanceRegistry)

    an [Exception] should be thrownBy dataObj.getDataFrame
  }
}
