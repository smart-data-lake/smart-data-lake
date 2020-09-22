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
package io.smartdatalake.util.hive

import java.nio.file.{Files, Path, Paths}

import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataobject.Table
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.hadoop.fs.{Path => HadoopPath}

import scala.util.Try

/**
 * Unit tests for HiveUtil
 */
class HiveUtilTest extends FunSuite with BeforeAndAfter with SmartDataLakeLogger {

  implicit lazy val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val hiveTable = Table(Some("default"), "unittesttable")
  private val hiveTableTmp = Table(Some("default"), "unittesttable_tmp")

  val tmpDirOnFS: Path = Files.createTempDirectory("sdl_test")
  val tableDirOnFS: Path = Paths.get(tmpDirOnFS.toString, hiveTable.name)
  val hdfsTablePath: HadoopPath = new HadoopPath(tableDirOnFS.toUri) // we use local filesystem and hive catalog for testing

  before {
    // make sure directory exists for Tick-Tock mode in Windows
    FileUtils.forceMkdir(tableDirOnFS.toFile)
  }

  after {
    cleanup()
  }

  private def cleanup(): Unit = {
    logger.info("cleanup!")
    // cleanup tables
    HiveUtil.dropTable(hiveTable)
    HiveUtil.dropTable(hiveTableTmp)
    // cleanup existing files
    FileUtils.deleteDirectory(tmpDirOnFS.toFile)
  }

  val testDataA: DataFrame = session.createDataset(Seq(
    (1, "A", "X"),
    (2, "B", "X"),
    (3, "C", "Y"),
    (4, "C", "Y"))).toDF( "id", "data1", "part" )
  val testDataB: DataFrame = session.createDataset(Seq(
    (1, "A", "C", "Z"),
    (2, "B", "B", "Z"),
    (3, "C", "A", "Y"),
    (4, "C", "A", "Y"))).toDF( "id", "data1", "data2", "part" )

  def checkPartitionsExpected( table: Table, expectedPartitions:Seq[Map[String,String]] ) : Boolean = {
    val tablePartitions = HiveUtil.getTablePartitions(table)
    tablePartitions.toSet.equals(expectedPartitions.toSet)
  }

  test("Create unpartitioned external table and overwrite data") {
    val partitions = Seq()

    logger.info("Creating table")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    intercept[AnalysisException]{
      // AnalysisException expected because table is not partitioned
      HiveUtil.getTablePartitions(hiveTable).isEmpty
    }
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataA ))

    logger.info("Overwriting data in existing table")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataA ))
  }

  test("Create unpartitioned external table and overwrite data with schema evolution without Tick-Tock") {
    val partitions = Seq()
    val useTickTock = false

    logger.info("Creating table")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    intercept[AnalysisException] {
      // AnalysisException expected because table is not partitioned
      HiveUtil.getTablePartitions(hiveTable).isEmpty
    }
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataA ))

    logger.info("Overwriting data in existing table with modified schema without Tick-Tock")
    HiveUtil.writeDfToHive(testDataB, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataB ))
  }

  test("Create unpartitioned external table and overwrite data with schema evolution with TickTock") {
    val partitions = Seq()
    val useTickTock = true

    logger.info("Creating table")
    HiveUtil.writeDfToHiveWithTickTock(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    intercept[AnalysisException]{
      // AnalysisException expected because table is not partitioned
      HiveUtil.getTablePartitions(hiveTable).isEmpty
    }
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataA ))

    logger.info("Overwriting data in existing table with modified schema with Tick-Tock")
    HiveUtil.writeDfToHiveWithTickTock(testDataB, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataB ))
  }

  test("Create partitioned external table and overwrite data") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(checkPartitionsExpected(hiveTable, Seq(Map( "part" -> "X"), Map("part" -> "Y"))))
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataA ))

    logger.info("Overwriting data in existing table with modified schema with Tick-Tock")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(checkPartitionsExpected(hiveTable, Seq(Map( "part" -> "X"), Map("part" -> "Y"))))
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataA ))
  }

  test("Create partitioned table and overwrite data with schema evolution with TickTock") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHiveWithTickTock(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(HiveUtil.getTablePartitions(hiveTable).toSet.equals(Set(Map( "part" -> "X"), Map("part" -> "Y"))))
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataA ))

    logger.info("Overwriting data in existing table with modified schema and Tick-Tock")
    HiveUtil.writeDfToHiveWithTickTock(testDataB, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(HiveUtil.getTablePartitions(hiveTable).toSet.equals(Set(Map( "part" -> "Y"), Map("part" -> "Z"))))
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataB ))
  }

  test("Creating a partitioned table and overwriting data with schema evolution with TickTock aborts") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(HiveUtil.getTablePartitions(hiveTable).toSet.equals(Set(Map( "part" -> "X"), Map("part" -> "Y"))))
    assert(TestUtil.isDataFrameEqual( session.table(hiveTable.fullName), testDataA ))

    logger.info("Overwriting data in existing table with modified schema and Tick-Tock")
    intercept[IllegalArgumentException] {
      HiveUtil.writeDfToHiveWithTickTock(testDataB, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    }
  }

  test("Unpartitioned external table with TickTock changes directory when written to") {
    val partitions = Seq()

    logger.info("Creating table")
    HiveUtil.writeDfToHiveWithTickTock(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    val suffix1 = HiveUtil.getCurrentTickTockLocationSuffix(hiveTable)

    logger.info("Overwriting data in existing table with modified schema and Tick-Tock")
    HiveUtil.writeDfToHiveWithTickTock(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    val suffix2 = HiveUtil.getCurrentTickTockLocationSuffix(hiveTable)

    assert(suffix1 != suffix2)
  }

  test("Partitioned external table with TickTock does not change directory when written to without schema evolution") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHiveWithTickTock(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    val suffix1 = HiveUtil.getCurrentTickTockLocationSuffix(hiveTable)

    logger.info("Overwriting data in existing table with modified schema and Tick-Tock")
    HiveUtil.writeDfToHiveWithTickTock(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    val suffix2 = HiveUtil.getCurrentTickTockLocationSuffix(hiveTable)

    assert(suffix1 == suffix2)
  }

  test("Partitioned external table with TickTock changes directory when written to with schema evolution") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHiveWithTickTock(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    val suffix1 = HiveUtil.getCurrentTickTockLocationSuffix(hiveTable)

    logger.info("Overwriting data in existing table with modified schema and Tick-Tock")
    HiveUtil.writeDfToHiveWithTickTock(testDataB, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    val suffix2 = HiveUtil.getCurrentTickTockLocationSuffix(hiveTable)

    assert(suffix1 != suffix2)
  }

  test("Normalize Paths") {
    // Make sure only the last tock is switched
    val inputText = "file:\\\\some\\tock\\path\\tock\\"
    val expectedNormalization = "/some/tock/path/tick"

    assert(HiveUtil.normalizePath(inputText) == expectedNormalization)
  }

  test("Normalize Paths with prefix") {
    // Make sure only the last tock is switched
    val pathPrefix = "hdfs://nameservice1/user/hansi"
    val inputText = s"${pathPrefix}file:\\\\some\\tock\\path\\tock\\"
    val expectedNormalization = "/some/tock/path/tick"

    assert(HiveUtil.normalizePath(inputText,Some(pathPrefix)) == expectedNormalization)
  }

}
