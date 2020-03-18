/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019 ELCA Informatique SA (<https://www.elca.ch>)
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
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.Try

/**
 * Unit tests for HiveUtil
 */
class HiveUtilTest extends FunSuite with BeforeAndAfter with SmartDataLakeLogger {

  implicit lazy val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  val hiveDBName = "default"
  val hiveTableName = "unittesttable"

  val tmpDirOnFS: Path = Files.createTempDirectory("sdl_test")
  val tableDirOnFS: Path = Paths.get(tmpDirOnFS.toString, hiveTableName)
  val hdfsTablePath: String = tableDirOnFS.toUri.toString // we use local filesystem and hive catalog for testing

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
    Try(HiveUtil.execSqlStmt(session, s"Drop table if exists $hiveDBName.$hiveTableName"))
    Try(HiveUtil.execSqlStmt(session, s"Drop table if exists $hiveDBName.${hiveTableName}_tmp"))
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

  def checkPartitionsExpected( hiveDb:String, tableName:String, expectedPartitions:Seq[Map[String,String]] ) : Boolean = {
    val tablePartitions = HiveUtil.getTablePartitions(hiveDb, tableName)
    tablePartitions.toSet.equals(expectedPartitions.toSet)
  }

  test("Create unpartitioned external table and overwrite data") {
    val partitions = Seq()

    logger.info("Creating table")
    HiveUtil.writeDfToHive(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    intercept[AnalysisException]{
      // AnalysisException expected because table is not partitioned
      HiveUtil.getTablePartitions(hiveDBName, hiveTableName).isEmpty
    }
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataA ))

    logger.info("Overwriting data in existing table")
    HiveUtil.writeDfToHive(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataA ))
  }

  test("Create unpartitioned external table and overwrite data with schema evolution without Tick-Tock") {
    val partitions = Seq()
    val useTickTock = false

    logger.info("Creating table")
    HiveUtil.writeDfToHive(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    intercept[AnalysisException] {
      // AnalysisException expected because table is not partitioned
      HiveUtil.getTablePartitions(hiveDBName, hiveTableName).isEmpty
    }
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataA ))

    logger.info("Overwriting data in existing table with modified schema without Tick-Tock")
    HiveUtil.writeDfToHive(session, testDataB, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataB ))
  }

  test("Create unpartitioned external table and overwrite data with schema evolution with TickTock") {
    val partitions = Seq()
    val useTickTock = true

    logger.info("Creating table")
    HiveUtil.writeDfToHiveWithTickTock(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    intercept[AnalysisException]{
      // AnalysisException expected because table is not partitioned
      HiveUtil.getTablePartitions(hiveDBName, hiveTableName).isEmpty
    }
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataA ))

    logger.info("Overwriting data in existing table with modified schema with Tick-Tock")
    HiveUtil.writeDfToHiveWithTickTock(session, testDataB, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataB ))
  }

  test("Create partitioned external table and overwrite data") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHive(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    assert(checkPartitionsExpected(hiveDBName, hiveTableName, Seq(Map( "part" -> "X"), Map("part" -> "Y"))))
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataA ))

    logger.info("Overwriting data in existing table with modified schema with Tick-Tock")
    HiveUtil.writeDfToHive(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    assert(checkPartitionsExpected(hiveDBName, hiveTableName, Seq(Map( "part" -> "X"), Map("part" -> "Y"))))
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataA ))
  }

  test("Create partitioned table and overwrite data with schema evolution with TickTock") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHiveWithTickTock(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    assert(HiveUtil.getTablePartitions(hiveDBName, hiveTableName).toSet.equals(Set(Map( "part" -> "X"), Map("part" -> "Y"))))
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataA ))

    logger.info("Overwriting data in existing table with modified schema and Tick-Tock")
    HiveUtil.writeDfToHiveWithTickTock(session, testDataB, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    assert(HiveUtil.getTablePartitions(hiveDBName, hiveTableName).toSet.equals(Set(Map( "part" -> "Y"), Map("part" -> "Z"))))
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataB ))
  }

  test("Creating a partitioned table and overwriting data with schema evolution with TickTock aborts") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHive(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    assert(HiveUtil.getTablePartitions(hiveDBName, hiveTableName).toSet.equals(Set(Map( "part" -> "X"), Map("part" -> "Y"))))
    assert(TestUtil.isDataFrameEqual( session.table(hiveTableName), testDataA ))

    logger.info("Overwriting data in existing table with modified schema and Tick-Tock")
    intercept[IllegalArgumentException] {
      HiveUtil.writeDfToHiveWithTickTock(session, testDataB, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    }
  }

  test("Unpartitioned external table with TickTock changes directory when written to") {
    val partitions = Seq()

    logger.info("Creating table")
    HiveUtil.writeDfToHiveWithTickTock(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    val suffix1 = HiveUtil.getCurrentTickTockLocationSuffix( hiveDBName, session, hiveTableName )

    logger.info("Overwriting data in existing table with modified schema and Tick-Tock")
    HiveUtil.writeDfToHiveWithTickTock(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    val suffix2 = HiveUtil.getCurrentTickTockLocationSuffix( hiveDBName, session, hiveTableName )

    assert(suffix1 != suffix2)
  }

  test("Partitioned external table with TickTock does not change directory when written to without schema evolution") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHiveWithTickTock(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    val suffix1 = HiveUtil.getCurrentTickTockLocationSuffix( hiveDBName, session, hiveTableName )

    logger.info("Overwriting data in existing table with modified schema and Tick-Tock")
    HiveUtil.writeDfToHiveWithTickTock(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    val suffix2 = HiveUtil.getCurrentTickTockLocationSuffix( hiveDBName, session, hiveTableName )

    assert(suffix1 == suffix2)
  }

  test("Partitioned external table with TickTock changes directory when written to with schema evolution") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHiveWithTickTock(session, testDataA, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    val suffix1 = HiveUtil.getCurrentTickTockLocationSuffix( hiveDBName, session, hiveTableName )

    logger.info("Overwriting data in existing table with modified schema and Tick-Tock")
    HiveUtil.writeDfToHiveWithTickTock(session, testDataB, hdfsTablePath, hiveTableName, hiveDBName, partitions, SaveMode.Overwrite)
    val suffix2 = HiveUtil.getCurrentTickTockLocationSuffix( hiveDBName, session, hiveTableName )

    assert(suffix1 != suffix2)
  }
}
