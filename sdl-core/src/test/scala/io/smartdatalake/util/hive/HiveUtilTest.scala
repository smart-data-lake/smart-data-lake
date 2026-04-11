/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.hive.HiveUtil
import io.smartdatalake.workflow.dataobject.generic.Table
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.Logger

import java.nio.file.{Files, Path, Paths}

/**
 * Unit tests for HiveUtil
 */
class HiveUtilTest extends AnyFunSuite with BeforeAndAfter with SmartDataLakeLogger
  with io.smartdatalake.testutils.spark.dataset.TestToolDataset {

  private implicit val loggerImpl: Logger = logger
  implicit lazy val session: SparkSession = TestUtil.session

  import session.implicits._

  private val hiveTable = Table(Some("default"), "unittesttable")

  val tmpDirOnFS: Path = Files.createTempDirectory("sdl_test")
  val tableDirOnFS: Path = Paths.get(tmpDirOnFS.toString, hiveTable.name)
  val hdfsTablePath: HadoopPath = new HadoopPath(tableDirOnFS.toUri) // we use local filesystem and hive catalog for testing

  before {
    // make sure directory exists in Windows
    FileUtils.forceMkdir(tableDirOnFS.toFile)
  }

  after {
    cleanup()
  }

  private def cleanup(): Unit = {
    logger.info("cleanup!")
    // cleanup tables
    HiveUtil.dropTable(hiveTable, hdfsTablePath)
    // cleanup existing files
    FileUtils.deleteDirectory(tmpDirOnFS.toFile)
  }

  val testDataA: DataFrame = session.createDataset(Seq(
    (1, "A", "X"),
    (2, "B", "X"),
    (3, "C", "Y"),
    (4, "C", "Y"))).toDF("id", "data1", "part")
  val testDataB: DataFrame = session.createDataset(Seq(
    (1, "A", "C", "Z"),
    (2, "B", "B", "Z"),
    (3, "C", "A", "Y"),
    (4, "C", "A", "Y"))).toDF("id", "data1", "data2", "part")

  def checkPartitionsExpected(table: Table, expectedPartitions: Seq[Map[String, String]]): Boolean = {
    val tablePartitions = HiveUtil.getTablePartitions(table)
    tablePartitions.toSet.equals(expectedPartitions.toSet)
  }

  test("Create unpartitioned external table and overwrite data") {
    val partitions = Seq()

    logger.info("Creating table")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    intercept[AnalysisException] {
      // AnalysisException expected because table is not partitioned
      HiveUtil.getTablePartitions(hiveTable).isEmpty
    }
    assert(session.table(hiveTable.fullName).equal(testDataA))

    logger.info("Overwriting data in existing table")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(session.table(hiveTable.fullName).equal(testDataA))
  }

  test("Create unpartitioned external table and overwrite data with schema evolution") {
    val partitions = Seq()

    logger.info("Creating table")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    intercept[AnalysisException] {
      // AnalysisException expected because table is not partitioned
      HiveUtil.getTablePartitions(hiveTable).isEmpty
    }
    assert(session.table(hiveTable.fullName).equal(testDataA))

    logger.info("Overwriting data in existing table with modified schema")
    HiveUtil.writeDfToHive(testDataB, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(session.table(hiveTable.fullName).equal(testDataB))
  }

  test("Create partitioned external table and overwrite data") {
    val partitions = Seq("part")

    logger.info("Creating table")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(checkPartitionsExpected(hiveTable, Seq(Map("part" -> "X"), Map("part" -> "Y"))))
    assert(session.table(hiveTable.fullName).equal(testDataA))

    logger.info("Overwriting data in existing table with modified schema")
    HiveUtil.writeDfToHive(testDataA, hdfsTablePath, hiveTable, partitions, SaveMode.Overwrite)
    assert(checkPartitionsExpected(hiveTable, Seq(Map("part" -> "X"), Map("part" -> "Y"))))
    assert(session.table(hiveTable.fullName).equal(testDataA))
  }

}
