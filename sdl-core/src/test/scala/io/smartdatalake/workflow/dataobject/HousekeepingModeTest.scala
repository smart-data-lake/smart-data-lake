/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files

class HousekeepingModeTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  val df1 = Seq(("doe","john",5,"20201101"),("einstein","albert",2,"20201201"))
    .toDF("lastname", "firstname", "rating", "dt")

  before {
    instanceRegistry.clear()
  }

  test("PartitionRetentionMode") {
    val srcDO = CsvFileDataObject("srcDO", tempPath+s"/src0", partitions=Seq("dt"))
    val tgtDO = CsvFileDataObject("tgtDO", tempPath+s"/tgt1", partitions=Seq("dt")
      , housekeepingMode = Some(PartitionRetentionMode("elements.dt >= 20201201"))
    )
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)
    tgtDO.writeSparkDataFrame(df1, Seq()) // prefill target as we only execute postExec...
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id)
    val srcSubFeed = SparkSubFeed(None, "srcDO", Seq())
    val tgtSubFeed = SparkSubFeed(None, "tgtDO", Seq())
    action1.prepare
    assert(tgtDO.listPartitions.map(_.apply("dt").toString).sorted == Seq("20201101","20201201"))
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed)) // exec housekeeping

    // check partition dt=20201101 is deleted
    assert(tgtDO.listPartitions == Seq(PartitionValues(Map("dt" -> "20201201"))))
  }

  test("PartitionArchiveCompactionMode with SparkFileDataObject") {
    val srcDO = CsvFileDataObject("srcDO", tempPath+s"/src0", partitions=Seq("dt"))
    val tgtDO = CsvFileDataObject("tgtDO", tempPath+s"/tgt1", partitions=Seq("dt")
      , housekeepingMode = Some(PartitionArchiveCompactionMode(
        archivePartitionExpression = Some("map('dt','20201101')"), // always archive to 20201101
        compactPartitionExpression = Some("true") // compact all partitions...
      ))
    )
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)
    tgtDO.writeSparkDataFrame(df1, Seq()) // prefill target as we only execute postExec...
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id)
    val srcSubFeed = SparkSubFeed(None, "srcDO", Seq())
    val tgtSubFeed = SparkSubFeed(None, "tgtDO", Seq())
    action1.prepare
    assert(tgtDO.listPartitions.map(_.apply("dt").toString).sorted == Seq("20201101","20201201"))
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed)) // exec housekeeping

    // check partition dt=20201201 is archived and dt=20201101 is compacted
    assert(tgtDO.listPartitions == Seq(PartitionValues(Map("dt" -> "20201101"))))
    assert(tgtDO.filesystem.exists(new Path(tgtDO.hadoopPath, "dt=20201101/_SDL_COMPACTED")))
    val actual = tgtDO.getSparkDataFrame()
    val expected = df1.withColumn("dt", lit("20201101"))
    val resultat = actual.isEqual(expected)
    if (!resultat) TestUtil.printFailedTestResult("historize 1st load mergeModeEnable")(actual)(expected)
    assert(resultat)
  }

  test("PartitionArchiveCompactionMode with HiveTableDataObject") {
    val srcDO = CsvFileDataObject("srcDO", tempPath+s"/src0", partitions=Seq("dt"))
    val tgtDO = HiveTableDataObject("tgtDO", Some(tempPath+s"/tgt1"), partitions=Seq("dt"), table = Table(Some("default"), "tgtDO")
      , housekeepingMode = Some(PartitionArchiveCompactionMode(
        archivePartitionExpression = Some("map('dt','20201101')"), // always archive to 20201101
        compactPartitionExpression = Some("true") // compact all partitions...
      ))
    )
    tgtDO.dropTable
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)
    tgtDO.writeSparkDataFrame(df1, Seq()) // prefill target as we only execute postExec...
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id)
    val srcSubFeed = SparkSubFeed(None, "srcDO", Seq())
    val tgtSubFeed = SparkSubFeed(None, "tgtDO", Seq())
    action1.prepare
    assert(tgtDO.listPartitions.map(_.apply("dt").toString).sorted == Seq("20201101","20201201"))
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed)) // exec housekeeping

    // check partition dt=20201201 is archived and dt=20201101 is compacted
    assert(tgtDO.listPartitions == Seq(PartitionValues(Map("dt" -> "20201101"))))
    assert(tgtDO.filesystem.exists(new Path(tgtDO.hadoopPath, "dt=20201101/_SDL_COMPACTED")))
    assert(tgtDO.getSparkDataFrame().isEqual(df1.withColumn("dt", lit("20201101"))))
  }

}
