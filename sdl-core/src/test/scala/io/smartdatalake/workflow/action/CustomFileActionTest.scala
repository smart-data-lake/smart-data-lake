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
package io.smartdatalake.workflow.action

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.executionMode.PartitionDiffMode
import io.smartdatalake.workflow.action.spark.customlogic.{CustomFileTransformer, CustomFileTransformerConfig}
import io.smartdatalake.workflow.dataobject.CsvFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, FileSubFeed, InitSubFeed}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.PrintWriter
import java.nio.file.{Files, Path => NioPath}
import scala.io.Source
import scala.util.Using

class CustomFileActionTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  private var tempDir: NioPath = _
  private var tempPath: String = _

  before {
    instanceRegistry.clear()
    tempDir = Files.createTempDirectory("test")
    tempPath = tempDir.toAbsolutePath.toString
  }

  after {
    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("custom csv-file transformation") {

    val feed = "filetransfer"
    val srcDir = "testSrc"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile).toFile)

    // setup DataObjects
    val srcDO = CsvFileDataObject("src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    val fileTransformerConfig = CustomFileTransformerConfig(className = Some("io.smartdatalake.workflow.action.TestFileTransformer"), options = Some(Map("test"->"true")))
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformerConfig, 1)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed)).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    // check if file is present
    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
    assert(r1.head.fileName == resourceFile)

    // read src with util and count
    val dfSrc = srcDO.getSparkDataFrame()
    assert(dfSrc.columns.length > 2)
    val srcCount = dfSrc.count()

    // read tgt with util and count
    val dfTgt = tgtDO.getSparkDataFrame()
    assert(dfTgt.columns.length == 2)
    val tgtCount = dfTgt.count()
    assert(srcCount == tgtCount)
  }

  test("custom csv-file transformation with partition diff execution mode") {

    val feed = "filetransfer"
    val srcDir = "testSrc"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    val srcPartitionValues = Seq(PartitionValues(Map("p"->"test")))
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve("p=test/" + resourceFile).toFile)

    // setup DataObjects
    val srcDO = CsvFileDataObject("src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), partitions = Seq("p"), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), partitions = Seq("p"), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    val fileTransformerConfig = CustomFileTransformerConfig(className = Some("io.smartdatalake.workflow.action.TestFileTransformer"), options = Some(Map("test"->"true")))
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformerConfig, 1, executionMode = Some(PartitionDiffMode()))
    val srcSubFeed = InitSubFeed("src1", srcPartitionValues) // InitSubFeed needed to test initExecutionMode!
    val tgtSubFeed = action1.exec(Seq(srcSubFeed)).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)
    assert(tgtSubFeed.partitionValues.toSet == srcPartitionValues.toSet)
    assert(tgtDO.listPartitions.toSet == srcPartitionValues.toSet)

    // check if file is present
    assert(tgtDO.getFileRefs(Seq()).map(_.fileName) == Seq(resourceFile))
    assert(tgtDO.getFileRefs(srcPartitionValues).map(_.fileName) == Seq(resourceFile))

    // check counts
    val dfSrc = srcDO.getSparkDataFrame()
    val dfTgt = tgtDO.getSparkDataFrame()
    assert(dfSrc.count() == dfTgt.count())
  }

}
object CustomFileActionTest {
  val delimiter = ","
}

class TestFileTransformer extends CustomFileTransformer {
  override def transform(options: Map[String,String], input: FSDataInputStream, output: FSDataOutputStream): Option[Exception] = {
    assert(options("test")=="true")
    Using.resource(Source.fromInputStream(input)) { src =>
      Using.resource(new PrintWriter(output)) { os =>
        src.getLines().foreach { l =>
          // reduce to 2 cols
          val transformedLine = l.split(CustomFileActionTest.delimiter).take(2).mkString(CustomFileActionTest.delimiter)
          os.println(transformedLine)
        }
      }
      None
    }
  }
}