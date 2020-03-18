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

import java.io.{File, PrintWriter}
import java.nio.file.Files

import com.holdenkarau.spark.testing.Utils
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.TryWithRessource
import io.smartdatalake.workflow.action.customlogic.{CustomFileTransformer, CustomFileTransformerConfig}
import io.smartdatalake.workflow.dataobject.CsvFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, FileSubFeed}
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.io.Source

class CustomFileActionTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog

  val tempDir: File = Utils.createTempDir()
  val tempPath: String = tempDir.toPath.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  before {
    instanceRegistry.clear
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
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val fileTransformerConfig = CustomFileTransformerConfig(className = Some("io.smartdatalake.workflow.action.TestFileTransformer"))
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformerConfig, false, 1)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed)).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    // check if file is present
    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
    assert(r1.head.fileName == resourceFile)

    // read src with util and count
    val dfSrc = srcDO.getDataFrame
    assert(dfSrc.columns.length > 2)
    val srcCount = dfSrc.count

    // read tgt with util and count
    val dfTgt = tgtDO.getDataFrame
    assert(dfTgt.columns.length == 2)
    val tgtCount = dfTgt.count
    assert(srcCount == tgtCount)
  }
}
object CustomFileActionTest {
  val delimiter = ","
}

class TestFileTransformer extends CustomFileTransformer {
  override def transform(input: FSDataInputStream, output: FSDataOutputStream): Option[Exception] = {
    TryWithRessource.execSource(Source.fromInputStream(input)) { src =>
      TryWithRessource.exec(new PrintWriter(output)) { os =>
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