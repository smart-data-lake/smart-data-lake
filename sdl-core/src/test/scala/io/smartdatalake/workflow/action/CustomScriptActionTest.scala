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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry, SdlConfigObject}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.script.CmdScript
import io.smartdatalake.workflow.dataobject.{CanReceiveScriptNotification, CsvFileDataObject, DataObject, DataObjectMetadata}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, FileSubFeed, SparkSubFeed}
import org.apache.commons.lang.NotImplementedException
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files

class CustomScriptActionTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    .copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("custom script action notification") {

    val feed = "script"
    val tempDir = Files.createTempDirectory(feed)

    // notify test variable & function
    var testVar: Option[String] = None
    def notify(v: String): Unit = {
      testVar = Some(v)
    }

    // setup DataObjects
    val src1DO = CsvFileDataObject("src1", tempDir.resolve("src1").toString.replace('\\', '/'))
    val src2DO = CsvFileDataObject("src2", tempDir.resolve("src2").toString.replace('\\', '/'))
    val tgtDO = TestScriptNotificationDataObject("tgt", notify )
    instanceRegistry.register(src1DO)
    instanceRegistry.register(src2DO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    val action1 = CustomScriptAction(id = "a1", Seq(src1DO.id, src2DO.id), Seq(tgtDO.id),
      scripts = Seq(CmdScript(winCmd = Some("echo test=OK"), linuxCmd = Some("echo test=OK"))))
    val src1SubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    val src2SubFeed = SparkSubFeed(None, "src2", partitionValues = Seq())
    val tgtSubFeed = action1.exec(Seq(src1SubFeed, src2SubFeed)).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    // check notification variable
    assert(testVar.contains("OK"))
  }

}

case class TestScriptNotificationDataObject(override val id: DataObjectId, notifyFunc: String => Unit) extends DataObject with CanReceiveScriptNotification {
  override def metadata: Option[DataObjectMetadata] = None

  override def scriptNotification(parameters: Map[String, String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    notifyFunc(parameters("test"))
  }

  override def factory: FromConfigFactory[DataObject] = throw new NotImplementedException
}
