/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.config.exporter.ExportWriter.formatSchema
import io.smartdatalake.config.exporter.HadoopExportWriter
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfTransformer
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.ParquetFileDataObject
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class OfflineCopyActionTest extends AnyFunSuite {

  private val tempDir = Files.createTempDirectory("test")

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextInitWithSchemaSource: ActionPipelineContext = contextInit.copy(
    globalConfig = contextInit.globalConfig.copy(dataObjectsSchemaSource = Some(tempDir.resolve("test1/schema").toString))
  )

  test("copy dry-run in offline environment, reading exported schemas") {

    // setup DataObjects
    val srcDO = ParquetFileDataObject("src1", tempDir.resolve("test1/src1").toString, filenameColumn = Some("_filename"))
    instanceRegistry.register(srcDO)
    val tgtDO = MockDataObject("tgt1")
    instanceRegistry.register(tgtDO)

    // prepare schema export
    val l1 = Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    val exporter = HadoopExportWriter(new Path(tempDir.resolve("test1/schema").toString))
    exporter.writeSchema(formatSchema(Some(SparkSchema(l1.schema)), None), srcDO.id, 1000L)

    // prepare & start load
    val customTransformerConfig = ScalaClassSparkDfTransformer(className = classOf[TestDfTransformer].getName)
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(customTransformerConfig))
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.init(Seq(srcSubFeed))(contextInitWithSchemaSource).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

  }

}
