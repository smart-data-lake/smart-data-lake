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

package io.smartdatalake.testutils

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.exporter.ExportWriter.formatSchema
import io.smartdatalake.config.exporter.HadoopExportWriter
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.generic.customlogic.CustomGenericDfTransformer
import io.smartdatalake.workflow.action.generic.transformer.ScalaClassGenericDfTransformer
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame, TableDataObject}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.spark.SparkFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.hadoop.fs.Path
import org.slf4j.Logger

import java.nio.file.Files

trait CopyActionBehaviour {
  this: SmartDataLakeLogger =>

  implicit private val implicitLogger: Logger = logger
  import TestUtil.registerDataObject

  def defaultEngineConnection: Connection with EngineConnection

  def testCopyActionOffline(
                              createSrcDataObject: ((String, InstanceRegistry) => DataObject with CanCreateDataFrame),
                              createTgtDataObject: ((String, Option[Seq[String]], InstanceRegistry) => DataObject with CanCreateDataFrame with CanWriteDataFrame)
                            ): DataFrameSubFeed = {

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(defaultEngineConnection)

    // setup DataObjects
    val srcDO = registerDataObject(createSrcDataObject("src1", instanceRegistry))
    val tgtDO = registerDataObject(createTgtDataObject("tgt1", Some(Seq("lastname", "firstname")), instanceRegistry))
    val helper = DataFrameSubFeed.getCompanion(srcDO.getSubFeedSupportedTypes.head)
    import helper.implicits._

    // prepare schema export
    val tempDir = Files.createTempDirectory(this.getClass.getSimpleName)
    val contextInitWithSchemaSource: ActionPipelineContext = contextInit.copy(
      globalConfig = contextInit.globalConfig.copy(dataObjectsSchemaSource = Some(tempDir.resolve("test1/schema").toString))
    )
    val l1 = Seq(("jonson", "rob", 5), ("doe", "bob", 3))
      .toDF("lastname", "firstname", "rating")
    val exporter = HadoopExportWriter(new Path(tempDir.resolve("test1/schema").toString))
    exporter.writeSchema(formatSchema(Some(l1.schema), None), srcDO.id, 1000L)

    // prepare & start load
    val customTransformerConfig = ScalaClassGenericDfTransformer(className = classOf[TestGenericDfTransformer].getName)
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id, transformers = Seq(customTransformerConfig))
    val srcSubFeed = ScalaSubFeed(None, "src1", Seq())
    val tgtSubFeed = action1.init(Seq(srcSubFeed))(contextInitWithSchemaSource).head.asInstanceOf[DataFrameSubFeed]
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    tgtSubFeed
  }
}


class TestGenericDfTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    import helper._
    df.withColumn("rating", col("rating") + lit(1))
  }
}
