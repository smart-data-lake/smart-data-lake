/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.workflow.action.generic.transformer

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf

class OptionsGenericTransformerTest extends AnyFunSuite {

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  private val helper = DataFrameSubFeed.getCompanion(typeOf[ScalaSubFeed])
  import helper.implicits._
  private val df = Seq(("a", 1)).toDF("dt", "value")
  private val partitionValues = Seq(PartitionValues(Map("dt" -> "20260101")), PartitionValues(Map("dt" -> "20260102")))

  test("default options of a 1:1 transformer include nbOfPartitions") {
    val transformer = OptionsCapturingDfTransformer()
    transformer.transform(ActionId("a"), partitionValues, df, DataObjectId("src"), None, Map())
    assert(transformer.capturedOptions(OptionsGenericDfsTransformer.NB_OF_PARTITIONS) == "2")
    assert(transformer.capturedOptions(OptionsGenericDfsTransformer.IS_EXEC) == context.isExecPhase.toString)
  }

  test("default options of a n:m transformer include nbOfPartitions") {
    val transformer = OptionsCapturingDfsTransformer()
    transformer.transform(ActionId("a"), partitionValues, Map("src" -> df), Map(), Seq("tgt"))
    assert(transformer.capturedOptions(OptionsGenericDfsTransformer.NB_OF_PARTITIONS) == "2")
  }

  test("configured options override the default nbOfPartitions") {
    val transformer = OptionsCapturingDfTransformer(options = Map(OptionsGenericDfsTransformer.NB_OF_PARTITIONS -> "5"))
    transformer.transform(ActionId("a"), partitionValues, df, DataObjectId("src"), None, Map())
    assert(transformer.capturedOptions(OptionsGenericDfsTransformer.NB_OF_PARTITIONS) == "5")
  }
}

case class OptionsCapturingDfTransformer(options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map())
    extends OptionsGenericDfTransformer {
  override def name: String = "capture"
  override def description: Option[String] = None
  var capturedOptions: Map[String, String] = Map()
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, options: Map[String, String])(implicit
      context: ActionPipelineContext
  ): GenericDataFrame = {
    capturedOptions = options
    df
  }
}

case class OptionsCapturingDfsTransformer(options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map())
    extends OptionsGenericDfsTransformer {
  override def name: String = "capture"
  override def description: Option[String] = None
  var capturedOptions: Map[String, String] = Map()
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String, GenericDataFrame], options: Map[String, String])(implicit
      context: ActionPipelineContext
  ): Map[String, GenericDataFrame] = {
    capturedOptions = options
    dfs
  }
}
