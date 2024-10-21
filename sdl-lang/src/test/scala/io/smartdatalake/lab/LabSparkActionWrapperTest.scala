/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.lab

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.workflow.action.CustomDataFrameAction
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfsTransformer
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class LabSparkActionWrapperTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  test("test applying transformers") {
    // setup DataObjects
    val srcDO1 = MockDataObject("src1").register
    val srcDO2 = MockDataObject("src2").register
    val tgtDO1 = MockDataObject("tgt1", primaryKey = Some(Seq("lastname", "firstname"))).register
    val tgtDO2 = MockDataObject("tgt2", primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare & start load
    val customTransformerConfig1 = ScalaClassSparkDfsTransformer(
      className = classOf[Tgt1DfsTransformer].getName, options = Map("increment1" -> "1")
    )
    val customTransformerConfig2 = ScalaClassSparkDfsTransformer(
      className = classOf[Tgt2DfsTransformer].getName, options = Map("increment2" -> "1")
    )
    val customTransformerConfig3 = ScalaClassSparkDfsTransformer(
      className = classOf[Tgt3DfsTransformer].getName
    )
    val customTransformerConfig4 = ScalaClassSparkDfsTransformer(
      className = classOf[Tgt4DfsTransformer].getName
    )

    val action1 = CustomDataFrameAction("action1", List(srcDO1.id, srcDO2.id), List(tgtDO1.id, tgtDO2.id), transformers = Seq(customTransformerConfig1, customTransformerConfig2))
    instanceRegistry.register(action1)
    val action1wrapper = LabSparkDfsActionWrapper(action1, contextExec)

    val l1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(l1, Seq())
    srcDO2.writeSparkDataFrame(l1, Seq())

    {
      val dfs = action1wrapper.buildDataFrames.get
      assert(dfs.keys == Set("src1", "src2", "tgt1", "tgt2"))
    }

    {
      val dfs = action1wrapper.buildDataFrames.withLimitedTransformerNb(1).get
      assert(dfs.keys == Set("src1", "src2", "tgt1"))
    }

    {
      val dfs = action1wrapper.buildDataFrames.withReplacedTransformer(0, customTransformerConfig3).get
      assert(dfs.keys == Set("src1", "src2", "tgt2", "tgt3"))
    }

    {
      val dfs = action1wrapper.buildDataFrames
        .withReplacedTransformer(0, customTransformerConfig4)
        .withAdditionalTransformerOptions(0, Map("option1" -> "test")).get
      assert(dfs.keys == Set("src1", "src2", "tgt2", "tgt3"))
    }
  }
}

class Tgt1DfsTransformer extends CustomDfsTransformer {
  def transform(session: SparkSession, increment1: Int, dfSrc1: DataFrame, dfSrc2: DataFrame): Map[String,DataFrame] = {
    import session.implicits._
    Map(
      "tgt1" -> dfSrc1.withColumn("rating", $"rating" + increment1)
    )
  }
}

class Tgt2DfsTransformer extends CustomDfsTransformer {
  def transform(session: SparkSession, increment2: Int, dfSrc1: DataFrame, dfSrc2: DataFrame): Map[String,DataFrame] = {
    import session.implicits._
    Map(
      "tgt2" -> dfSrc2.withColumn("rating", $"rating" + increment2)
    )
  }
}

class Tgt3DfsTransformer extends CustomDfsTransformer {
  def transform(session: SparkSession, dfSrc1: DataFrame, dfSrc2: DataFrame): Map[String,DataFrame] = {
    Map(
      "tgt3" -> dfSrc2
    )
  }
}

class Tgt4DfsTransformer extends CustomDfsTransformer {
  def transform(session: SparkSession, dfSrc1: DataFrame, dfSrc2: DataFrame, option1: String): Map[String, DataFrame] = {
    Map(
      "tgt3" -> dfSrc2
    )
  }
}