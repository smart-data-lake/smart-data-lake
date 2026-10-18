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
package io.smartdatalake.workflow.action.spark

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.definitions.{MergeFilterDialect, MergeModeFilter, SaveModeMergeOptions}
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.UpsertAction
import io.smartdatalake.workflow.action.executionMode.SparkStreamingMode
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite

class MergeModeFilterSparkTest extends AnyFunSuite {

  private val session = SparkTestUtil.session
  import session.implicits._

  private val actionId = ActionId("test")

  private def setup(): (InstanceRegistry, ActionPipelineContext, MockSparkDataObject, SparkDataFrame) = {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
    implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext
    val output = MockSparkDataObject("tgt", primaryKey = Some(Seq("id")))
    instanceRegistry.register(output)
    val df = SparkDataFrame(Seq((1, "A"), (2, "B")).toDF("id", "dt"))
    output.writeSparkDataFrame(df.inner, Seq(), isRecursiveInput = false, saveModeOptions = None) // output table must exist
    (instanceRegistry, context, output, df)
  }

  private def storageLevel(df: Any): StorageLevel = df.asInstanceOf[SparkDataFrame].inner.storageLevel

  test("incoming data is not cached, as caching interferes with the observation of metrics") {
    val (_, context, output, df) = setup()
    val (levelRange, optionsRange) = MergeModeFilter(Seq("id"))
      .writeWithPredicate(actionId, output, df, Seq(), SaveModeMergeOptions(), usePartitionValues = true)((d, o) => (storageLevel(d), o))(context)
    assert(levelRange == StorageLevel.NONE)
    assert(optionsRange.additionalMergePredicate.contains("(existing.id BETWEEN 1 AND 2)"))
    val (levelExact, optionsExact) = MergeModeFilter(Seq("dt"))
      .writeWithPredicate(actionId, output, df, Seq(PartitionValues(Map("dt" -> "A"))), SaveModeMergeOptions(), usePartitionValues = true)((d, o) => (storageLevel(d), o))(context)
    assert(levelExact == StorageLevel.NONE)
    assert(optionsExact.additionalMergePredicate.contains("(existing.dt = 'A')"))
  }

  test("exact predicate for date partition columns") {
    val dfDate = SparkDataFrame(Seq((1, java.sql.Date.valueOf("2026-01-02"))).toDF("id", "dt"))
    val filter = MergeModeFilter()
    val dialect = new MergeFilterDialect {
      override def quote(column: String): String = column
      override def literal(value: Any): Option[String] = MergeModeFilter.renderLiteral(value, isSparkSql = true)
    }
    val isoValues = Seq(PartitionValues(Map("dt" -> "2026-01-02")))
    assert(filter.getExactPartitionValues(Seq("dt"), dfDate, isoValues) == isoValues)
    assert(filter.getExactPartitionValues(Seq("dt"), dfDate, Seq(PartitionValues(Map("dt" -> "20260102")))).isEmpty)
    assert(filter.createPredicate(actionId, Seq("dt"), dfDate, isoValues, dialect).contains("existing.dt = DATE '2026-01-02'"))
  }

  test("mergeModeFilter is rejected with streaming execution mode") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
    instanceRegistry.register(MockSparkDataObject("src"))
    instanceRegistry.register(MockSparkDataObject("tgt", primaryKey = Some(Seq("id"))))
    val ex = intercept[ConfigurationException](
      UpsertAction("a1", "src", "tgt", executionMode = Some(SparkStreamingMode("/tmp/checkpoint")), mergeModeFilter = Some(MergeModeFilter(Seq("id"))))
    )
    assert(ex.getMessage.contains("not supported with streaming"))
  }
}
