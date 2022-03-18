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
package io.smartdatalake.workflow.dataobject
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.action.CustomSparkAction
import io.smartdatalake.workflow.action.customlogic.CustomDfsTransformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassDfsTransformer
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files


class TickTockHiveTableDataObjectTest extends FunSuite with BeforeAndAfter {
  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("Empty dataframe is created if schemaMin is provided") {

    val schemaMin: StructType = StructType(StructField("id", IntegerType, nullable = false) :: StructField("name", StringType, nullable=false) :: StructField("rating", IntegerType, nullable=false) :: Nil)

    // create source dataobject
    val srcTable = Table(Some("default"), "input")
    val srcDO = TickTockHiveTableDataObject("input", Some(tempPath + s"/${srcTable.fullName}"), table = srcTable, partitions = Seq(), numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)

    // create target
    val tgtTable = Table(Some("default"),"output")
    val tgtDO = TickTockHiveTableDataObject("output", Some(tempPath + s"/${tgtTable.fullName}"), table = tgtTable, partitions = Seq(), numInitialHdfsPartitions = 1, schemaMin=Some(SparkSchema(schemaMin)))
    tgtDO.dropTable
    instanceRegistry.register(tgtDO)

    val customTransformerConfig = ScalaClassDfsTransformer(className = classOf[TestDfsTransformerEmptyDf].getName)
    val action = CustomSparkAction("action", List(srcDO.id), List(tgtDO.id), transformers = Seq(customTransformerConfig), recursiveInputIds = List(tgtDO.id))

    // write test files
    val l1 = Seq((1, "john", 5)).toDF("id", "name", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())

    action.exec(Seq(SparkSubFeed(None, "input", Seq()), SparkSubFeed(None, "output", Seq())))(contextExec)

    val r = session.table(s"${tgtTable.fullName}")
      .select($"rating")
      .as[Int].collect().toSeq
    assert(r.size == 1)
    assert(r.head == 6) // should be increased by 1 through TestDfTransformer
  }
}

class TestDfsTransformerEmptyDf extends CustomDfsTransformer {
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String,DataFrame]): Map[String,DataFrame] = {
    import session.implicits._

    Map(
      "output" -> dfs("input").withColumn("rating", $"rating"+1)
    )

  }
}
