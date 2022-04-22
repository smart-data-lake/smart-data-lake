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
import io.smartdatalake.testutils.TestUtil._
import io.smartdatalake.workflow.action.spark.customlogic.{CustomDsTransformer, InputDSType, OutputDSType}
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDsTransformer
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.CsvFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files

case class InputDataSet(name: String, rating: Int) extends InputDSType

case class OutputDataSet(name: String, rating: Int, doubled_rating: Int) extends OutputDSType

class TestDSTransformer extends CustomDsTransformer[InputDataSet, OutputDataSet] {

  import functions.col

  override def transform(session: SparkSession, options: Map[String, String], inputDS: Dataset[InputDataSet], dataObjectId: String): Dataset[OutputDataSet] = {
    inputDS.withColumn("doubled_rating", col("rating") * 2).as[OutputDataSet]
  }
}

class ScalaClassSparkDsTransformerTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog

  import sessionHiveCatalog.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextExec: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("One simple Dataset transformation with different input and output Dataset-type") {

    // setup DataObjects
    // source has partition columns dt and type
    val srcDO = CsvFileDataObject("src1", tempPath + "/src1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int"))))
    instanceRegistry.register(srcDO)
    // first table has partitions columns dt and type (same as source)
    val tgt1DO = CsvFileDataObject("tgt1", tempPath + "/tgt1", partitions = Seq("name")
      , schema = Some(SparkSchema(StructType.fromDDL("name string, rating int, doubled_rating int"))))
    instanceRegistry.register(tgt1DO)

    // fill src with first files
    val dfSrc1 = Seq(("john", 5))
      .toDF("name", "rating")
    srcDO.writeSparkDataFrame(dfSrc1, Seq())

    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgt1DO)

    // prepare & start load
    val testAction = CopyAction(id = s"ScalaClassSparkDsTransformer", inputId = srcDO.id, outputId = tgt1DO.id,
      transformers = Seq(ScalaClassSparkDsTransformer(transformerClassName = "io.smartdatalake.workflow.action.TestDSTransformer")))
    val srcSubFeed = SparkSubFeed(None, "src1", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))

    val actual = tgt1DO.getSparkDataFrame().as[OutputDataSet].head()
    assert(actual.doubled_rating == 10)
  }
}