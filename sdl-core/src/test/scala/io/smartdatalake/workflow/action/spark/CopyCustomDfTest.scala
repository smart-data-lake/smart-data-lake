/*
 * sdl-core - Build your data lake the smart way.
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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.spark.dataset.Collection
import io.smartdatalake.testutils.{MockSparkDataObject, TestUtil}
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.spark.transformer.StandardizeSparkDatatypesTransformer
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.{ParquetFileDataObject, TestData}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path => NioPath}

class CopyCustomDfTest extends AnyFunSuite with BeforeAndAfter
  with io.smartdatalake.testutils.spark.dataset.TestToolDataset {
  @transient implicit private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextExec: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

  private var tempDir: NioPath = _
  private var tempPath: String = _

  before {
    instanceRegistry.clear()
    instanceRegistry.register(TestUtil.defaultSparkConnection)
    tempDir = Files.createTempDirectory("test")
    tempPath = tempDir.toAbsolutePath.toString
  }

  after {
    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("load custom data frame into mock DataObject. Reads data and compares with input.") {

    // setup DataObjects
    val feed = "customDf2Hive"
    val sourceDO = MockSparkDataObject(id = "source").register
    sourceDO.writeSparkDataFrame(Collection.dfSimple1)
    val targetDO = MockSparkDataObject(id = "target").register
    instanceRegistry.register(sourceDO)


    // prepare & start load
    val testAction = CopyAction(id = s"${feed}Action", inputId = sourceDO.id, outputId = targetDO.id)
    val srcSubFeed = SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))

    val expected = sourceDO.getSparkDataFrame()
    val actual = targetDO.getSparkDataFrame()
    val resultat: Boolean = expected.equal(actual)
    if (!resultat) printFailedTestResult("Df2HiveTable", Seq())(actual)(expected)
    assert(resultat)
  }


  test("columns of decimal type should be casted to integral or float type.") {

    // setup DataObjects
    val feed = "customDf_dfManyTypes"
    val sourceDO = MockSparkDataObject(id = "source").register
    sourceDO.writeSparkDataFrame(Collection.dfManyTypes)
    val targetDO = ParquetFileDataObject(id = "target", tempPath + s"/customDfCopy")
    instanceRegistry.register(sourceDO)
    instanceRegistry.register(targetDO)

    // prepare & start load
    val testAction = CopyAction(id = s"${feed}Action", inputId = sourceDO.id, outputId = targetDO.id,
      transformers = Seq(StandardizeSparkDatatypesTransformer())
    )
    val srcSubFeed = SparkSubFeed(None, "source", partitionValues = Seq())
    testAction.exec(Seq(srcSubFeed))

    val actual = targetDO.getSparkDataFrame()
    val expected = sourceDO.getSparkDataFrame()
      .withColumn("_decimal_2_0", $"_decimal_2_0".cast(ByteType))
      .withColumn("_decimal_4_0", $"_decimal_4_0".cast(ShortType))
      .withColumn("_decimal_10_0", $"_decimal_10_0".cast(IntegerType))
      .withColumn("_decimal_11_0", $"_decimal_11_0".cast(LongType))
      .withColumn("_decimal_4_3", $"_decimal_4_3".cast(FloatType))
      .withColumn("_decimal_38_1", $"_decimal_38_1".cast(DoubleType))
    val resultat: Boolean = expected.equal(actual)
    if (!resultat) printFailedTestResult("customDf_dfManyTypes", Seq())(actual)(expected)
    assert(resultat)
  }

}
