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
package io.smartdatalake.workflow.action.spark.transformer

import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigParser, InstanceRegistry}
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestTool, SparkTestUtil}
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.spark.customlogic.{CustomDfTransformer, CustomDfsTransformer}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, InitSubFeed}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

/**
 * Tests for the dynamic transform method of the 1:1 transformer interfaces and of transformers implemented as
 * Scala code compiled at runtime.
 */
class DynamicTransformTest extends AnyFunSuite with SparkTestTool {

  @transient implicit private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  protected implicit val session: SparkSession = SparkTestUtil.session

  import session.implicits._

  private lazy val dfSrc: DataFrame = Seq(("x", 1)).toDF("a", "b")

  test("CopyAction with dynamic ScalaClassSparkDfTransformer") {

    val config = ConfigFactory.parseString(
      s"""
         |actions {
         |   dynamicCopy {
         |     type = CopyAction
         |     inputId = src
         |     outputId = tgt
         |     transformers = [{
         |       type = ScalaClassSparkDfTransformer
         |       className = io.smartdatalake.workflow.action.spark.transformer.DynamicDfTransformer
         |       options {
         |         factor = 4
         |       }
         |     }]
         |   }
         |}
         |dataObjects {
         |  src {
         |    type = io.smartdatalake.testutils.spark.MockSparkDataObject
         |  }
         |  tgt {
         |    type = io.smartdatalake.testutils.spark.MockSparkDataObject
         |  }
         |}
         |""".stripMargin).resolve

    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    implicit val contextInit: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
    val contextExec = contextInit.copy(phase = ExecutionPhase.Exec)
    val src = instanceRegistry.get[MockSparkDataObject](DataObjectId("src"))
    val tgt = instanceRegistry.get[MockSparkDataObject](DataObjectId("tgt"))
    val action = instanceRegistry.get[CopyAction](ActionId("dynamicCopy"))
    val initSubFeeds = Seq(InitSubFeed("src", Seq()))

    src.writeSparkDataFrame(dfSrc)
    action.prepare
    action.init(initSubFeeds)
    action.exec(initSubFeeds)(contextExec)

    assert(tgt.getSparkDataFrame().head().getInt(1) == 4)
  }

  test("CustomDfTransformer can dynamically map parameters") {
    val transformer = new DynamicDfTransformer
    val result = transformer.transform(session, Map("factor" -> "3"), dfSrc, "src")
    assert(result.columns.toSeq == Seq("a", "b"))
    assert(result.head().getInt(1) == 3)
  }

  test("CustomDfTransformer maps the single input DataFrame independent of the parameter name") {
    val transformer = new DynamicDfTransformerWithOtherDfName
    val result = transformer.transform(session, Map(), dfSrc, "src")
    assert(result.columns.toSeq == Seq("a", "b"))
  }

  test("CustomDfTransformer gets dataObjectId as option") {
    val transformer = new DynamicDfTransformerWithDataObjectId
    val result = transformer.transform(session, Map(), dfSrc, "src")
    assert(result.head().getString(0) == "src")
  }

  test("CustomDfTransformer with standard transform method is still supported") {
    val transformer = new StdDfTransformer
    val result = transformer.transform(session, Map(), dfSrc, "src")
    assert(result.columns.toSeq == Seq("a", "b"))
  }

  test("CustomDfTransformer can dynamically map a typed Dataset parameter and return value") {
    val transformer = new DynamicDsTransformer
    val result = transformer.transform(session, Map("factor" -> "2"), dfSrc, "src")
    assert(result.head().getInt(1) == 2)
  }

  test("ScalaCode compiled at runtime can implement a dynamic CustomDfsTransformer") {
    val code =
      """
        |import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
        |import org.apache.spark.sql.DataFrame
        |import org.apache.spark.sql.functions.lit
        |new CustomDfsTransformer {
        |  def transform(dfSrc: DataFrame, factor: Int = 2): Map[String,DataFrame] = {
        |    Map("tgt" -> dfSrc.withColumn("c", lit(factor)))
        |  }
        |}
        |""".stripMargin
    val transformer = CustomCodeUtil.compileCode[CustomDfsTransformer](code)
    val result = transformer.transform(session, Map("factor" -> "5"), Map("src" -> dfSrc))
    assert(result.keys.toSeq == Seq("tgt"))
    assert(result("tgt").head().getInt(2) == 5)
  }

  test("ScalaCode compiled at runtime can implement a dynamic CustomDfTransformer") {
    val code =
      """
        |import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformer
        |import org.apache.spark.sql.DataFrame
        |import org.apache.spark.sql.functions.lit
        |new CustomDfTransformer {
        |  def transform(df: DataFrame, factor: Int = 2): DataFrame = df.withColumn("c", lit(factor))
        |}
        |""".stripMargin
    val transformer = CustomCodeUtil.compileCode[CustomDfTransformer](code)
    val result = transformer.transform(session, Map("factor" -> "7"), dfSrc, "src")
    assert(result.head().getInt(2) == 7)
  }

  test("Notebook code is wrapped into a CustomDfTransformer calling the function dynamically") {
    val notebookCode =
      """
        |import org.apache.spark.sql.functions.lit
        |def myTransform(df: DataFrame, factor: Int = 2): DataFrame = df.withColumn("c", lit(factor))
        |""".stripMargin
    val code = ScalaNotebookSparkDfTransformer.prepareFunction(notebookCode, "myTransform")
    val transformer = ScalaNotebookSparkDfTransformer.compileCode(code)
    val result = transformer.transform(session, Map("factor" -> "9"), dfSrc, "src")
    assert(result.head().getInt(2) == 9)
  }

  test("Notebook function with classic signature is still supported") {
    val notebookCode =
      """
        |def myTransform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String): DataFrame = df
        |""".stripMargin
    val code = ScalaNotebookSparkDfTransformer.prepareFunction(notebookCode, "myTransform")
    val transformer = ScalaNotebookSparkDfTransformer.compileCode(code)
    val result = transformer.transform(session, Map(), dfSrc, "src")
    assert(result.columns.toSeq == Seq("a", "b"))
  }
}

class DynamicDfTransformer extends CustomDfTransformer {
  def transform(session: SparkSession, dfSrc: DataFrame, factor: Int): DataFrame = {
    import org.apache.spark.sql.functions.col
    assert(session != null)
    dfSrc.withColumn("b", col("b") * factor)
  }
}

class DynamicDfTransformerWithOtherDfName extends CustomDfTransformer {
  def transform(dfSomethingElse: DataFrame): DataFrame = dfSomethingElse
}

class DynamicDfTransformerWithDataObjectId extends CustomDfTransformer {
  def transform(df: DataFrame, dataObjectId: String): DataFrame = {
    import org.apache.spark.sql.functions.lit
    df.select(lit(dataObjectId).as("a"))
  }
}

class StdDfTransformer extends CustomDfTransformer {
  override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = df
}

class DynamicDsTransformer extends CustomDfTransformer {
  def transform(ds: Dataset[TestDs], factor: Int): Dataset[TestDs] = {
    import ds.sparkSession.implicits._
    ds.map(x => x.copy(b = x.b * factor)) // typed operation, needs a correct encoder for TestDs
  }
}

case class TestDs(a: String, b: Int)
