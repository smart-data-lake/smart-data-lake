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

package io.smartdatalake.workflow.action.spark.transformer

import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigParser, InstanceRegistry}
import io.smartdatalake.testutils.{MockSparkDataObject, TestUtil}
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.workflow.action.CustomDataFrameAction
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, InitSubFeed}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.runtime.universe.typeOf

class CustomDfsTransformerTest extends AnyFunSuite
  with io.smartdatalake.testutils.spark.dataset.TestToolDataset
  with io.smartdatalake.util.spark.dataset.Equality {

  @transient implicit private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  test("CustomDataFrameAction with dynamic transformer returning multiple DataFrames") {

    val config = ConfigFactory.parseString(
      s"""
         |actions {
         |   dynamicDfTransform {
         |     type = CustomDataFrameAction
         |     inputIds = [src]
         |     outputIds = [tgt]
         |     transformers = [{
         |       type = ScalaClassSparkDfsTransformer
         |       className = io.smartdatalake.workflow.action.spark.transformer.DynamicReturnDfTransformer
         |       options {
         |         long = 123
         |       }
         |     }]
         |   }
         |}
         |dataObjects {
         |  src {
         |    type = io.smartdatalake.testutils.MockSparkDataObject
         |  }
         |  tgt {
         |    type = io.smartdatalake.testutils.MockSparkDataObject
         |  }
         |}
         |""".stripMargin).resolve

    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    val contextExec = contextInit.copy(phase = ExecutionPhase.Exec)
    val src = instanceRegistry.get[MockSparkDataObject](DataObjectId("src"))
    val tgt = instanceRegistry.get[MockSparkDataObject](DataObjectId("tgt"))
    val action = instanceRegistry.get[CustomDataFrameAction](ActionId("dynamicDfTransform"))
    val initSubFeeds = Seq(InitSubFeed("src", Seq()))

    val dfSrc = Seq(("x", 1)).toDF("a", "b")
    src.writeSparkDataFrame(dfSrc)
    action.prepare
    action.init(initSubFeeds)
    action.exec(initSubFeeds)(contextExec)

    assert(dfSrc.equal(tgt.getSparkDataFrame()))
  }

  test("CustomDataFrameAction with dynamic transformer returning one Dataset[Test]") {

    val config = ConfigFactory.parseString(
      s"""
         |actions {
         |   dynamicDsTransform {
         |     type = CustomDataFrameAction
         |     inputIds = [src]
         |     outputIds = [tgt]
         |     transformers = [{
         |       type = ScalaClassSparkDfsTransformer
         |       className = io.smartdatalake.workflow.action.spark.transformer.DynamicReturnDsTransformer
         |       options {
         |         long = 123
         |       }
         |     }]
         |   }
         |}
         |dataObjects {
         |  src {
         |    type = io.smartdatalake.testutils.MockSparkDataObject
         |  }
         |  tgt {
         |    type = io.smartdatalake.testutils.MockSparkDataObject
         |  }
         |}
         |""".stripMargin).resolve

    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    val contextExec = contextInit.copy(phase = ExecutionPhase.Exec)
    val src = instanceRegistry.get[MockSparkDataObject](DataObjectId("src"))
    val tgt = instanceRegistry.get[MockSparkDataObject](DataObjectId("tgt"))
    val action = instanceRegistry.get[CustomDataFrameAction](ActionId("dynamicDsTransform"))
    val initSubFeeds = Seq(InitSubFeed("src", Seq()))

    val dfSrc = Seq(("x", 1)).toDF("a", "b")
    src.writeSparkDataFrame(dfSrc)
    action.prepare
    action.init(initSubFeeds)
    action.exec(initSubFeeds)(contextExec)

    assert(dfSrc.equal(tgt.getSparkDataFrame()))
  }

  test("CustomDfsTransformer can dynamically map parameters") {
    val transformer = new DynamicReturnUnitTestTransformer(true, Some(true), false, 1L, Some(1L), Seq())
    val options = Map("isExec" -> "true", "optionalBoolean" -> "true", "defaultBoolean" -> "false", "long" -> "1", "optionalLong" -> "1")
    val df1 = Seq(("x", 1)).toDF("a", "b")
    val dfs = Map("test" -> df1)
    val resultDfs = transformer.transform(session, options, dfs)
    assert(resultDfs.keys.toSeq == Seq("test"))
  }

  test("Extract method parameter default values") {
    val transformer = new DynamicReturnUnitTestTransformer(true, Some(true), false, 1L, Some(1L), Seq())
    val transformMethodsOfSubclass = CustomCodeUtil.getClassMethodsByName(transformer.getClass, "transform")
      .filter(_.owner != typeOf[CustomDfsTransformer].typeSymbol)
    val transformMethod = transformMethodsOfSubclass.head
    val params = CustomCodeUtil.analyzeMethodParameters(Some(transformer), transformMethod)
    assert(params.find(_.name == "defaultBoolean").get.defaultValue.nonEmpty)
  }

  test("CustomDfsTransformer can dynamically map parameters with default values or optional") {
    val transformer = new DynamicReturnUnitTestTransformer(true, None, true, 1L, None, Seq("a", "b", "c"))
    val options = Map("isExec" -> "true", "long" -> "1", "seqString" -> "a,b, c")
    val df1 = Seq(("x", 1)).toDF("a", "b")
    val dfs = Map("test" -> df1)
    val resultDfs = transformer.transform(session, options, dfs)
    assert(resultDfs.keys.toSeq == Seq("test"))
  }
}

/**
 * This transformer is created for unit tests.
 * It cannot be used in Actions as it has this needs a constructor without arguments.
 */
class DynamicReturnUnitTestTransformer(isExecExpected: Boolean, optionalBooleanExpected: Option[Boolean], defaultBooleanExpected: Boolean = true, longExpected: Long, optionalLongExpected: Option[Long], seqStringExpected: Seq[String]) extends CustomDfsTransformer {
  def transform(session: SparkSession, dfTest: DataFrame, dsTest: Dataset[Test], isExec: Boolean, optionalBoolean: Option[Boolean], defaultBoolean: Boolean = true, long: Long, optionalLong: Option[Long], seqString: Seq[String] = Seq()) = {
    assert(session != null)
    assert(dfTest.columns.toSeq == Seq("a", "b"))
    assert(dsTest.toDF().columns.toSeq == Seq("a", "b"))
    assert(isExec == isExecExpected)
    assert(optionalBoolean == optionalBooleanExpected)
    assert(defaultBoolean == defaultBooleanExpected)
    assert(long == longExpected)
    assert(optionalLong == optionalLongExpected)
    assert(seqString == seqStringExpected)
    Map("test" -> dfTest)
  }
}

class DynamicReturnDfTransformer extends CustomDfsTransformer {
  def transform(session: SparkSession, dfSrc: DataFrame, dsSrc: Dataset[Test], isExec: Boolean, optionalBoolean: Option[Boolean], defaultBoolean: Boolean = true, long: Long, optionalLong: Option[Long]) = {
    Map("tgt" -> dfSrc)
  }
}

class DynamicReturnDsTransformer extends CustomDfsTransformer {
  def transform(session: SparkSession, dfSrc: DataFrame, dsSrc: Dataset[Test], isExec: Boolean, optionalBoolean: Option[Boolean], defaultBoolean: Boolean = true, long: Long, optionalLong: Option[Long]) = {
    dsSrc
  }
}

case class Test(a: String, b: Int)