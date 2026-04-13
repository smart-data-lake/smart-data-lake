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
package io.smartdatalake.workflow.connection.spark

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, GlobalConfig, SmartDataLakeBuilderConfig, TestUDFAddXCreator}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.{MockSparkDataObject, TestUtil}
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.secrets.{SecretProvider, SecretProviderConfig, StringOrSecret}
import io.smartdatalake.workflow.{ActionDAGRun, ActionPipelineContext, ExecutionPhase}
import io.smartdatalake.workflow.action.executionMode.DataFrameIncrementalMode
import io.smartdatalake.workflow.action.generic.transformer.{SQLDfTransformer, SQLDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.SparkUDFCreatorConfig
import io.smartdatalake.workflow.action.{ActionMetadata, CopyAction, CustomDataFrameAction}
import io.smartdatalake.workflow.connection.SparkClassicConnection
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class SparkClassicConnectionTest extends AnyFunSuite with BeforeAndAfter {
  implicit lazy val session: SparkSession = TestUtil.session

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()

  private val contextInit = TestUtil.getDefaultActionPipelineContext
  private val contextPrep = contextInit.copy(phase = ExecutionPhase.Prepare)
  private implicit val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  private val sdlb = DefaultSmartDataLakeBuilder

  before {
    instanceRegistry.clear()
    instanceRegistry.register(TestUtil.defaultSparkConnection)
  }

  test("sparkOptions secrets are resolved in Spark session configuration") {
    // prepare
    val providerConfig = SecretProviderConfig(classOf[TestSecretProvider].getName, Some(Map()))

    // execute
    val globalConfig = GlobalConfig(
      secretProviders = Some(Map("TESTPROVIDER" -> providerConfig))
    )
    val sparkClassicConnection = SparkClassicConnection(
      id = "testConnection",
      master = Some("local"),
      sparkOptions = Map("spark.authenticate.secret" -> StringOrSecret("###TESTPROVIDER#secret###"))
    )
    val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext.copy(globalConfig = globalConfig)
    val sparkSession = sparkClassicConnection.sparkSession(context)

    // check
    assert(sparkSession.conf.get("spark.authenticate.secret") == "resolvedSecret")
  }

  test("apply udf from spark connection") {
    import session.implicits._

    // init sdlb
    val appName = "sdlb"
    val feedName = "test"

    // setup DataObjects
    val src1DO = MockSparkDataObject("src1").register
    val tgt1DO = MockSparkDataObject("tgt1").register

    val sparkClassicConnection = SparkClassicConnection(
      id = "testConnection",
      master = Some("local"),
      sparkUDFs = Some(Map("udfAddX" -> SparkUDFCreatorConfig(className = classOf[TestUDFAddXCreator].getName, options = Some(Map("x" -> "1")))))
    )
    instanceRegistry.register(sparkClassicConnection)

    // prepare data
    val dfSrc1 = Seq(
      (1, "20180101", "person",  "doe",  "john", 5),
      (2, "20190101", "company", "olmo", "-",    10)
    )
      .toDF("id", "dt", "type", "lastname", "firstname", "rating")
    src1DO.writeSparkDataFrame(dfSrc1, Seq())
    val dfSrc2 = Seq((1, "abc"))
      .toDF("id", "comment")

    // start first dag run -> fail
    // action1 has data
    val action1 = CopyAction(
      "a",
      src1DO.id,
      tgt1DO.id,
      transformers = Seq(SQLDfTransformer(code = Some("select id, dt, type, lastname, firstname, udfAddX(rating) rating from src1"))),
      metadata = Some(ActionMetadata(feed = Some(feedName))),
      engineConnectionId = Some("testConnection")
    )
    instanceRegistry.register(action1)

    // start dag run
    val dag = ActionDAGRun(Seq(action1))
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val actual = tgt1DO.getSparkDataFrame()
      .select($"rating").as[Int].collect
    assert(actual.toSeq == Seq(6, 11))
  }
}

class TestSecretProvider(options: Map[String, String]) extends SecretProvider {
  override def getSecret(name: String): String =
    name match {
      case "secret" => "resolvedSecret"
      case _        => throw new IllegalArgumentException("Secret cannot be resolved")
    }
}
