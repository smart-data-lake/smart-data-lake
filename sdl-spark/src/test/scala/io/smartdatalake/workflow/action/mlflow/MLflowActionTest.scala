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
package io.smartdatalake.workflow.action.mlflow

import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigParser, ConfigurationException, InstanceRegistry}
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.util.mlflow.MLflowRunInfo
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.MLflowDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, InitSubFeed, ParameterSubFeed}
import io.smartdatalake.testutils.spark.MockSparkDataObject
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests of the MLflow Actions which do not need MLflow or a python environment.
 *
 * They cover configuration parsing and the Init phase, which by contract must not talk to MLflow at all.
 * Everything needing a real MLflow instance is in [[MLflowEndToEndTest]].
 */
class MLflowActionTest extends AnyFunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = SparkTestUtil.session
  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext

  before {
    instanceRegistry.clear()
  }

  private def registerMlflow(id: String = "mlflow-test", experimentName: String = "test-experiment"): MLflowDataObject = {
    val mlflow = MLflowDataObject(DataObjectId(id), experimentName = experimentName)
    instanceRegistry.register(mlflow)
    mlflow
  }

  private def registerInput(id: String = "src"): MockSparkDataObject = {
    val input = MockSparkDataObject(DataObjectId(id))
    input.writeSparkDataFrame(Seq((1, 2.5), (2, 3.5)).toDF("nights", "reviews_per_month"))
    instanceRegistry.register(input)
    input
  }

  test("MLflowDataObject is parsable from config") {
    val config = ConfigFactory.parseString(
      """
        |id = mlflow-test
        |type = MLflowDataObject
        |trackingUri = "http://localhost:5001"
        |experimentName = "price-prediction"
        |envManager = virtualenv
        |""".stripMargin)
    val dataObject = ConfigParser.parseConfigObject[io.smartdatalake.workflow.dataobject.DataObject](config).asInstanceOf[MLflowDataObject]
    assert(dataObject.experimentName == "price-prediction")
    assert(dataObject.trackingUri == "http://localhost:5001")
    assert(dataObject.envManager == "virtualenv")
  }

  test("MLflowDataObject rejects an unknown envManager") {
    intercept[ConfigurationException](MLflowDataObject(DataObjectId("mlflow-test"), experimentName = "e", envManager = "docker"))
  }

  test("MLflowDataObject remembers the run info it is notified about") {
    val mlflow = registerMlflow()
    assert(mlflow.getLastRunInfo.isEmpty)
    val runInfo = Map(MLflowRunInfo.RunId -> "run-1", MLflowRunInfo.ModelUri -> "runs:/run-1/model")
    mlflow.parameterNotification(runInfo, Seq())
    assert(mlflow.getLastRunInfo.contains(runInfo))
    assert(mlflow.getRunInfo.contains(runInfo)) // served from memory, no MLflow call
  }

  test("MLflowTrainAction is parsable from config") {
    registerMlflow()
    registerInput()
    val config = ConfigFactory.parseString(
      """
        |id = train
        |type = MLflowTrainAction
        |inputId = src
        |mlflowId = mlflow-test
        |modelName = "price-regressor"
        |registerModel = true
        |modelAlias = champion
        |pythonModelCode = "pass"
        |""".stripMargin)
    val action = ConfigParser.parseConfigObject[Action](config).asInstanceOf[MLflowTrainAction]
    assert(action.modelName == "price-regressor")
    assert(action.inputs.map(_.id.id) == Seq("src"))
    assert(action.outputs.map(_.id.id) == Seq("mlflow-test"))
    assert(action.modelCode == "pass")
  }

  test("MLflowTrainAction needs exactly one of pythonModelCode and pythonModelFile") {
    val mlflow = registerMlflow()
    registerInput()
    val missing = intercept[ConfigurationException](
      MLflowTrainAction(ActionId("train"), DataObjectId("src"), mlflow.id, "m").modelCode)
    assert(missing.getMessage.contains("pythonModelCode or pythonModelFile"))
    val both = intercept[ConfigurationException](
      MLflowTrainAction(ActionId("train"), DataObjectId("src"), mlflow.id, "m",
        pythonModelCode = Some("pass"), pythonModelFile = Some("model.py")).modelCode)
    assert(both.getMessage.contains("only one of"))
  }

  test("MLflowTrainAction rejects a modelAlias without registerModel") {
    val mlflow = registerMlflow()
    registerInput()
    intercept[ConfigurationException](
      MLflowTrainAction(ActionId("train"), DataObjectId("src"), mlflow.id, "m",
        pythonModelCode = Some("pass"), modelAlias = Some("champion")))
  }

  test("MLflowTrainAction does not touch MLflow in init phase") {
    val mlflow = registerMlflow()
    val input = registerInput()
    val action = MLflowTrainAction(ActionId("train"), input.id, mlflow.id, "m", pythonModelCode = Some("pass"))
    // an unreachable tracking uri would make any MLflow call fail, so this asserts that none is made
    val initContext = context.copy(phase = ExecutionPhase.Init)
    val result = action.init(Seq(InitSubFeed(input.id, Seq())))(initContext)
    assert(result.size == 1)
    val subFeed = result.head.asInstanceOf[ParameterSubFeed]
    assert(subFeed.dataObjectId == mlflow.id)
    assert(subFeed.parameters.isEmpty)
    assert(mlflow.getLastRunInfo.isEmpty)
  }

  test("MLflowPredictAction is parsable from config") {
    registerMlflow()
    registerInput()
    instanceRegistry.register(MockSparkDataObject(DataObjectId("tgt")))
    val config = ConfigFactory.parseString(
      """
        |id = predict
        |type = MLflowPredictAction
        |inputId = src
        |mlflowId = mlflow-test
        |outputId = tgt
        |modelName = "price-regressor"
        |modelAlias = champion
        |predictionColumn = price_pred
        |resultType = double
        |""".stripMargin)
    val action = ConfigParser.parseConfigObject[Action](config).asInstanceOf[MLflowPredictAction]
    assert(action.inputs.map(_.id.id) == Seq("src", "mlflow-test"))
    assert(action.outputs.map(_.id.id) == Seq("tgt"))
    assert(action.getModelUri == "models:/price-regressor@champion")
  }

  test("MLflowPredictAction rejects an ambiguous model reference") {
    val mlflow = registerMlflow()
    registerInput()
    instanceRegistry.register(MockSparkDataObject(DataObjectId("tgt")))
    intercept[ConfigurationException](
      MLflowPredictAction(ActionId("predict"), DataObjectId("src"), mlflow.id, DataObjectId("tgt"),
        modelName = Some("m"), modelAlias = Some("champion"), modelVersion = Some("1")))
    intercept[ConfigurationException](
      MLflowPredictAction(ActionId("predict"), DataObjectId("src"), mlflow.id, DataObjectId("tgt"),
        modelAlias = Some("champion")))
  }

  test("MLflowPredictAction resolves the model uri by precedence") {
    val mlflow = registerMlflow()
    val input = registerInput()
    instanceRegistry.register(MockSparkDataObject(DataObjectId("tgt")))
    def action(modelName: Option[String] = None, alias: Option[String] = None,
               version: Option[String] = None, uri: Option[String] = None) =
      MLflowPredictAction(ActionId("predict"), input.id, mlflow.id, DataObjectId("tgt"),
        modelName = modelName, modelAlias = alias, modelVersion = version, modelUri = uri)
    assert(action(uri = Some("runs:/abc/model"), modelName = Some("m"), alias = Some("champion")).getModelUri == "runs:/abc/model")
    assert(action(modelName = Some("m"), alias = Some("champion")).getModelUri == "models:/m@champion")
    assert(action(modelName = Some("m"), version = Some("3")).getModelUri == "models:/m/3")
    // fall back to the model of the run reported by a train action in the same job
    mlflow.parameterNotification(Map(MLflowRunInfo.ModelUri -> "runs:/run-1/model"), Seq())
    assert(action().getModelUri == "runs:/run-1/model")
  }

  test("MLflowPredictAction adds the prediction column in init phase without loading the model") {
    val mlflow = registerMlflow()
    val input = registerInput()
    val output = MockSparkDataObject(DataObjectId("tgt"))
    instanceRegistry.register(output)
    val action = MLflowPredictAction(ActionId("predict"), input.id, mlflow.id, output.id,
      modelName = Some("m"), modelAlias = Some("champion"), predictionColumn = "price_pred")
    val initContext = context.copy(phase = ExecutionPhase.Init)
    val result = action.init(Seq(InitSubFeed(input.id, Seq()), InitSubFeed(mlflow.id, Seq())))(initContext)
    assert(result.size == 1)
    val subFeed = result.head.asInstanceOf[SparkSubFeed]
    assert(subFeed.dataObjectId == output.id)
    val schema = subFeed.dataFrame.get.inner.schema
    assert(schema.fieldNames.toSeq == Seq("nights", "reviews_per_month", "price_pred"))
    assert(schema("price_pred").dataType.typeName == "double")
  }

  test("MLflowPredictAction fails early on an unknown feature column") {
    val mlflow = registerMlflow()
    val input = registerInput()
    val output = MockSparkDataObject(DataObjectId("tgt"))
    instanceRegistry.register(output)
    val action = MLflowPredictAction(ActionId("predict"), input.id, mlflow.id, output.id,
      modelName = Some("m"), modelAlias = Some("champion"), featureColumns = Some(Seq("does_not_exist")))
    val initContext = context.copy(phase = ExecutionPhase.Init)
    val ex = intercept[AssertionError](action.init(Seq(InitSubFeed(input.id, Seq()), InitSubFeed(mlflow.id, Seq())))(initContext))
    assert(ex.getMessage.contains("does_not_exist"))
  }
}
