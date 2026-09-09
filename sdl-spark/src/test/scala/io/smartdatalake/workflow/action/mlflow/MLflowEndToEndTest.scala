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

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.util.mlflow.MLflowRunInfo
import io.smartdatalake.workflow.dataobject.MLflowDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, InitSubFeed}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.net.{HttpURLConnection, URI}
import scala.sys.process._
import scala.util.Try

/**
 * Trains a model and applies it, against a real MLflow instance.
 *
 * This test is skipped unless all of the following are available, as none of them is needed to build SDLB:
 * - a python interpreter with `mlflow` and `scikit-learn` installed
 * - a reachable MLflow tracking server
 *
 * To run it locally:
 * {{{
 *   pip install mlflow scikit-learn
 *   mlflow server --host 127.0.0.1 --port 5000
 *   mvn -B test -pl sdl-spark -Dsuites=io.smartdatalake.workflow.action.mlflow.MLflowEndToEndTest
 * }}}
 * Set the environment variable MLFLOW_TRACKING_URI to use a different server.
 */
class MLflowEndToEndTest extends AnyFunSuite {

  protected implicit val session: SparkSession = SparkTestUtil.session
  import session.implicits._

  private val trackingUri = sys.env.getOrElse("MLFLOW_TRACKING_URI", "http://localhost:5000")

  private def pythonCmd: Option[String] = Seq("python3", "python").find(cmd => Try(Seq(cmd, "--version").! == 0).getOrElse(false))

  private def hasPythonModules(cmd: String, modules: String*): Boolean =
    Try(Seq(cmd, "-c", modules.map("import " + _).mkString("; ")).! == 0).getOrElse(false)

  private def isTrackingServerReachable: Boolean = Try {
    val connection = URI.create(s"$trackingUri/health").toURL.openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(2000)
    connection.setReadTimeout(2000)
    try connection.getResponseCode == 200 finally connection.disconnect()
  }.getOrElse(false)

  test("train a model and apply it") {
    val python = pythonCmd
    assume(python.isDefined, "no Python interpreter found")
    assume(hasPythonModules(python.get, "mlflow", "sklearn"), "python modules mlflow and scikit-learn are needed")
    assume(isTrackingServerReachable, s"no MLflow tracking server reachable at $trackingUri")

    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext

    val experimentName = s"sdlb-test-${System.currentTimeMillis()}"
    val modelName = s"sdlb-test-model-${System.currentTimeMillis()}"
    val mlflow = MLflowDataObject(DataObjectId("mlflow-test"), experimentName = experimentName, trackingUri = trackingUri)
    val input = MockSparkDataObject(DataObjectId("src"))
    val output = MockSparkDataObject(DataObjectId("tgt"))
    Seq(mlflow, input, output).foreach(instanceRegistry.register)
    input.writeSparkDataFrame(Seq((1.0, 3.0), (2.0, 5.0), (3.0, 7.0), (4.0, 9.0)).toDF("x", "y"))

    val trainAction = MLflowTrainAction(ActionId("train"), input.id, mlflow.id, modelName,
      registerModel = true, modelAlias = Some("champion"),
      pythonModelCode = Some(
        """
          |from sklearn.linear_model import LinearRegression
          |pdf = df.toPandas()
          |LinearRegression().fit(pdf[["x"]], pdf["y"])
          |""".stripMargin))

    val execContext = context.copy(phase = ExecutionPhase.Exec)
    val trainResult = trainAction.exec(Seq(InitSubFeed(input.id, Seq())))(execContext)
    val runInfo = mlflow.getLastRunInfo.get
    assert(MLflowRunInfo.fields.forall(runInfo.contains))
    assert(runInfo(MLflowRunInfo.ExperimentName) == experimentName)
    assert(runInfo(MLflowRunInfo.ModelUri).nonEmpty)
    assert(trainResult.size == 1)

    val predictAction = MLflowPredictAction(ActionId("predict"), input.id, mlflow.id, output.id,
      modelName = Some(modelName), modelAlias = Some("champion"), featureColumns = Some(Seq("x")))
    predictAction.exec(Seq(InitSubFeed(input.id, Seq()), InitSubFeed(mlflow.id, Seq())))(execContext)

    val predictions = output.getSparkDataFrame()(execContext)
    assert(predictions.columns.contains("prediction"))
    // y = 2x + 1, so the model should predict close to that
    predictions.select("x", "prediction").collect().foreach { row =>
      val x = row.getDouble(0)
      val prediction = row.getDouble(1)
      assert(math.abs(prediction - (2 * x + 1)) < 0.5, s"unexpected prediction $prediction for x=$x")
    }
  }
}
