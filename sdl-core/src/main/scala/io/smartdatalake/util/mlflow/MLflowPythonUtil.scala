/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.util.mlflow

import io.smartdatalake.util.spark.{PythonSparkEntryPoint, PythonUtil}
import org.apache.spark.sql.SparkSession

case class LatestRunInfo(experimentId: String, runId: String, modelUri: String)

case class MLflowPythonUtil(entryPoint: MLflowPythonSparkEntryPoint, trackingURI: String) {
  val mlflowBoilerplate =
    s"""
       |import mlflow
       |import json
       |# config tracking server
       |mlflow.set_tracking_uri("${trackingURI}")
       |# mlflow client
       |client = mlflow.MlflowClient()
       |""".stripMargin

  def getOrCreateExperimentID(experimentName: String): Option[String] = {
    val createExperimentCode = s"""
                                  |# if experiment does not exist create it, otherwise return its ID
                                  |experiment = mlflow.get_experiment_by_name("$experimentName")
                                  |if experiment is not None:
                                  |    id = experiment.experiment_id
                                  |else:
                                  |    id = mlflow.create_experiment("${experimentName}")
                                  |entryPoint.setExperimentId(id)
                                  |""".stripMargin

    PythonUtil.execPythonSparkCode(entryPoint, mlflowBoilerplate + createExperimentCode)
    entryPoint.experimentId
  }

  def setLatest(experimentId: String): LatestRunInfo = {
    val getLatestRunIdCode =
      s"""
         |from mlflow.entities import ViewType
         |runs = mlflow.search_runs(experiment_ids="$experimentId", filter_string="", run_view_type=ViewType.ACTIVE_ONLY,max_results=1, order_by=["start_time DESC"], output_format="list")
         |if runs:
         |  run = runs[0]
         |  model_history_list = json.loads(run.data.tags["mlflow.log-model.history"])
         |  artifact_path = model_history_list[0]["artifact_path"]
         |  model_uri = f"runs:/{run.info.run_id}/{artifact_path}"
         |  run_id = run.info.run_id
         |  entryPoint.setLatestModelUri(model_uri)
         |  entryPoint.setLatestRunId(run_id)
         |""".stripMargin
    PythonUtil.execPythonSparkCode(entryPoint, mlflowBoilerplate + getLatestRunIdCode)

    // return
    LatestRunInfo(
      experimentId = entryPoint.experimentId.getOrElse(""),
      runId = entryPoint.latestRunId.getOrElse(""),
      modelUri = entryPoint.modelUri.getOrElse("")
    )
  }

  def registerModel(modelUri: String, modelName: String, description: String = "") = {
    val registerModelCode =
      s"""
         |model = client.search_registered_models(filter_string = "name = '$modelName'")
         |if len(model) == 0:
         |  registry_details = client.create_registered_model(name="$modelName", description="$description")
         |  print(registry_details)
         |model_details = mlflow.register_model(model_uri="$modelUri", name="$modelName")
         |print(model_details)
         |
         |""".stripMargin
    PythonUtil.execPythonSparkCode(entryPoint, mlflowBoilerplate + registerModelCode)
  }

  def transitionModel(modelName: String, version: String, fromStage: String, toStage: String) = {
    val transitionModelCode =
      s"""
         |version = "$version"
         |# get latest version if necessary
         |if version == "latest":
         |  model = client.get_latest_versions(name="$modelName", stages=["$fromStage"])[0]
         |  version = model.version
         |  print(model)
         |
         |# transition model accordingly
         |new_model = client.transition_model_version_stage(name="$modelName", version=version, stage="$toStage")
         |print(new_model)
         |
         |""".stripMargin
    PythonUtil.execPythonSparkCode(entryPoint, mlflowBoilerplate + transitionModelCode)
  }

}

private[smartdatalake] class MLflowPythonSparkEntryPoint(
                                                          override val session: SparkSession,
                                                          options: Map[String, String],
                                                          var experimentId: Option[String] = None,
                                                          var latestRunId: Option[String] = None,
                                                          var modelUri: Option[String] = None
                                                        ) extends PythonSparkEntryPoint(session, options) {

  def setExperimentId(id: String): Unit = {
    experimentId = Some(id)
  }

  def setLatestRunId(id: String): Unit = {
    latestRunId = Some(id)
  }

  def setLatestModelUri(uri: String): Unit = {
    modelUri = Some(uri)
  }

}

/**
 * Exception is thrown if the Python transformation can not be executed correctly
 */
private[smartdatalake] class PythonTransformationException(msg: String, throwable: Throwable)
  extends RuntimeException(msg, throwable)
