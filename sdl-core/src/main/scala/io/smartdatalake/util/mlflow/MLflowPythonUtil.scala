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

import io.smartdatalake.util.mlflow.ModelType.ModelType
import io.smartdatalake.util.spark.{ PythonSparkEntryPoint, PythonUtil }
import org.apache.spark.sql.SparkSession

object ModelType extends Enumeration {
  type ModelType = Value
  val Sklearn, Tensorflow = Value
}

case class MLflowPythonUtil(entryPoint: MLflowPythonSparkEntryPoint, trackingURI: String) {
  val mlflowBoilerplate =
    s"""
       |import mlflow
       |# config tracking server
       |mlflow.set_tracking_uri("${trackingURI}")
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

  def logSklearnModelBoilerplate(): String = {

    val logModelCode =
      s"""
         |# log model to MLflow
         |def log_sklearn_model(model: any, artifact_path: str,registered_model_name: str):
         |    model_info = mlflow.sklearn.log_model(
         |        sk_model=sk_model,
         |        artifact_path=artifact_path,
         |        registered_model_name=registered_model_name
         |    )
         |    entryPoint.setModelUri(modelmodel_uri)
         |""".stripMargin
    //return
    mlflowBoilerplate + logModelCode
  }

}

private[smartdatalake] class MLflowPythonSparkEntryPoint(
                                                          override val session: SparkSession,
                                                          options: Map[String, String],
                                                          var experimentId: Option[String] = None,
                                                          var modelUri: Option[String] = None
                                                        ) extends PythonSparkEntryPoint(session, options) {

  def setExperimentId(id: String): Unit = {
    experimentId = Some(id)
  }
  def setModelUri(id: String): Unit = {
    modelUri = Some(id)
  }
}

/**
 * Exception is thrown if the Python transformation can not be executed correctly
 */
private[smartdatalake] class PythonTransformationException(msg: String, throwable: Throwable)
  extends RuntimeException(msg, throwable)
