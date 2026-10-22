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
package io.smartdatalake.util.mlflow

/**
 * Keys of the information about an MLflow run, as passed on by
 * [[io.smartdatalake.workflow.action.mlflow.MLflowTrainAction]] through a
 * [[io.smartdatalake.workflow.ParameterSubFeed]].
 *
 * All values are Strings, as they are transported as key/values. `date` is the start of the run as an ISO-8601
 * timestamp in UTC, `duration` is the duration of the run in seconds.
 *
 * These keys are also used by the generated python code in [[MLflowPythonCode]], so keep both in sync.
 */
private[smartdatalake] object MLflowRunInfo {
  val ExperimentId = "experimentId"
  val ExperimentName = "experimentName"
  val ModelName = "modelName"
  val RunId = "runId"
  val RunName = "runName"
  val Duration = "duration"
  val Date = "date"
  val ArtifactPath = "artifactPath"
  val ModelUri = "modelUri"
  val EstimatorName = "estimatorName"

  val fields: Seq[String] = Seq(ExperimentId, ExperimentName, ModelName, RunId, RunName, Duration, Date, ArtifactPath, ModelUri, EstimatorName)
}
