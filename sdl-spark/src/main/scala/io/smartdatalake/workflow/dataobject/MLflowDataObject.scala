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
package io.smartdatalake.workflow.dataobject

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.mlflow.{MLflowPythonUtil, MLflowRunInfo}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.generic.CanReceiveParameterNotification

/**
 * [[DataObject]] representing an experiment of an MLflow instance.
 *
 * It holds the information needed to connect to MLflow and is used
 * - as output of [[io.smartdatalake.workflow.action.mlflow.MLflowTrainAction]], where it receives the information
 *   about the training run as [[io.smartdatalake.workflow.ParameterSubFeed]] parameters, see [[MLflowRunInfo]],
 * - as input of [[io.smartdatalake.workflow.action.mlflow.MLflowPredictAction]], where it provides the connection
 *   and the model of the latest training run.
 *
 * No data is read or written by this DataObject, and it does not create a DataFrame. The model itself is stored by
 * MLflow, not by SDLB.
 *
 * Note that the model is configured on the Actions, not here: one experiment can produce several models.
 *
 * Example:
 * {{{
 * dataObjects {
 *   mlflow-price-model {
 *     type = MLflowDataObject
 *     trackingUri = "http://localhost:5000"
 *     experimentName = "price-prediction"
 *   }
 * }
 * }}}
 *
 * @param trackingUri     Uri of the MLflow tracking server. Default is http://localhost:5000.
 * @param registryUri     Optional Uri of the MLflow model registry, if it is not the tracking server.
 * @param experimentName  Name of the experiment in MLflow. It is created if it does not exist yet.
 * @param envManager      How MLflow restores the model's environment when applying it, one of `local`, `virtualenv`
 *                        or `conda`. With `local` the environment running SDLB must already satisfy the model's
 *                        requirements, with the other two MLflow builds an environment on every executor.
 *                        Default is `local`.
 */
case class MLflowDataObject(override val id: DataObjectId,
                            experimentName: String,
                            trackingUri: String = "http://localhost:5000",
                            registryUri: Option[String] = None,
                            envManager: String = "local",
                            override val metadata: Option[DataObjectMetadata] = None
                           )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanReceiveParameterNotification with SmartDataLakeLogger {

  private val allowedEnvManagers = Seq("conda", "virtualenv", "local")
  if (!allowedEnvManagers.contains(envManager)) {
    throw ConfigurationException(s"($id) envManager must be one of (${allowedEnvManagers.mkString(", ")})")
  }

  /**
   * Information about the run reported by an MLflowTrainAction in this job, if there was one.
   * It is not persisted: MLflow itself is the durable store, see [[getRunInfo]].
   */
  @transient private var lastRunInfo: Option[Map[String, String]] = None

  def getLastRunInfo: Option[Map[String, String]] = lastRunInfo

  override def parameterNotification(parameters: Map[String, String], partitionValues: Seq[PartitionValues])
                                    (implicit context: ActionPipelineContext): Unit = {
    lastRunInfo = Some(parameters)
    logger.info(s"($id) MLflow run ${parameters.getOrElse(MLflowRunInfo.RunId, "?")} of experiment $experimentName " +
      s"recorded, modelUri=${parameters.getOrElse(MLflowRunInfo.ModelUri, "?")}")
  }

  /**
   * Information about the latest run of the experiment: the one reported in this job if there was one, otherwise the
   * latest run known to MLflow.
   */
  def getRunInfo(implicit context: ActionPipelineContext): Option[Map[String, String]] = {
    lastRunInfo.orElse(getPythonUtil.getLatestRunInfo())
  }

  /**
   * Options passed to the generated python code, see [[io.smartdatalake.util.mlflow.MLflowPythonCode]].
   */
  private[smartdatalake] def pythonOptions: Map[String, String] = Map(
    "trackingUri" -> trackingUri,
    "experimentName" -> experimentName,
    "envManager" -> envManager
  ) ++ registryUri.map("registryUri" -> _)

  private[smartdatalake] def getPythonUtil(implicit context: ActionPipelineContext): MLflowPythonUtil =
    MLflowPythonUtil(id, SparkSubFeed.getSparkSession, pythonOptions)
}

object MLflowDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MLflowDataObject = {
    extract[MLflowDataObject](config)
  }
}
