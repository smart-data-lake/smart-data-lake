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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import com.typesafe.config.Config
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.mlflow.{MLflowPythonSparkEntryPoint, MLflowPythonUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.{DataFrame, SparkSession}

// TODO: model according to experiment definition
case class MLflowExperiment(experimentName: String)

case class ModelTransitionInfo(version: String, fromStage: String, toStage: String)

/**
 * [[DataObject]] for interaction with MLflow
 *
 * @param trackingURI Uri of the MLflow server. Default is http://localhost:5000.
 * @param experimentName The name of the experiment stored in MLflow.
 * @param modelName The name of the model stored in the MLflow model registry.
 * @param modelDescription The description displayed in the MLflow model registry.
 * @param modelTransitionInfo The information that must be provided if in the MLflowTrainAction a model transition is configured.
 */
case class MLflowDataObject(
                             override val id: DataObjectId,
                             trackingURI: String,
                             experimentName: String,
                             modelName: String = "Default",
                             modelDescription: Option[String] = None,
                             modelTransitionInfo : Option[ModelTransitionInfo] = None,
                             envManager: String = "conda",
                             override val metadata: Option[DataObjectMetadata] = None
                           )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject
    with CanCreateSparkDataFrame
    with SmartDataLakeLogger {


  var experimentId: Option[String] = None
  private val allowedEnvManager = Seq("conda","virtualenv","local")
  if (!allowedEnvManager.contains(envManager)){
    throw ConfigurationException(s"($id) envManager must be one of (${allowedEnvManager.mkString(",")})")
  }

  def getPythonMLflowClient(entryPoint: Option[MLflowPythonSparkEntryPoint], mlflowURI: String): Option[MLflowPythonUtil] = {
    var pythonMLflowClient: Option[MLflowPythonUtil] = None
    // create entry point
    if (entryPoint.isEmpty) {
      throw MLflowException("Creation of MLflowPythonSparkEntryPoint was not successful")
    } else {
      pythonMLflowClient = Some(MLflowPythonUtil(entryPoint.get, mlflowURI))
      experimentId = pythonMLflowClient
        .getOrElse(throw MLflowException("PythonUtil for MLflow not ready"))
        .getOrCreateExperimentID(experimentName)
      logger.info(s"$id: MLflow using experiment $experimentName (id=$experimentId)")
    }

    // return
    pythonMLflowClient

  }

  override def getSparkDataFrame(
                                  partitionValues: Seq[PartitionValues] = Seq()
                                )(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session = context.sparkSession
    import session.implicits._

    // TODO: fetch latest run information and return as DataFrame
    val df: DataFrame = Seq.empty[MLflowExperiment].toDF()
    // return
    df
  }

  override def factory: FromConfigFactory[DataObject] = MLflowDataObject

}

object MLflowDataObject extends FromConfigFactory[DataObject] with SmartDataLakeLogger {
  override def fromConfig(
                           config: Config
                         )(implicit instanceRegistry: InstanceRegistry): MLflowDataObject = {
    extract[MLflowDataObject](config)
  }
}

case class MLflowException(msg: String, cause: Throwable = null) extends Exception(msg, cause)
