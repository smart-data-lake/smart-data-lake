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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Condition
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.workflow.action.{Action, ActionMetadata}
import io.smartdatalake.workflow.dataobject.spark.CanCreateSparkDataFrame
import io.smartdatalake.workflow.dataobject.{DataObject, MLflowDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ParameterSubFeed, SubFeed}
import org.apache.hadoop.conf.Configuration

/**
 * [[Action]] to train a machine learning model with python code and track it with MLflow.
 *
 * The training data is read from `inputId`, and the python model code gets it as a PySpark DataFrame in the variable
 * `df`. The code is executed inside an MLflow run with `mlflow.autolog()` enabled, so for most frameworks the
 * parameters, metrics and the model itself are logged without any further code.
 *
 * The information about the run is passed on to the [[MLflowDataObject]] and to subsequent Actions as
 * [[ParameterSubFeed]] parameters, see [[io.smartdatalake.util.mlflow.MLflowRunInfo]].
 *
 * Nothing is executed in Init phase, so a dry run creates no MLflow runs.
 *
 * Note that this needs a python environment with `mlflow` installed, and MLflow version 2.9 or newer.
 *
 * Example:
 * {{{
 * actions {
 *   train-price-model {
 *     type = MLflowTrainAction
 *     inputId = int-listings
 *     mlflowId = mlflow-price-model
 *     modelName = "price-regressor"
 *     registerModel = true
 *     modelAlias = "champion"
 *     pythonModelCode = """
 *       from sklearn.linear_model import LinearRegression
 *       pdf = df.toPandas()
 *       LinearRegression().fit(pdf[["reviews_per_month"]], pdf["price"])
 *     """
 *   }
 * }
 * }}}
 *
 * @param inputId          id of the DataObject with the training data. It must be able to create a Spark DataFrame.
 * @param mlflowId         id of the [[MLflowDataObject]] holding the connection and the experiment
 * @param modelName        name of the model, used in the run information and in the MLflow model registry
 * @param pythonModelCode  the model training code as python. Either this or `pythonModelFile` must be defined.
 * @param pythonModelFile  file with the model training code as python. Either this or `pythonModelCode` must be defined.
 * @param modelDescription optional description of the model in the MLflow model registry
 * @param registerModel    if the trained model should be registered in the MLflow model registry. Default is false.
 * @param modelAlias       optional alias to set on the newly registered model version, e.g. `champion`.
 *                         Requires `registerModel = true`. Note that MLflow model stages are not supported, as they
 *                         are deprecated since MLflow 2.9 and removed in MLflow 3.
 * @param options          additional options passed to the python code as `options` dict
 */
case class MLflowTrainAction(override val id: ActionId,
                             inputId: DataObjectId,
                             mlflowId: DataObjectId,
                             modelName: String,
                             pythonModelCode: Option[String] = None,
                             pythonModelFile: Option[String] = None,
                             modelDescription: Option[String] = None,
                             registerModel: Boolean = false,
                             modelAlias: Option[String] = None,
                             options: Map[String, String] = Map(),
                             override val executionCondition: Option[Condition] = None,
                             override val metadata: Option[ActionMetadata] = None
                            )(implicit val instanceRegistry: InstanceRegistry) extends MLflowActionImpl {

  private val input = getInputDataObject[DataObject with CanCreateSparkDataFrame](inputId)
  override val mlflow: MLflowDataObject = getOutputDataObject[MLflowDataObject](mlflowId)

  override val inputs: Seq[DataObject] = Seq(input)
  override val outputs: Seq[DataObject] = Seq(mlflow)

  private[mlflow] val modelCode: String = {
    implicit val defaultHadoopConf: Configuration = new Configuration()
    if (pythonModelCode.isDefined && pythonModelFile.isDefined) {
      throw ConfigurationException(s"($id) only one of pythonModelCode and pythonModelFile may be defined")
    }
    pythonModelFile.map(file => HdfsUtil.readHadoopFile(file))
      .orElse(pythonModelCode)
      .getOrElse(throw ConfigurationException(s"($id) the ML model must be provided either as pythonModelCode or pythonModelFile"))
  }

  if (modelAlias.isDefined && !registerModel) {
    throw ConfigurationException(s"($id) modelAlias needs registerModel = true, as an alias can only be set on a registered model version")
  }

  validateConfig()

  override protected def pythonOptions: Map[String, String] = super.pythonOptions ++ options ++ Map(
    "modelName" -> modelName,
    "registerModel" -> registerModel.toString
  ) ++ modelDescription.map("modelDescription" -> _) ++ modelAlias.map("modelAlias" -> _)

  /**
   * Nothing is trained in Init phase: a model has no schema that subsequent Actions could depend on, and a dry run
   * must not create MLflow runs.
   */
  override def init(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = withSkippedOutputsOnNoData {
    validateInputSubFeeds(subFeeds)
    Seq(ParameterSubFeed(None, mlflowId, Seq()))
  }

  override def exec(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = withSkippedOutputsOnNoData {
    validateInputSubFeeds(subFeeds)
    assert(!context.simulation, s"($id) MLflowTrainAction does not support simulation runs")
    val partitionValues = getPartitionValues(subFeeds, inputId)
    val trainDf = getInputDataFrame(subFeeds, input)
    logger.info(s"($id) training model $modelName in experiment ${mlflow.experimentName}")
    val runInfo = getPythonUtil.train(modelCode, trainDf)
    mlflow.parameterNotification(runInfo, partitionValues)
    Seq(ParameterSubFeed(Some(runInfo), mlflowId, partitionValues, metrics = Some(runInfo.toMap[String, Any])))
  }
}

object MLflowTrainAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MLflowTrainAction = {
    extract[MLflowTrainAction](config)
  }
}
