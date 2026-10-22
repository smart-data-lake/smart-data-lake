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
import io.smartdatalake.definitions.{Condition, SaveModeOptions}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformer, GenericDfsTransformerDef}
import io.smartdatalake.workflow.action.{Action, ActionMetadata, DataFrameActionImpl}
import io.smartdatalake.workflow.dataobject.expectation.ActionExpectation
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame}
import io.smartdatalake.workflow.dataobject.spark.{CanCreateSparkDataFrame, CanWriteSparkDataFrame}
import io.smartdatalake.workflow.dataobject.{DataObject, MLflowDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ParameterSubFeed, SubFeed}
import org.apache.hadoop.conf.Configuration

/**
 * [[Action]] to train a machine learning model with python code and track it with MLflow.
 *
 * The training data is read from `inputId`, and the python model code gets it as a PySpark DataFrame in the variable
 * `df`. The code is executed inside an MLflow run with `mlflow.autolog()` enabled, so for most frameworks the
 * parameters, metrics and the model itself are logged without any further code.
 *
 * The [[MLflowDataObject]] of `outputMlflowId` is an additional output of this DataFrame Action, see
 * [[io.smartdatalake.workflow.action.DataFrameActionImpl.additionalOutputs]]: it receives the information about the
 * run as [[ParameterSubFeed]] parameters, see [[io.smartdatalake.util.mlflow.MLflowRunInfo]], which also makes a
 * subsequent [[MLflowPredictAction]] depend on this Action.
 *
 * `outputId` is optional. Configure it to write the training data on to a DataObject, which also allows preparing
 * the features with `transformers`. Without it this Action only trains a model.
 *
 * No model is trained in Init phase, so a dry run creates no MLflow runs.
 *
 * Note that this needs a python environment with `mlflow` installed, and MLflow version 2.9 or newer.
 *
 * Example:
 * {{{
 * actions {
 *   train-price-model {
 *     type = MLflowTrainAction
 *     inputId = int-listings
 *     outputMlflowId = mlflow-price-model
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
 * @param outputMlflowId   id of the [[MLflowDataObject]] holding the connection and the experiment. It receives the
 *                         information about the training run.
 * @param modelName        name of the model, used in the run information and in the MLflow model registry
 * @param outputId         optional id of a DataObject the training data is written to. Needed to use `transformers`.
 * @param pythonModelCode  the model training code as python. Either this or `pythonModelFile` must be defined.
 * @param pythonModelFile  file with the model training code as python. Either this or `pythonModelCode` must be defined.
 * @param modelDescription optional description of the model in the MLflow model registry
 * @param registerModel    if the trained model should be registered in the MLflow model registry. Default is false.
 * @param modelAlias       optional alias to set on the newly registered model version, e.g. `champion`.
 *                         Requires `registerModel = true`. Note that MLflow model stages are not supported, as they
 *                         are deprecated since MLflow 2.9 and removed in MLflow 3.
 * @param options          additional options passed to the python code as `options` dict
 * @param transformers      optional list of transformations to apply to the training data before training the model.
 *                          Needs `outputId`, as a transformer writes its result to an output DataObject.
 */
case class MLflowTrainAction(override val id: ActionId,
                             inputId: DataObjectId,
                             outputMlflowId: DataObjectId,
                             modelName: String,
                             outputId: Option[DataObjectId] = None,
                             pythonModelCode: Option[String] = None,
                             pythonModelFile: Option[String] = None,
                             modelDescription: Option[String] = None,
                             registerModel: Boolean = false,
                             modelAlias: Option[String] = None,
                             options: Map[String, String] = Map(),
                             transformers: Seq[GenericDfsTransformer] = Seq(),
                             override val cacheInput: Boolean = false,
                             override val cacheOutput: Boolean = false,
                             override val executionMode: Option[ExecutionMode] = None,
                             override val executionCondition: Option[Condition] = None,
                             override val metricsFailCondition: Option[String] = None,
                             override val expectations: Seq[ActionExpectation] = Seq(),
                             override val saveModeOptions: Option[SaveModeOptions] = None,
                             override val metadata: Option[ActionMetadata] = None
                            )(implicit val instanceRegistry: InstanceRegistry) extends DataFrameActionImpl with MLflowActionImpl {

  private val input = getInputDataObject[DataObject with CanCreateSparkDataFrame](inputId)
  private val output = outputId.map(getOutputDataObject[DataObject with CanWriteSparkDataFrame](_))
  override val mlflow: MLflowDataObject = getOutputDataObject[MLflowDataObject](outputMlflowId)

  override val dataFrameInputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  // the DataFrame output is optional: without it this Action only trains a model
  override val dataFrameOutputs: Seq[DataObject with CanWriteDataFrame] = output.toSeq
  // The MLflow DataObject is an additional output: it receives the information about the training run as
  // key/values instead of a DataFrame, and it creates the dependency for a subsequent MLflowPredictAction.
  override val additionalOutputs: Seq[MLflowDataObject] = Seq(mlflow)

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
  if (transformers.nonEmpty && outputId.isEmpty) {
    throw ConfigurationException(s"($id) transformers need outputId, as a transformer writes its result to an output DataObject")
  }

  validateConfig()

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    transformers.foreach(_.prepare(id))
  }

  override protected def pythonOptions: Map[String, String] = super.pythonOptions ++ options ++ Map(
    "modelName" -> modelName,
    "registerModel" -> registerModel.toString
  ) ++ modelDescription.map("modelDescription" -> _) ++ modelAlias.map("modelAlias" -> _)

  /**
   * The training data, after applying the configured transformers, is passed on to the DataFrame output if one is
   * configured. The model itself is trained in [[execAdditionalOutputSubFeeds]], on the same DataFrame.
   */
  override protected def transform(inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed])
                                  (implicit context: ActionPipelineContext): Seq[DataFrameSubFeed] = {
    if (outputSubFeeds.isEmpty) return outputSubFeeds // no DataFrame output configured, see dataFrameOutputs
    val transformedDfs = applyTransformers(transformers, inputSubFeeds.head.partitionValues, inputSubFeeds)
    outputSubFeeds.map { outputSubFeed =>
      // without transformers the training data is passed on unchanged, so the result is still the input DataFrame
      val dataFrame = transformedDfs.getOrElse(outputSubFeed.dataObjectId.id,
        if (transformers.isEmpty) inputSubFeeds.head.dataFrame
          .getOrElse(throw new IllegalStateException(s"($id) SubFeed of $inputId has no DataFrame"))
        else throw ConfigurationException(s"($id) No result found for output ${outputSubFeed.dataObjectId}. Available results are ${transformedDfs.keys.mkString(", ")}.")
      )
      outputSubFeed.withDataFrame(Some(dataFrame))
    }
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String, String])
                                       (implicit context: ActionPipelineContext): Map[PartitionValues, PartitionValues] =
    applyTransformers(transformers, partitionValues, executionModeResultOptions)

  /**
   * Nothing is trained in Init phase: a model has no schema that subsequent Actions could depend on, and a dry run
   * must not create MLflow runs.
   */
  override protected def initAdditionalOutputSubFeeds(implicit context: ActionPipelineContext): Seq[SubFeed] =
    Seq(ParameterSubFeed(None, outputMlflowId, Seq()))

  override protected def execAdditionalOutputSubFeeds(inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed])
                                                     (implicit context: ActionPipelineContext): Seq[SubFeed] = {
    assert(!context.simulation, s"($id) does not support simulation runs")
    val partitionValues = inputSubFeeds.head.partitionValues
    val trainDf = getSparkDataFrame(getTrainingSubFeed(inputSubFeeds, outputSubFeeds))
    logger.info(s"($id) training model $modelName in experiment ${mlflow.experimentName}")
    val runInfo = getPythonUtil.train(modelCode, trainDf)
    val subFeed = ParameterSubFeed(Some(runInfo), outputMlflowId, partitionValues, metrics = Some(runInfo.toMap[String, Any]))
    writeAdditionalOutputSubFeed(subFeed)
    Seq(subFeed)
  }

  /**
   * The DataFrame the model is trained on. If a DataFrame output is configured, this is the transformed DataFrame
   * prepared by [[transform]], so the transformers are applied exactly once. Otherwise it is the input DataFrame.
   */
  private def getTrainingSubFeed(inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed]): DataFrameSubFeed =
    outputSubFeeds.headOption.filter(_.dataFrame.isDefined).getOrElse(inputSubFeeds.head)
}

object MLflowTrainAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MLflowTrainAction = {
    extract[MLflowTrainAction](config)
  }
}
