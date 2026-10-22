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
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.mlflow.MLflowRunInfo
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, GenericDfTransformerDef}
import io.smartdatalake.workflow.action.{Action, ActionMetadata, DataFrameOneToOneActionImpl}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.expectation.ActionExpectation
import io.smartdatalake.workflow.dataobject.spark.{CanCreateSparkDataFrame, CanWriteSparkDataFrame}
import io.smartdatalake.workflow.dataobject.{DataObject, MLflowDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

/**
 * [[Action]] to apply a machine learning model tracked by MLflow to a DataFrame.
 *
 * The model is loaded with `mlflow.pyfunc.spark_udf` and applied to the DataFrame of `inputId`, adding the
 * prediction as an additional column. The result is written to `outputId`. Configured `transformers` are applied to
 * the input before the model.
 *
 * The [[MLflowDataObject]] of `inputMlflowId` is an additional input of this DataFrame Action, see
 * [[io.smartdatalake.workflow.action.DataFrameActionImpl.additionalInputs]]: it delivers no DataFrame, but it holds
 * the connection to MLflow and creates the dependency on a preceding [[MLflowTrainAction]] in the DAG.
 *
 * The model is addressed by an alias or a version of the registered model `modelName`, or by an explicit `modelUri`.
 * If none of these is given, the model of the latest run of the experiment is used, which is the model just trained
 * by an [[MLflowTrainAction]] in the same job.
 *
 * The model is not loaded in Init phase, so a dry run neither contacts MLflow nor downloads model artifacts.
 *
 * Note that this needs a python environment with `mlflow` installed, and MLflow version 2.9 or newer.
 *
 * Example:
 * {{{
 * actions {
 *   predict-price {
 *     type = MLflowPredictAction
 *     inputId = int-listings
 *     inputMlflowId = mlflow-price-model
 *     outputId = int-listings-predicted
 *     modelName = "price-regressor"
 *     modelAlias = "champion"
 *     featureColumns = [reviews_per_month, minimum_nights]
 *   }
 * }
 * }}}
 *
 * @param inputId          id of the DataObject with the data to predict on. It must be able to create a Spark DataFrame.
 * @param inputMlflowId    id of the [[MLflowDataObject]] holding the connection and the experiment. The model of
 *                         its latest run is applied if no model is configured explicitly.
 * @param outputId         id of the DataObject the predictions are written to
 * @param modelName        name of the registered model in the MLflow model registry
 * @param modelAlias       optional alias of the model version to apply, resolved as "models:/{modelName}@{modelAlias}".
 *                         Note that MLflow model stages are not supported, as they are deprecated since MLflow 2.9
 *                         and removed in MLflow 3. Use aliases instead.
 * @param modelVersion     optional version of the model to apply, resolved as "models:/{modelName}/{modelVersion}"
 * @param modelUri         optional explicit model uri, overriding `modelAlias` and `modelVersion`
 * @param predictionColumn name of the column the prediction is added as. Default is `prediction`.
 * @param resultType       Spark data type of the prediction column. Default is `double`.
 * @param featureColumns   optional columns of the input passed to the model. Default is all columns of the input.
 * @param options          additional options passed to the python code as `options` dict
 * @param transformers     optional list of transformations to apply to the input before applying the model
 */
case class MLflowPredictAction(override val id: ActionId,
                               inputId: DataObjectId,
                               inputMlflowId: DataObjectId,
                               outputId: DataObjectId,
                               modelName: Option[String] = None,
                               modelAlias: Option[String] = None,
                               modelVersion: Option[String] = None,
                               modelUri: Option[String] = None,
                               predictionColumn: String = "prediction",
                               resultType: String = "double",
                               featureColumns: Option[Seq[String]] = None,
                               options: Map[String, String] = Map(),
                               transformers: Seq[GenericDfTransformer] = Seq(),
                               override val cacheInput: Boolean = false,
                               override val cacheOutput: Boolean = false,
                               override val executionMode: Option[ExecutionMode] = None,
                               override val executionCondition: Option[Condition] = None,
                               override val metricsFailCondition: Option[String] = None,
                               override val expectations: Seq[ActionExpectation] = Seq(),
                               override val saveModeOptions: Option[SaveModeOptions] = None,
                               override val metadata: Option[ActionMetadata] = None
                              )(implicit val instanceRegistry: InstanceRegistry) extends DataFrameOneToOneActionImpl with MLflowActionImpl {

  override val input: DataObject with CanCreateSparkDataFrame = getInputDataObject[DataObject with CanCreateSparkDataFrame](inputId)
  override val output: DataObject with CanWriteSparkDataFrame = getOutputDataObject[DataObject with CanWriteSparkDataFrame](outputId)
  override val mlflow: MLflowDataObject = getInputDataObject[MLflowDataObject](inputMlflowId)

  // The MLflow DataObject is an additional input: it delivers no DataFrame, but it creates the dependency on the
  // MLflowTrainAction in the DAG, as the DAG derives its edges from the inputs of an Action.
  override val additionalInputs: Seq[DataObject] = Seq(mlflow)

  if (modelAlias.isDefined && modelVersion.isDefined) {
    throw ConfigurationException(s"($id) only one of modelAlias and modelVersion may be defined")
  }
  if ((modelAlias.isDefined || modelVersion.isDefined) && modelName.isEmpty) {
    throw ConfigurationException(s"($id) modelName is needed to resolve modelAlias or modelVersion")
  }

  validateConfig()

  override def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfTransformerDef] = transformers

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    getTransformers.foreach(_.prepare(id))
  }

  override protected def pythonOptions: Map[String, String] =
    super.pythonOptions ++ options ++ modelName.map("modelName" -> _)

  /**
   * Resolve the uri of the model to apply.
   */
  private[mlflow] def getModelUri(implicit context: ActionPipelineContext): String = {
    modelUri
      .orElse(modelName.flatMap(name => modelAlias.map(alias => s"models:/$name@$alias")))
      .orElse(modelName.flatMap(name => modelVersion.map(version => s"models:/$name/$version")))
      .orElse(mlflow.getRunInfo.flatMap(_.get(MLflowRunInfo.ModelUri)).filter(_.nonEmpty))
      .getOrElse(throw ConfigurationException(s"($id) could not determine the model to apply. Configure modelUri, or " +
        s"modelName with modelAlias or modelVersion, or make sure the experiment ${mlflow.experimentName} has a run with a logged model."))
  }

  private def getFeatureColumns(df: DataFrame): Seq[String] = {
    val columns = featureColumns.getOrElse(df.columns.toSeq)
    val missing = columns.diff(df.columns.toSeq)
    assert(missing.isEmpty, s"($id) featureColumns ${missing.mkString(", ")} do not exist in ${input.id}. " +
      s"Available columns are ${df.columns.mkString(", ")}.")
    columns
  }

  /**
   * Apply the configured transformers and then the model.
   *
   * The model is only loaded in Exec phase. In Init phase the prediction column is added as a null column of
   * `resultType`, which gives subsequent Actions and the output DataObject the correct schema, so a dry run
   * neither contacts MLflow nor downloads a model artifact.
   */
  override def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)
                        (implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val transformedSubFeed = applyTransformers(getTransformers, inputSubFeed, outputSubFeed)
    val df = getSparkDataFrame(transformedSubFeed)
    val columns = getFeatureColumns(df) // fail early on a wrong column name, in both phases
    val predictedDf = if (context.isExecPhase) {
      assert(!context.simulation, s"($id) does not support simulation runs")
      val uri = getModelUri
      logger.info(s"($id) applying model $uri to ${input.id}")
      getPythonUtil.predict(df, uri, columns, predictionColumn, resultType)
    } else {
      df.withColumn(predictionColumn, lit(null).cast(resultType))
    }
    transformedSubFeed.withDataFrame(Some(SparkDataFrame(predictedDf)))
  }

  override def transformPartitionValues(partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String, String])
                                       (implicit context: ActionPipelineContext): Map[PartitionValues, PartitionValues] =
    applyTransformers(getTransformers, partitionValues, executionModeResultOptions)
}

object MLflowPredictAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MLflowPredictAction = {
    extract[MLflowPredictAction](config)
  }
}
