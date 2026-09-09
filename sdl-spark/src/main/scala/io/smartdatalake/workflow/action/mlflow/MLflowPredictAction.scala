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
import io.smartdatalake.util.mlflow.MLflowRunInfo
import io.smartdatalake.workflow.action.{Action, ActionMetadata}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.spark.{CanCreateSparkDataFrame, CanWriteSparkDataFrame}
import io.smartdatalake.workflow.dataobject.{DataObject, MLflowDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SubFeed}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

/**
 * [[Action]] to apply a machine learning model tracked by MLflow to a DataFrame.
 *
 * The model is loaded with `mlflow.pyfunc.spark_udf` and applied to the DataFrame of `inputId`, adding the
 * prediction as an additional column. The result is written to `outputId`.
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
 *     mlflowId = mlflow-price-model
 *     outputId = int-listings-predicted
 *     modelName = "price-regressor"
 *     modelAlias = "champion"
 *     featureColumns = [reviews_per_month, minimum_nights]
 *   }
 * }
 * }}}
 *
 * @param inputId          id of the DataObject with the data to predict on. It must be able to create a Spark DataFrame.
 * @param mlflowId         id of the [[MLflowDataObject]] holding the connection and the experiment
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
 */
case class MLflowPredictAction(override val id: ActionId,
                               inputId: DataObjectId,
                               mlflowId: DataObjectId,
                               outputId: DataObjectId,
                               modelName: Option[String] = None,
                               modelAlias: Option[String] = None,
                               modelVersion: Option[String] = None,
                               modelUri: Option[String] = None,
                               predictionColumn: String = "prediction",
                               resultType: String = "double",
                               featureColumns: Option[Seq[String]] = None,
                               options: Map[String, String] = Map(),
                               override val executionCondition: Option[Condition] = None,
                               override val metadata: Option[ActionMetadata] = None
                              )(implicit val instanceRegistry: InstanceRegistry) extends MLflowActionImpl {

  private val input = getInputDataObject[DataObject with CanCreateSparkDataFrame](inputId)
  override val mlflow: MLflowDataObject = getInputDataObject[MLflowDataObject](mlflowId)
  private val output = getOutputDataObject[DataObject with CanWriteSparkDataFrame](outputId)

  // the MLflow DataObject has to be an input, as the DAG derives its edges from the inputs of an Action
  override val inputs: Seq[DataObject] = Seq(input, mlflow)
  override val outputs: Seq[DataObject] = Seq(output)

  if (modelAlias.isDefined && modelVersion.isDefined) {
    throw ConfigurationException(s"($id) only one of modelAlias and modelVersion may be defined")
  }
  if ((modelAlias.isDefined || modelVersion.isDefined) && modelName.isEmpty) {
    throw ConfigurationException(s"($id) modelName is needed to resolve modelAlias or modelVersion")
  }

  validateConfig()

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
   * In Init phase the model is not loaded. The output schema is the input schema plus the prediction column, which
   * is enough for subsequent Actions and lets the output DataObject prepare itself.
   */
  override def init(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = withSkippedOutputsOnNoData {
    validateInputSubFeeds(subFeeds)
    val partitionValues = getPartitionValues(subFeeds, inputId)
    val inputDf = getEmptyInputDataFrame(subFeeds)
    getFeatureColumns(inputDf) // fail early on a wrong column name
    val outputDf = inputDf.withColumn(predictionColumn, lit(null).cast(resultType))
    output.initSparkDataFrame(outputDf, partitionValues)
    Seq(SparkSubFeed(Some(SparkDataFrame(outputDf)), outputId, partitionValues))
  }

  override def exec(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): Seq[SubFeed] = withSkippedOutputsOnNoData {
    validateInputSubFeeds(subFeeds)
    assert(!context.simulation, s"($id) MLflowPredictAction does not support simulation runs")
    val partitionValues = getPartitionValues(subFeeds, inputId)
    val inputDf = getInputDataFrame(subFeeds, input)
    val uri = getModelUri
    logger.info(s"($id) applying model $uri to ${input.id}")
    val predictedDf = getPythonUtil.predict(inputDf, uri, getFeatureColumns(inputDf), predictionColumn, resultType)
    val metrics = output.writeSparkDataFrame(predictedDf, partitionValues)
    Seq(SparkSubFeed(None, outputId, partitionValues, metrics = Some(metrics)))
  }

  /**
   * An empty DataFrame with the schema of the input, without reading any data.
   *
   * The schema is taken from the incoming SubFeed if the preceding Action propagated one, and from the DataObject
   * otherwise. This mirrors what DataFrameActionImpl does for DataFrame Actions in Init phase.
   */
  private def getEmptyInputDataFrame(subFeeds: Seq[SubFeed])(implicit context: ActionPipelineContext): DataFrame = {
    val inputSubFeed = subFeeds.find(_.dataObjectId == inputId).map(SparkSubFeed.fromSubFeed)
    val schema = inputSubFeed.flatMap(_.schemaOpt)
      .orElse(SparkSubFeed.getDeclaredDataObjectSchema(input))
    schema.map(s => SparkSubFeed.getEmptyDataFrame(s, inputId).asInstanceOf[SparkDataFrame].inner)
      .getOrElse(input.getSparkDataFrame(Seq()).filter(lit(false)))
  }
}

object MLflowPredictAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MLflowPredictAction = {
    extract[MLflowPredictAction](config)
  }
}
