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

package io.smartdatalake.workflow.action

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ ActionId, DataObjectId }
import io.smartdatalake.config.{ FromConfigFactory, InstanceRegistry }
import io.smartdatalake.definitions.Condition
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{ GenericDfsTransformer, GenericDfsTransformerDef }
import io.smartdatalake.workflow.action.spark.transformer.PythonCodeDfsTransformer
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.{ ActionPipelineContext, DataFrameSubFeed, ExecutionPhase }

import scala.reflect.runtime.universe.{ Type, typeOf }

/**
 * @param mlflowId id of the MLflow DataObject
 * @param outputId id of an DataObject that contains the predictions
 * @param inputIds input DataObjects
 * @param predictIdSelector id of the DataObject that will be used for prediction
 * @param transformers optional list of transformations to apply. See [[spark.transformer]] for a list of included Transformers.
 *                     The transformations are applied according to the lists ordering.
 * @param resultType expected result type of the prediction
 */
case class MLflowPredictAction(
                                override val id: ActionId,
                                mlflowId: DataObjectId,
                                outputId: DataObjectId,
                                inputIds: Seq[DataObjectId],
                                predictIdSelector: Option[String] = None,
                                transformers: Seq[GenericDfsTransformer] = Seq(),
                                resultType: Option[String] = None,
                                override val breakDataFrameLineage: Boolean = false,
                                override val persist: Boolean = false,
                                override val mainInputId: Option[DataObjectId] = None,
                                override val mainOutputId: Option[DataObjectId] = None,
                                override val executionMode: Option[ExecutionMode] = None,
                                override val executionCondition: Option[Condition] = None,
                                override val metricsFailCondition: Option[String] = None,
                                override val metadata: Option[ActionMetadata] = None,
                              )(implicit instanceRegistry: InstanceRegistry)
  extends DataFrameActionImpl {

  // handle MLflow in and output data object
  val mlflow = getInputDataObject[MLflowDataObject](mlflowId)
  val prediction = getOutputDataObject[DataObject with CanWriteDataFrame with CanCreateDataFrame](outputId)

  // handle data in- and outputs
  override val inputs: Seq[DataObject with CanCreateDataFrame] =
    inputIds.map(getInputDataObject[DataObject with CanCreateDataFrame]) :+ mlflow

  override val outputs: Seq[DataObject with CanWriteDataFrame] = Seq(prediction)

  private val transformerDefs: Seq[GenericDfsTransformer] = transformers

  override val transformerSubFeedType: Option[Type] = {
    val transformerTypeStats = transformerDefs
      .map(_.getSubFeedSupportedType)
      .filterNot(_ =:= typeOf[DataFrameSubFeed]) // ignore generic transformers
      .groupBy(identity)
      .mapValues(_.size)
      .toSeq
      .sortBy(_._2)
    assert(
      transformerTypeStats.size <= 1,
      s"No common transformer subFeedType type found: ${transformerTypeStats
        .map { case (tpe, cnt) => s"${tpe.typeSymbol.name}: $cnt" }
        .mkString(",")}"
    )
    transformerTypeStats.map(_._1).headOption
  }

  validateConfig()

  private def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfsTransformerDef] = {
    mlflow.pythonMLflowApi.get.setLatest(mlflow.experimentId.get)

    val getPredictionIdPython =
      f"""
         |import re
         |
         |class TrainingDataFrameError(Exception):
         |  pass
         |
         |def get_predict_id(selector: str, ids: list[str]):
         |  if len(ids) > 1:
         |    # get DataObjectId according the given trainIdSelector
         |    r = re.compile(".*${predictIdSelector.getOrElse("predict")}.*")
         |    possible_predict_ids = list(filter(r.match, ids))
         |    if len(possible_predict_ids) != 1:
         |      raise TrainingDataFrameError(f"Zero or multiple DataObjectIds detected for prediction {possible_predict_ids}. One and only one DataObjectId must be provided for training. If only one Id is provided, this will be used. Otherwise the Id is selected using the case sensitive trainIdSelector (${predictIdSelector.get})")
         |    return possible_predict_ids[0]
         |  elif len(ids) == 1:
         |    return ids[0]
         |  else:
         |    raise TrainingDataFrameError(f"Zero input DataObjectId provided {ids}.")
         |""".stripMargin

    // handle MLflow interaction in different phases
    val transformer = context.phase match {
      // return empty dataframe in Init phase
      case ExecutionPhase.Init =>
        val initMakePredictionsPython =
          s"""
             |import mlflow
             |import json
             |from pyspark.sql.types import StringType
             |from pyspark.sql.functions import lit
             |
             |mlflow.set_tracking_uri("${mlflow.trackingURI}")
             |# set experiment name
             |mlflow.set_experiment("${mlflow.experimentName}")
             |# get predict dataframe from SDL
             |predict_id = get_predict_id("$predictIdSelector", list(inputDfs.keys()))
             |print(f"The following DataObjectId will be used for training: {predict_id}")
             |df = inputDfs[f"{predict_id}"]
             |# add predictions; TODO: get correct type according to input
             |df_predict = df.withColumn("predictions", lit(None).cast(StringType()))
             |df_predict.show()
             |# generate output dict
             |outDfs={}
             |outDfs["${outputId.id}"] = df_predict
             |setOutputDfs(outDfs)
             |""".stripMargin

        val initMlflowTransformer = PythonCodeDfsTransformer(
          name = "pythonInitTransformer",
          code = Some(getPredictionIdPython + initMakePredictionsPython)
        )
        transformerDefs :+ initMlflowTransformer

      case ExecutionPhase.Exec =>
        // TODO: change logic, only prediction needed
        val makePredictionPython =
          s"""
             |import mlflow
             |import json
             |from pyspark.sql.types import StringType
             |from pyspark.sql.functions import lit
             |
             |mlflow.set_tracking_uri("${mlflow.trackingURI}")
             |# set experiment name
             |mlflow.set_experiment("${mlflow.experimentName}")
             |# get predict dataframe from SDL
             |predict_id = get_predict_id("$predictIdSelector", list(inputDfs.keys()))
             |print(f"The following DataObjectId will be used for training: {predict_id}")
             |df_predict = inputDfs[f"{predict_id}"]
             |from pyspark.sql.functions import struct, col
             |# Load model as a Spark UDF. Override result_type if the model does not return double values.
             |print("${mlflow.pythonMLflowApi.get.entryPoint.modelUri}")
             |loaded_model = mlflow.pyfunc.spark_udf(session, model_uri="${mlflow.pythonMLflowApi.get.entryPoint.modelUri.get}", result_type="${resultType.getOrElse("string")}")
             |print(loaded_model)
             |# Predict on a Spark DataFrame.
             |df_final = df_predict.withColumn('predictions', loaded_model(struct(*map(col, df_predict.columns))))
             |df_final.show(5)
             |print(type(df_final))
             |# generate output dict
             |outDfs={}
             |outDfs["${outputId.id}"] = df_final
             |setOutputDfs(outDfs)
             |""".stripMargin

        val predictionTransformer = PythonCodeDfsTransformer(
          name = "modelTransformer",
          description = Some("applies a sklearn model on the provided data"),
          code = Some(getPredictionIdPython + makePredictionPython)
        )
        transformerDefs :+ predictionTransformer
    }

    // return
    transformer
  }

  override def transform(inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed])(implicit
                                                                                                      context: ActionPipelineContext
  ): Seq[DataFrameSubFeed] = {
    val partitionValues = getMainPartitionValues(inputSubFeeds)
    applyTransformers(getTransformers, partitionValues, inputSubFeeds, outputSubFeeds)
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    transformerDefs.foreach(_.prepare(id))
  }

  override def factory: FromConfigFactory[Action] = MLflowPredictAction
}

object MLflowPredictAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MLflowPredictAction = {
    extract[MLflowPredictAction](config)
  }
}
