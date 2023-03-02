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
import io.smartdatalake.config.{ ConfigurationException, FromConfigFactory, InstanceRegistry }
import io.smartdatalake.definitions.Condition
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{ GenericDfsTransformer, GenericDfsTransformerDef }
import io.smartdatalake.workflow.action.spark.transformer.PythonCodeDfsTransformer
import io.smartdatalake.workflow.dataobject.{
  CanCreateDataFrame,
  CanWriteDataFrame,
  DataObject,
  MLflowDataObject,
  MLflowException
}
import io.smartdatalake.workflow.{ ActionPipelineContext, DataFrameSubFeed, ExecutionPhase, SubFeed }
import org.apache.hadoop.conf.Configuration

import scala.reflect.runtime.universe.{ Type, typeOf }

/**
 * This [[Action]] copies data between an input and output DataObject using DataFrames.
 * The input DataObject reads the data and converts it to a DataFrame according to its definition.
 * The DataFrame might be transformed using SQL or DataFrame transformations.
 * Then the output DataObjects writes the DataFrame to the output according to its definition.
 *
 * @param mlflowId id of the MLflow DataObject
 * @param runInfoId id of an DataObject that implements the CanCreateDataFrame trait. Here the run information of the training is written down
 * @param inputIds input DataObjects
 * @param trainIdSelector id of the DataObject that will be used for training
 * @param transformers optional list of transformations to apply. See [[spark.transformer]] for a list of included Transformers.
 *                     The transformations are applied according to the lists ordering.
 * @param pythonModelCode machine learning model provided directly as python code
 * @param pythonModelFile machine learning model provided as python file
 * @param registerModel register model in MLflow registry
 */
case class MLflowTrainAction(
                              override val id: ActionId,
                              mlflowId: DataObjectId,
                              runInfoId: DataObjectId,
                              inputIds: Seq[DataObjectId],
                              trainIdSelector: Option[String] = None,
                              transformers: Seq[GenericDfsTransformer] = Seq(),
                              pythonModelCode: Option[String] = None,
                              pythonModelFile: Option[String] = None,
                              registerModel: Boolean = false,
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

  // get model code either from the file are directly from input
  private val pythonCode = {
    implicit val defaultHadoopConf: Configuration = new Configuration()
    pythonModelFile
      .map(file => HdfsUtil.readHadoopFile(file))
      .orElse(pythonModelCode)
      .getOrElse(throw ConfigurationException(s"ML model must either be provided as python file or code"))
  }

  // handle MLflow in and output data object
  val mlflow = getInputDataObject[MLflowDataObject](mlflowId)
  val mlflowRunInfo = getOutputDataObject[DataObject with CanWriteDataFrame with CanCreateDataFrame](runInfoId)

  // handle data in- and outputs
  override val inputs: Seq[DataObject with CanCreateDataFrame] =
    inputIds.map(getInputDataObject[DataObject with CanCreateDataFrame]) :+ mlflow

  override val outputs: Seq[DataObject with CanWriteDataFrame] = Seq(mlflowRunInfo)

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
    val runInfoSchemaPython =
      f"""
         |from pyspark.sql.types import StructField, StructType, MapType, StringType, IntegerType, FloatType, TimestampType
         |run_info_schema = StructType([StructField("experimentId", StringType(), True), StructField("experimentName", StringType(), True), StructField("runId",StringType(),True), StructField("runName",StringType(),True),StructField("duration", FloatType(), True),StructField("date", TimestampType(), True), StructField("artifactPath", StringType(), True), StructField("modelUri", StringType(), True),StructField("estimatorName", StringType(), True)])
         |""".stripMargin

    val getTrainingIdPython =
      f"""
         |import re
         |
         |class TrainingDataFrameError(Exception):
         |  pass
         |
         |def get_train_id(selector: str, ids: list[str]):
         |  if len(ids) > 1:
         |    # get DataObjectId according the given trainIdSelector
         |    r = re.compile(".*${trainIdSelector.getOrElse("train")}.*")
         |    possible_train_ids = list(filter(r.match, ids))
         |    if len(possible_train_ids) != 1:
         |      raise TrainingDataFrameError(f"Zero or multiple DataObjectIds detected for training {possible_train_ids}. One and only one DataObjectId must be provided for training. If only one Id is provided, this will be used. Otherwise the Id is selected using the case sensitive trainIdSelector (${trainIdSelector.get})")
         |    return possible_train_ids[0]
         |  elif len(ids) == 1:
         |    return ids[0]
         |  else:
         |    raise TrainingDataFrameError(f"Zero input DataObjectId provided {ids}.")
         |""".stripMargin

    // handle MLflow interaction in different phases
    val transformer = context.phase match {
      // return empty dataframe in Init phase
      case ExecutionPhase.Init =>
        val initPython =
          f"""
             |run_info_df = session.createDataFrame([], run_info_schema)
             |# check input
             |train_id = get_train_id("$trainIdSelector", list(inputDfs.keys()))
             |print(f"The following DataObjectId will be used for training: {train_id}")
             |# generate output dict
             |outDfs = {}
             |outDfs["${runInfoId.id}"] = run_info_df
             |setOutputDfs(outDfs)
             |""".stripMargin

        val initMlflowTransformer = PythonCodeDfsTransformer(
          name = "pythonInitTransformer",
          code = Some(runInfoSchemaPython + getTrainingIdPython + initPython)
        )
        transformerDefs :+ initMlflowTransformer

      case ExecutionPhase.Exec =>
        val pythonTrainPrefixBoilerPlate = runInfoSchemaPython + getTrainingIdPython +
          s"""
             |import mlflow
             |import json
             |mlflow.set_tracking_uri("${mlflow.trackingURI}")
             |# set experiment name
             |mlflow.set_experiment("${mlflow.experimentName}")
             |# turn on autolog
             |mlflow.autolog()
             |# start run and get status
             |mlflow.start_run()
             |run = mlflow.active_run()
             |print("run_id: {}; status: {}".format(run.info.run_id, run.info.status))
             |# get training dataframe from SDL
             |train_id = get_train_id("$trainIdSelector", list(inputDfs.keys()))
             |print(f"The following DataObjectId will be used for training: {train_id}")
             |df = inputDfs[f"{train_id}"]
             |""".stripMargin

        val pythonTrainPostfixBoilerPlate =
          s"""
             |# end run and get status
             |mlflow.end_run()
             |run = mlflow.get_run(run.info.run_id)
             |print("run_id: {}; status: {}".format(run.info.run_id, run.info.status))
             |#print(run)
             |# create return dataframe
             |from datetime import datetime
             |from zoneinfo import ZoneInfo
             |time_format = "%Y-%m-%d %H:%M:%S"
             |tz_info=ZoneInfo("Europe/Zurich")
             |date = datetime.fromtimestamp(int(run.info.start_time)/1000.0, tz=tz_info).strftime(time_format)
             |duration = (int(run.info.end_time)-int(run.info.start_time)) / 1000.0
             |model_history_list = json.loads(run.data.tags["mlflow.log-model.history"])
             |model_history = model_history_list[0]
             |artifact_path = model_history["artifact_path"]
             |estimator_name = run.data.tags["estimator_name"]
             |model_uri = f"runs:/{run.info.run_id}/{artifact_path}"
             |run_info = [{"experimentId": f"{run.info.experiment_id}","experimentName": "${mlflow.experimentName}", "runId": f"{run.info.run_id}", "runName": f"{run.info.run_name}", "duration": duration, "date": datetime.strptime(date,time_format), "artifactPath":f"{artifact_path}", "modelUri": f"{model_uri}","estimatorName":estimator_name}]
             |run_info_df = session.createDataFrame(run_info, run_info_schema)
             |run_info_df.show(5)
             |# generate output dict
             |outDfs={}
             |outDfs["${runInfoId.id}"] = run_info_df
             |setOutputDfs(outDfs)
             |""".stripMargin

        val trainTransformer = PythonCodeDfsTransformer(
          name = "modelTransformer",
          description = Some("trains a sklearn model on the provided data"),
          code = Some(pythonTrainPrefixBoilerPlate + pythonCode + pythonTrainPostfixBoilerPlate)
        )
        transformerDefs :+ trainTransformer
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

  override def postExec(inputSubFeeds: Seq[SubFeed], outputSubFeeds: Seq[SubFeed])(implicit
                                                                                   context: ActionPipelineContext
  ): Unit = {
    super.postExec(inputSubFeeds, outputSubFeeds)
    // TODO: check if this is only executed if there was no failure during EXEC phase
    if (registerModel) {
      // model has been logged during transform phase
      val latestRunInfo = mlflow.pythonMLflowApi.get.setLatest(
        mlflow.experimentId.getOrElse(throw MLflowException("No experimentId was set in the MLflowDataObject"))
      )
      // register Model does not wait until model is registered, so its not immediately visible within MLflow
      mlflow.pythonMLflowApi.get.registerModel(latestRunInfo.modelUri, mlflow.experimentName)
    }
  }

  override def factory: FromConfigFactory[Action] = MLflowTrainAction
}

object MLflowTrainAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MLflowTrainAction = {
    extract[MLflowTrainAction](config)
  }
}
