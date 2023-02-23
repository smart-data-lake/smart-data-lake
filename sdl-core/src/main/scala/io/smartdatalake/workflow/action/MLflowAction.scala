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
import io.smartdatalake.util.spark.PythonSparkEntryPoint
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{ GenericDfsTransformer, GenericDfsTransformerDef }
import io.smartdatalake.workflow.action.spark.transformer.PythonCodeDfsTransformer
import io.smartdatalake.workflow.dataobject.{ CanCreateDataFrame, CanWriteDataFrame, DataObject, MLflowDataObject }
import io.smartdatalake.workflow.{ ActionPipelineContext, DataFrameSubFeed, ExecutionPhase }
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{ DataFrame, SparkSession }

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.{ Type, typeOf }
import java.sql.Timestamp

//
case class MLflowRunInformation(
                                 experimentId: String,
                                 experimentName: String,
                                 runId: String,
                                 runName: String,
                                 created: Timestamp
                               )

object ModeType extends Enumeration {
  type ModeType = Value
  val Train = Value("train")
  val Predict = Value("predict")
}

/**
 * This [[Action]] copies data between an input and output DataObject using DataFrames.
 * The input DataObject reads the data and converts it to a DataFrame according to its definition.
 * The DataFrame might be transformed using SQL or DataFrame transformations.
 * Then the output DataObjects writes the DataFrame to the output according to its definition.
 *
 * @param inputIds input DataObjects
 * @param outputIds output DataObjects
 * @param transformers optional list of transformations to apply. See [[spark.transformer]] for a list of included Transformers.
 *                     The transformations are applied according to the lists ordering.
 */
case class MLflowAction(
                         override val id: ActionId,
                         inputIds: Seq[DataObjectId],
                         outputIds: Seq[DataObjectId],
                         transformers: Seq[GenericDfsTransformer] = Seq(),
                         mode: String,
                         pythonModelCode: Option[String] = None,
                         pythonModelFile: Option[String] = None,
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
      .getOrElse(throw ConfigurationException(s"Either file or code must be defined for PythonCodeTransformer"))
  }

  // check mode
  ModeType.withName(mode.toLowerCase) match {
    case ModeType.Train   => logger.info(s"[$id] will be used to train a model provided the input data")
    case ModeType.Predict => logger.info(s"[$id] will be used to predict data points given the model")
    case _                => throw ConfigurationException(s"($id) mode is one of ${ModeType.ValueSet}")
  }

  // handle in- and outputs
  override val inputs: Seq[DataObject with CanCreateDataFrame] =
    inputIds.map(getInputDataObject[DataObject with CanCreateDataFrame])
  override val outputs: Seq[DataObject with CanWriteDataFrame] =
    outputIds.map(getOutputDataObject[DataObject with CanWriteDataFrame])

  // get MLflow DataObject
  private val getMLflowDataObject = () => {
    val mlflow = inputs.filter(_.isInstanceOf[io.smartdatalake.workflow.dataobject.MLflowDataObject])
    if (mlflow.size != 1) {
      throw ConfigurationException(
        s"($id) Too many or too few MLflowDataObjects are given as input. Exactly one must be provided."
      )
    } else {
      //return
      mlflow(0).asInstanceOf[io.smartdatalake.workflow.dataobject.MLflowDataObject]
    }
  }
  private val mlflow = getMLflowDataObject()

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
    println(transformerTypeStats)
    transformerTypeStats.map(_._1).headOption
  }

  validateConfig()

  private def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfsTransformerDef] = {
    // handle MLflow interaction

    // TODO: handle Init phase, to avoid handling this in the python code, we
    val pythonTrainPrefixBoilerPlate =
      s"""
         |import mlflow
         |mlflow.set_tracking_uri("${mlflow.trackingURI}")
         |# set experiment name
         |mlflow.set_experiment("${mlflow.experimentName}")
         |# turn on autolog
         |mlflow.autolog()
         |mlflow.start_run()
         |# start run and get status
         |run = mlflow.active_run()
         |print("run_id: {}; status: {}".format(run.info.run_id, run.info.status))
         |""".stripMargin

    val pythonTrainPostfixBoilerPlate =
      s"""
         |# end run and get status
         |mlflow.end_run()
         |run = mlflow.get_run(run.info.run_id)
         |print("run_id: {}; status: {}".format(run.info.run_id, run.info.status))
         |# run_info = mlflow.search_runs(filter_string=f'run_id = "{run.info.run_id}"').to_list()[0]
         |# create return dataframe
         |run_info = [{"experimentName": "${mlflow.experimentName}", "runId": f"{run.info.run_id}", "runName": f"{run.info.run_name}"}]
         |run_info_df = session.createDataFrame(run_info)
         |# prepare SDL return
         |outDfs={}
         |outDfs["run_info_iris"] = run_info_df
         |setOutputDfs(outDfs)
         |""".stripMargin

    val trainTransformer = PythonCodeDfsTransformer(
      name = "modelTransformer",
      description = Some("trains a sklearn model on the provided data"),
      code = Some(pythonTrainPrefixBoilerPlate + pythonCode + pythonTrainPostfixBoilerPlate)
    )

    //return
    transformerDefs :+ trainTransformer
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

  override def factory: FromConfigFactory[Action] = MLflowAction
}

object MLflowAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MLflowAction = {
    extract[MLflowAction](config)
  }
}
