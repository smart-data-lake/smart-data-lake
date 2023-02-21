/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.config.{ FromConfigFactory, InstanceRegistry }
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.mlflow.tracking.MlflowClient
import com.typesafe.config.Config
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.mlflow.MLflowUtils.getOrCreateExperimentId
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe.Type

// TODO: model according to experiment definition
case class MLFlowExperiment(experimentName: String)

/**
 * [[DataObject]] for interaction with MLflow
 *
 * @param mlflowURI Uri of the MLflow server. Default is http://localhost:5000.
 * @param experimentName The name of the experiment stored in MLflow.
 */
case class MlFlowDataObject(
    override val id: DataObjectId,
    mlflowURI: String,
    experimentName: String,
    override val metadata: Option[DataObjectMetadata] = None
)(@transient implicit val instanceRegistry: InstanceRegistry)
    extends DataObject
    with CanCreateSparkDataFrame
    with SmartDataLakeLogger {

  // get MLflow client
  val mlflowClient = new MlflowClient(mlflowURI)

  // get experiment or create it
  val experimentId = getOrCreateExperimentId(mlflowClient, experimentName)

  override def getSparkDataFrame(
      partitionValues: Seq[PartitionValues] = Seq()
  )(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session = context.sparkSession
    import session.implicits._

    // TODO: fetch latest run information and return as DataFrame
    val df: DataFrame = Seq.empty[MLFlowExperiment].toDF()
    // return
    df
  }

  override def factory: FromConfigFactory[DataObject] = MlFlowDataObject

}

object MlFlowDataObject extends FromConfigFactory[DataObject] with SmartDataLakeLogger {
  override def fromConfig(
      config: Config
  )(implicit instanceRegistry: InstanceRegistry): MlFlowDataObject = {
    extract[MlFlowDataObject](config)
  }
}
