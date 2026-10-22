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

import io.smartdatalake.util.mlflow.MLflowPythonUtil
import io.smartdatalake.workflow.action.DataFrameActionImpl
import io.smartdatalake.workflow.action.executionMode.DataFrameStreamingExecutionMode
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.MLflowDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Common implementation of the MLflow Actions.
 *
 * The [[MLflowDataObject]] is connected as an additional input or output of a DataFrame Action, see
 * [[DataFrameActionImpl.additionalInputs]] and [[DataFrameActionImpl.additionalOutputs]]: it transports no
 * DataFrame, but the key/values about the MLflow run as a [[io.smartdatalake.workflow.ParameterSubFeed]].
 * Everything else - execution modes, expectations, save mode options, DataFrame caching, transformers - is
 * inherited from the DataFrame Action implementation.
 *
 * These Actions run on the classic Spark engine only: MLflow is driven through python, which needs a Spark session
 * inside the SDLB process, so neither Spark Connect nor Snowpark can be used.
 */
private[smartdatalake] trait MLflowActionImpl extends DataFrameActionImpl {

  /**
   * The MLflow DataObject holding the connection information.
   */
  def mlflow: MLflowDataObject

  // `mlflow.pyfunc.spark_udf` and the python model code need a Spark session inside the SDLB process,
  // so the classic Spark engine is the only option, regardless of the configured transformers.
  override lazy val transformerSubFeedType: Option[Type] = Some(typeOf[SparkSubFeed])

  override def validateConfig(): Unit = {
    super.validateConfig()
    // the python model code and spark_udf work on a batch DataFrame
    assert(!executionMode.exists(_.isInstanceOf[DataFrameStreamingExecutionMode]),
      s"($id) streaming execution modes are not supported, as MLflow needs a batch DataFrame")
  }

  /**
   * Options passed to the generated python code, see [[io.smartdatalake.util.mlflow.MLflowPythonCode]].
   * Subclasses add their own.
   */
  protected def pythonOptions: Map[String, String] = mlflow.pythonOptions

  protected def getPythonUtil(implicit context: ActionPipelineContext): MLflowPythonUtil =
    MLflowPythonUtil(id, SparkSubFeed.getSparkSession, pythonOptions)

  /**
   * Get the Spark DataFrame of a SubFeed.
   */
  protected def getSparkDataFrame(subFeed: DataFrameSubFeed): DataFrame = subFeed.dataFrame match {
    case Some(dataFrame: SparkDataFrame) => dataFrame.inner
    case Some(dataFrame) => throw new IllegalStateException(s"($id) needs a Spark DataFrame, but ${subFeed.dataObjectId} delivered a ${dataFrame.subFeedType.typeSymbol.name}")
    case None => throw new IllegalStateException(s"($id) SubFeed of ${subFeed.dataObjectId} has no DataFrame")
  }
}
