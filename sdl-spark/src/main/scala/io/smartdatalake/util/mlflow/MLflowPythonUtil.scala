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
package io.smartdatalake.util.mlflow

import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.{PythonSparkEntryPoint, PythonTransformationException, PythonUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters._

/**
 * Talks to MLflow by executing python code in the Spark session, see [[MLflowPythonCode]].
 *
 * There is no MLflow client for the JVM involved: everything goes through the py4j gateway of
 * [[PythonUtil.execPythonSparkCode]], which needs a python environment with `mlflow` installed.
 *
 * @param id id of the Action or DataObject using this, used for error messages
 * @param session Spark session to run the python code in
 * @param options configuration passed to the python code as `options` dict, see [[MLflowPythonCode]]
 */
private[smartdatalake] case class MLflowPythonUtil(id: ConfigObjectId, session: SparkSession, options: Map[String, String])
  extends SmartDataLakeLogger {

  /**
   * Get the id of the experiment, creating it if it does not exist yet.
   */
  def getOrCreateExperimentId(): String = {
    val entryPoint = exec(MLflowPythonCode.getOrCreateExperimentCode)
    entryPoint.getResults.getOrElse(MLflowRunInfo.ExperimentId,
      throw new IllegalStateException(s"($id) MLflow did not return an experiment id for experiment ${options.getOrElse("experimentName", "")}"))
  }

  /**
   * Execute the given python model code within an MLflow run, with the training DataFrame available as `df`.
   *
   * @return the information about the run, see [[MLflowRunInfo]]
   */
  def train(pythonModelCode: String, trainDf: DataFrame, additionalOptions: Map[String, String] = Map()): Map[String, String] = {
    val code = MLflowPythonCode.trainPreludeCode + PythonUtil.dedent(pythonModelCode) +
      System.lineSeparator() + MLflowPythonCode.trainPostludeCode
    val entryPoint = exec(code, Some(trainDf), additionalOptions)
    val runInfo = entryPoint.getResults
    if (runInfo.isEmpty) throw new IllegalStateException(s"($id) MLflow training did not return any run information")
    runInfo
  }

  /**
   * Apply an MLflow model to the given DataFrame, adding the prediction as an additional column.
   */
  def predict(inputDf: DataFrame, modelUri: String, featureColumns: Seq[String], predictionColumn: String, resultType: String): DataFrame = {
    val predictOptions = Map(
      "modelUri" -> modelUri,
      "featureColumns" -> featureColumns.mkString(","),
      "predictionColumn" -> predictionColumn,
      "resultType" -> resultType
    )
    val entryPoint = exec(MLflowPythonCode.predictCode, Some(inputDf), predictOptions)
    entryPoint.outputDf.getOrElse(throw new IllegalStateException(s"($id) MLflow prediction did not return a DataFrame"))
  }

  /**
   * Get the information about the latest run of the experiment, if there is one.
   */
  def getLatestRunInfo(): Option[Map[String, String]] = {
    val entryPoint = exec(MLflowPythonCode.getLatestRunInfoCode)
    Some(entryPoint.getResults).filter(_.nonEmpty)
  }

  private def exec(code: String, inputDf: Option[DataFrame] = None, additionalOptions: Map[String, String] = Map()): MLflowPythonSparkEntryPoint = {
    val entryPoint = new MLflowPythonSparkEntryPoint(session, options ++ additionalOptions, inputDf)
    try {
      PythonUtil.execPythonSparkCode(entryPoint, MLflowPythonCode.preludeCode + code)
    } catch {
      case e: Throwable => throw new PythonTransformationException(s"($id) Could not execute MLflow python code. Error: ${e.getMessage}", e)
    }
    entryPoint
  }
}

/**
 * py4j entry point for [[MLflowPythonCode]]. Python pushes its results back through [[setResults]] and [[setOutputDf]].
 */
private[smartdatalake] class MLflowPythonSparkEntryPoint(override val session: SparkSession,
                                                         options: Map[String, String],
                                                         inputDf: Option[DataFrame] = None)
  extends PythonSparkEntryPoint(session, options) {

  // it seems that py4j needs getter functions for attributes
  def getInputDf: DataFrame = inputDf.getOrElse(throw new IllegalStateException("No input DataFrame was provided to the MLflow python code"))

  private var results: Map[String, String] = Map()
  def setResults(r: java.util.Map[String, String]): Unit = results = r.asScala.toMap
  def getResults: Map[String, String] = results

  private var outputDfInternal: Option[DataFrame] = None
  def setOutputDf(df: DataFrame): Unit = outputDfInternal = Some(df)
  def outputDf: Option[DataFrame] = outputDfInternal
}
