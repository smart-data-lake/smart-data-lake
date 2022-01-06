/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.sparktransformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.{DefaultExpressionData, PythonSparkEntryPoint, PythonUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionHelper
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Configuration of a custom Spark-DataFrame transformation between one input and one output (1:1) as Python/PySpark code.
 * Note that this transformer needs a Python and PySpark environment installed.
 * PySpark session is initialize and available under variables `sc`, `session`, `sqlContext`.
 * Other variables available are
 * - `inputDf`: Input DataFrame
 * - `options`: Transformation options as Map[String,String]
 * - `dataObjectId`: Id of input dataObject as String
 * Output DataFrame must be set with `setOutputDf(df)`.
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param file           Optional file with python code to use for python transformation. The python code can use variables inputDf, dataObjectId and options. The transformed DataFrame has to be set with setOutputDf.
 * @param code           Optional python code to user for python transformation. The python code can use variables inputDf, dataObjectId and options. The transformed DataFrame has to be set with setOutputDf.
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class PythonCodeDfTransformer(override val name: String = "pythonTransform", override val description: Option[String] = None, code: Option[String] = None, file: Option[String] = None, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsDfTransformer {
  private val pythonCode = {
    implicit val defaultHadoopConf: Configuration = new Configuration()
    file.map(file => HdfsUtil.readHadoopFile(file))
      .orElse(code)
      .getOrElse(throw ConfigurationException(s"Either file or code must be defined for PythonCodeTransformer"))
  }
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId, options: Map[String, String])(implicit context: ActionPipelineContext): DataFrame = {
    // python transformation is executed by passing options and input/output DataFrame through entry point
    val objectId = ActionHelper.replaceSpecialCharactersWithUnderscore(dataObjectId.id)
    try {
      val entryPoint = new DfTransformerPythonSparkEntryPoint(context.sparkSession, options, df, objectId)
      val additionalInitCode =
        """
          |# prepare input parameters
          |inputDf = DataFrame(entryPoint.getInputDf(), sqlContext) # convert input dataframe to pyspark
          |dataObjectId = entryPoint.getDataObjectId()
          |# helper function to return output dataframe
          |def setOutputDf( df ):
          |    entryPoint.setOutputDf(df._jdf)
          """.stripMargin
      PythonUtil.execPythonSparkCode(entryPoint, additionalInitCode + sys.props("line.separator") + pythonCode)
      entryPoint.outputDf.getOrElse(throw new IllegalStateException(s"($actionId.transformers.$name) Python transformation must set output DataFrame (call setOutputDf(df))"))
    } catch {
      case e: Throwable => throw new PythonTransformationException(s"($actionId.transformers.$name) Could not execute Python code. Error: ${e.getMessage}", e)
    }
  }
  override def factory: FromConfigFactory[ParsableDfTransformer] = PythonCodeDfTransformer
}

object PythonCodeDfTransformer extends FromConfigFactory[ParsableDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): PythonCodeDfTransformer = {
    extract[PythonCodeDfTransformer](config)
  }
}


private[smartdatalake] class DfTransformerPythonSparkEntryPoint(override val session: SparkSession, options: Map[String,String], inputDf: DataFrame, dataObjectId: String, var outputDf: Option[DataFrame] = None) extends PythonSparkEntryPoint(session, options) {
  // it seems that py4j needs getter functions for attributes
  def getInputDf: DataFrame = inputDf
  def getDataObjectId: String = dataObjectId
  def setOutputDf(df: DataFrame): Unit = {
    outputDf = Some(df)
  }
}

/**
 * Exception is thrown if the Python transformation can not be executed correctly
 */
private[smartdatalake] class PythonTransformationException(msg: String, throwable: Throwable) extends RuntimeException(msg, throwable)
