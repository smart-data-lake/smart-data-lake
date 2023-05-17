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

package io.smartdatalake.workflow.action.spark.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.spark.{DefaultExpressionData, PythonSparkEntryPoint, PythonUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformer, OptionsSparkDfsTransformer}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
 * Configuration of a custom Spark-DataFrame transformation between many inputs and many outputs (n:m) as Python/PySpark code.
 * Note that this transformer needs a Python and PySpark environment installed.
 * PySpark session is initialize and available under variables `sc`, `session`, `sqlContext`.
 * Other variables available are
 * - `inputDfs`: Input DataFrames
 * - `options`: Transformation options as Map[String,String]
 * Output DataFrames must be set with `setOutputDfs(dict)`.
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param file           Optional file with python code to use for python transformation. The python code can use variables inputDfs and options. The transformed DataFrames has to be set with setOutputDfs.
 * @param code           Optional python code to user for python transformation. The python code can use variables inputDfs and options. The transformed DataFrame has to be set with setOutputDfs.
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class PythonCodeDfsTransformer(
    override val name: String = "pythonSparkTransform",
    override val description: Option[String] = None,
    code: Option[String] = None,
    file: Option[String] = None,
    options: Map[String, String] = Map(),
    runtimeOptions: Map[String, String] = Map()
) extends OptionsSparkDfsTransformer {
  private val pythonCode = {
    implicit val defaultHadoopConf: Configuration = new Configuration()
    file
      .map(file => HdfsUtil.readHadoopFile(file))
      .orElse(code)
      .getOrElse(throw ConfigurationException(s"Either file or code must be defined for PythonCodeDfsTransformer"))
  }

  override def transformSparkWithOptions(
      actionId: ActionId,
      partitionValues: Seq[PartitionValues],
      dfs: Map[String, DataFrame],
      options: Map[String, String]
  )(implicit context: ActionPipelineContext): Map[String, DataFrame] = {
    // python transformation is executed by passing options and input/output DataFrame through entry point
    try {
      val entryPoint = new DfsTransformerPythonSparkEntryPoint(context.sparkSession, options, dfs)
      val additionalInitCode =
        """
          |# prepare input parameters
          |inputDfs = {}
          |for k,v in entryPoint.getInputDfs().items():
          |    inputDfs[k] = DataFrame(v, sqlContext) # convert input dataframe to pyspark
          |# helper function to return output dataframe
          |outputDfs = gateway.jvm.java.util.HashMap()
          |def setOutputDfs(dict):
          |    for k in dict.keys():
          |        outputDfs[k] = dict[k]._jdf # convert output dataframe to java
          |    entryPoint.setOutputDfs(outputDfs)
          """.stripMargin
      PythonUtil.execPythonSparkCode(
        entryPoint,
        additionalInitCode + sys.props("line.separator") + pythonCode.stripMargin
      )
      entryPoint.outputDfs.getOrElse(
        throw new IllegalStateException(
          s"($actionId.transformers.$name) Python transformation must set output DataFrames (call setOutputDfs(df))"
        )
      )
    } catch {
      case e: Throwable =>
        throw new PythonTransformationException(
          s"($actionId.transformers.$name) Could not execute Python code. Error: ${e.getMessage}",
          e
        )
    }
  }
  override def factory: FromConfigFactory[GenericDfsTransformer] = PythonCodeDfsTransformer
}

object PythonCodeDfsTransformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): PythonCodeDfsTransformer = {
    extract[PythonCodeDfsTransformer](config)
  }
}

private[smartdatalake] class DfsTransformerPythonSparkEntryPoint(
    override val session: SparkSession,
    options: Map[String, String],
    inputDfs: Map[String, DataFrame],
    var outputDfs: Option[Map[String, DataFrame]] = None
) extends PythonSparkEntryPoint(session, options) {
  // it seems that py4j needs getter functions for attributes
  def getInputDfs: java.util.Map[String, DataFrame] = inputDfs.asJava
  def setOutputDfs(outDfs: java.util.Map[String, DataFrame]): Unit = {
    outputDfs = Some(outDfs.asScala.toMap)
  }
}