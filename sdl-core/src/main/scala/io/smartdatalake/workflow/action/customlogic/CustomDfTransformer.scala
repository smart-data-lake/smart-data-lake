/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action.customlogic

import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc._
import io.smartdatalake.util.spark.DefaultExpressionData
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig.fnTransformType
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformerDef, SQLDfTransformer}
import io.smartdatalake.workflow.action.spark.transformer.{PythonCodeDfTransformer, ScalaClassDfTransformer, ScalaCodeDfTransformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Interface to define a custom Spark-DataFrame transformation (1:1)
 */
trait CustomDfTransformer extends Serializable {

  /**
   * Function to be implemented to define the transformation between an input and output DataFrame (1:1)
   *
   * @param session Spark Session
   * @param options Options specified in the configuration for this transformation
   * @param df DataFrames to be transformed
   * @param dataObjectId Id of DataObject of SubFeed
   * @return Transformed DataFrame
   */
  def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String) : DataFrame

  /**
   * Optional function to define the transformation of input to output partition values.
   * For example this enables to implement aggregations where multiple input partitions are combined into one output partition.
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   *
   * @param partitionValues partition values to be transformed
   * @param options Options specified in the configuration for this transformation
   * @return Map of input to output partition values. This allows to map partition values forward and backward, which is needed in execution modes.
   *         Return None if mapping is 1:1.
   */
  def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues,PartitionValues]] = None
}

/**
 * Configuration of a custom Spark-DataFrame transformation between one input and one output (1:1)
 * Define a transform function which receives a DataObjectIds, a DataFrames and a map of options and has to return a
 * DataFrame, see also [[CustomDfTransformer]].
 *
 * Note about Python transformation: Environment with Python and PySpark needed.
 * PySpark session is initialize and available under variables `sc`, `session`, `sqlContext`.
 * Other variables available are
 * - `inputDf`: Input DataFrame
 * - `options`: Transformation options as Map[String,String]
 * - `dataObjectId`: Id of input dataObject as String
 * Output DataFrame must be set with `setOutputDf(df)`.
 *
 * @param className Optional class name implementing trait [[CustomDfTransformer]]
 * @param scalaFile Optional file where scala code for transformation is loaded from. The scala code in the file needs to be a function of type [[fnTransformType]].
 * @param scalaCode Optional scala code for transformation. The scala code needs to be a function of type [[fnTransformType]].
 * @param sqlCode Optional SQL code for transformation.
 *                Use tokens %{<key>} to replace with runtimeOptions in SQL code.
 *                Example: "select * from test where run = %{runId}"
 * @param pythonFile Optional pythonFile to use for python transformation. The python code can use variables inputDf, dataObjectId and options. The transformed DataFrame has to be set with setOutputDf.
 * @param pythonCode Optional pythonCode to use for python transformation. The python code can use variables inputDf, dataObjectId and options. The transformed DataFrame has to be set with setOutputDf.
 * @param options Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class CustomDfTransformerConfig( className: Option[String] = None, scalaFile: Option[String] = None, scalaCode: Option[String] = None, sqlCode: Option[String] = None, pythonFile: Option[String] = None, pythonCode: Option[String] = None, options: Option[Map[String,String]] = None, runtimeOptions: Option[Map[String,String]] = None) {
  require(className.isDefined || scalaFile.isDefined || scalaCode.isDefined || sqlCode.isDefined || pythonFile.isDefined || pythonCode.isDefined, "Either className, scalaFile, scalaCode, sqlCode, pythonFile or code must be defined for CustomDfTransformer")

  val impl: GenericDfTransformerDef = className.map(clazz => ScalaClassDfTransformer(className = clazz, options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map())))
  .orElse {
    scalaFile.map(file => ScalaCodeDfTransformer(file = Some(file), options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map())))
  }.orElse{
    scalaCode.map(code => ScalaCodeDfTransformer(code = Some(code), options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map())))
  }.orElse {
    sqlCode.map(code => SQLDfTransformer(code = code, options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map())))
  }.orElse {
    pythonFile.map(file => PythonCodeDfTransformer(file = Some(file), options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map())))
  }.orElse{
    pythonCode.map(code => PythonCodeDfTransformer(code = Some(code), options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map())))
  }.get

  override def toString: String = {
    if (className.isDefined)      "className: " +className.get
    else if(scalaFile.isDefined)  "scalaFile: " +scalaFile.get
    else if(scalaCode.isDefined)  "scalaCode: " +scalaCode.get
    else if(sqlCode.isDefined)    "sqlCode: "   +sqlCode.get
    else if(pythonCode.isDefined) "code: "+pythonCode.get
    else if(pythonFile.isDefined) "pythonFile: "+pythonFile.get
    else throw new IllegalStateException("transformation undefined!")
  }
}

object CustomDfTransformerConfig {
  type fnTransformType = (SparkSession, Map[String, String], DataFrame, String) => DataFrame
}
