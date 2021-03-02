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

import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.{CustomCodeUtil, DefaultExpressionData, PythonUtil, SparkExpressionUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionHelper
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig.fnTransformType
import org.apache.spark.python.PythonHelper.SparkEntryPoint
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
   */
  def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Map[PartitionValues,PartitionValues] = PartitionValues.oneToOneMapping(partitionValues)
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
 * @param pythonCode Optional pythonCode to user for python transformation. The python code can use variables inputDf, dataObjectId and options. The transformed DataFrame has to be set with setOutputDf.
 * @param options Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class CustomDfTransformerConfig( className: Option[String] = None, scalaFile: Option[String] = None, scalaCode: Option[String] = None, sqlCode: Option[String] = None, pythonFile: Option[String] = None, pythonCode: Option[String] = None, options: Map[String,String] = Map(), runtimeOptions: Map[String,String] = Map()) {
  require(className.isDefined || scalaFile.isDefined || scalaCode.isDefined || sqlCode.isDefined || pythonFile.isDefined || pythonCode.isDefined, "Either className, scalaFile, scalaCode, sqlCode, pythonFile or code must be defined for CustomDfTransformer")

  val impl : Option[CustomDfTransformer] = className.map {
    clazz => CustomCodeUtil.getClassInstanceByName[CustomDfTransformer](clazz)
  }.orElse {
    scalaFile.map {
      file =>
        val fnTransform = CustomCodeUtil.compileCode[fnTransformType](HdfsUtil.readHadoopFile(file))
        new CustomDfTransformerWrapper( fnTransform )
    }
  }.orElse {
    scalaCode.map {
      code =>
        val fnTransform = CustomCodeUtil.compileCode[fnTransformType](code)
        new CustomDfTransformerWrapper( fnTransform )
    }
  }.orElse {
    sqlCode.map {
      sql =>
        val fnTransform = createSqlFnTransform(sql)
        new CustomDfTransformerWrapper( fnTransform )
    }
  }.orElse {
    pythonFile.map {
      file =>
        val fnTransform = createPythonFnTransform(HdfsUtil.readHadoopFile(file))
        new CustomDfTransformerWrapper( fnTransform )
    }
  }.orElse {
    pythonCode.map {
      code =>
        val fnTransform = createPythonFnTransform(code.stripMargin)
        new CustomDfTransformerWrapper( fnTransform )
    }
  }

  override def toString: String = {
    if (className.isDefined)      "className: " +className.get
    else if(scalaFile.isDefined)  "scalaFile: " +scalaFile.get
    else if(scalaCode.isDefined)  "scalaCode: " +scalaCode.get
    else if(sqlCode.isDefined)    "sqlCode: "   +sqlCode.get
    else if(pythonCode.isDefined) "code: "+pythonCode.get
    else if(pythonFile.isDefined) "pythonFile: "+pythonFile.get
    else throw new IllegalStateException("transformation undefined!")
  }

  def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit session: SparkSession, context: ActionPipelineContext) : DataFrame = {
    // replace runtime options
    val runtimeOptionsReplaced = prepareRuntimeOptions(actionId, partitionValues)
    // transform
    impl.get.transform(session, options ++ runtimeOptionsReplaced, df, dataObjectId.id)
  }

  def transformPartitionValues(actionId: ActionId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    // replace runtime options
    val runtimeOptionsReplaced = prepareRuntimeOptions(actionId, partitionValues)
    // transform
    impl.get.transformPartitionValues(options ++ runtimeOptionsReplaced, partitionValues)
  }

  private def prepareRuntimeOptions(actionId: ActionId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[String,String] = {
    lazy val data = DefaultExpressionData.from(context, partitionValues)
    runtimeOptions.mapValues {
      expr => SparkExpressionUtil.evaluateString(actionId, Some("transformation.runtimeOptions"), expr, data)
    }.filter(_._2.isDefined).mapValues(_.get)
  }

  private def createSqlFnTransform(sql: String): fnTransformType = {
    (session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectIdStr: String) => {
      val dataObjectId = DataObjectId(dataObjectIdStr)
      val objectId = ActionHelper.replaceSpecialCharactersWithUnderscore(dataObjectIdStr)
      val preparedSql = SparkExpressionUtil.substituteOptions( dataObjectId, Some("transform.sqlCode"), sql, options)
      try {
        df.createOrReplaceTempView(s"$objectId")
        session.sql(preparedSql)
      } catch {
        case e : Throwable => throw new SQLTransformationException(s"(transformation for $dataObjectId) Could not execute SQL query. Check your query and remember that special characters are replaced by underscores (name of the temp view used was: ${objectId}). Error: ${e.getMessage}")
      }
    }
  }

  private def createPythonFnTransform(code: String): fnTransformType = {
    (session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String) => {
      // python transformation is executed by passing options and input/output DataFrame through entry point
      val objectId = ActionHelper.replaceSpecialCharactersWithUnderscore(dataObjectId)
      try {
        val entryPoint = new DfTransformerPySparkEntryPoint(session, options, df, objectId)
        val additionalInitCode = """
                                   |# prepare input parameters
                                   |options = entryPoint.options
                                   |inputDf = DataFrame(entryPoint.getInputDf(), sqlContext) # convert input dataframe to pyspark
                                   |dataObjectId = entryPoint.getDataObjectId()
                                   |# helper function to return output dataframe
                                   |def setOutputDf( df ):
                                   |    entryPoint.setOutputDf(df._jdf)
          """.stripMargin
        PythonUtil.execPythonTransform( entryPoint, additionalInitCode + sys.props("line.separator") + code)
        entryPoint.outputDf.getOrElse(throw new IllegalStateException("Python transformation must set output DataFrame (call setOutputDf(df))"))
      } catch {
        case e: Throwable => throw new PythonTransformationException(s"Could not execute Python code. Error: ${e.getMessage}", e)
      }
    }
  }
}

object CustomDfTransformerConfig {
  type fnTransformType = (SparkSession, Map[String, String], DataFrame, String) => DataFrame
}

private[smartdatalake] class DfTransformerPySparkEntryPoint(override val session: SparkSession, options: Map[String,String], inputDf: DataFrame, dataObjectId: String, var outputDf: Option[DataFrame] = None) extends SparkEntryPoint {
  // it seems that py4j can handle functions but not pure attributes -> wrapper functions needed...
  def getOptions: Map[String,String] = options
  def getInputDf: DataFrame = inputDf
  def getDataObjectId: String = dataObjectId
  def setOutputDf(df: DataFrame): Unit = {
    outputDf = Some(df)
  }
}