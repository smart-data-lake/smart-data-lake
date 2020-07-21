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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.{CustomCodeUtil, PythonUtil}
import io.smartdatalake.workflow.action.ActionHelper
import org.apache.spark.python.PythonHelper.SparkEntryPoint
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Interface to define custom logic for a DataFrame
  */
trait CustomDfTransformer extends Serializable {

  /**
   * Functions provided by the creator, used to transform a DataFrame
    *
    * @param session Spark Session
    * @param options additional options
    * @param df DataFrame to be transformed
    * @param dataObjectId Id of DataObject of SubFeed
    * @return Transformed DataFrame
    */
  def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String) : DataFrame

}

case class CustomDfTransformerConfig( className: Option[String] = None, scalaFile: Option[String] = None, scalaCode: Option[String] = None, sqlCode: Option[String] = None, pythonFile: Option[String] = None, pythonCode: Option[String] = None, options: Map[String,String] = Map()) {
  require(className.isDefined || scalaFile.isDefined || scalaCode.isDefined || sqlCode.isDefined || pythonFile.isDefined || pythonCode.isDefined, "Either className, scalaFile, scalaCode, sqlCode, pythonFile or pythonCode must be defined for CustomDfTransformer")

  val impl : Option[CustomDfTransformer] = className.map {
    clazz => CustomCodeUtil.getClassInstanceByName[CustomDfTransformer](clazz)
  }.orElse {
    scalaFile.map {
      file =>
        val fnTransform = CustomCodeUtil.compileCode[(SparkSession, Map[String,String], DataFrame, String) => DataFrame](HdfsUtil.readHadoopFile(file))
        new CustomDfTransformerWrapper( fnTransform )
    }
  }.orElse {
    scalaCode.map {
      code =>
        val fnTransform = CustomCodeUtil.compileCode[(SparkSession, Map[String,String], DataFrame, String) => DataFrame](code)
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
        val fnTransform = createPythonFnTransform(code)
        new CustomDfTransformerWrapper( fnTransform )
    }
  }

  override def toString: String = {
    if (className.isDefined)      "className: " +className.get
    else if(scalaFile.isDefined)  "scalaFile: " +scalaFile.get
    else if(scalaCode.isDefined)  "scalaCode: " +scalaCode.get
    else if(sqlCode.isDefined)    "sqlCode: "   +sqlCode.get
    else if(pythonCode.isDefined) "pythonCode: "+pythonCode.get
    else if(pythonFile.isDefined) "pythonFile: "+pythonFile.get
    else throw new IllegalStateException("transformation undefined!")
  }

  def transform(df: DataFrame, dataObjectId: DataObjectId)(implicit session: SparkSession) : DataFrame = {
    impl.getOrElse(throw new IllegalStateException("transformation undefined!"))
      .transform(session, options, df, dataObjectId.id)
  }

  private def createSqlFnTransform(sql: String): (SparkSession, Map[String, String], DataFrame, String) => DataFrame = {
    (session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String) => {
      val objectId = ActionHelper.replaceSpecialCharactersWithUnderscore(dataObjectId)
      try {
        df.createOrReplaceTempView(s"$objectId")
        //TODO: replace tokens with options
        session.sql(sqlCode.get)
      } catch {
        case e: Throwable => throw new SQLTransformationException(s"Could not execute SQL query. Check your query and remember that special characters are replaced by underscores (name of the temp view used was: $objectId). Error: ${e.getMessage}")
      }
    }
  }

  private def createPythonFnTransform(code: String): (SparkSession, Map[String, String], DataFrame, String) => DataFrame = {
    (session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String) => {
      // python transformation is executed by creating a temp view with the input DataFrame and reading a temp view to get the output DataFrame
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
        PythonUtil.execPythonTransform( entryPoint, additionalInitCode+"\n"+code)
        entryPoint.outputDf.getOrElse(throw new IllegalStateException("Python transformation must set output DataFrame (call setOutputDf(df))"))
      } catch {
        case e: Throwable => throw new PythonTransformationException(s"Could not execute Python code. Error: ${e.getMessage}", e)
      }
    }
  }
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
