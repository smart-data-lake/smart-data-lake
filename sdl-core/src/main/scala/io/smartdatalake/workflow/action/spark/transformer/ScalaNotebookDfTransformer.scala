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
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.AuthMode
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.spark.DefaultExpressionData
import io.smartdatalake.util.webservice.ScalaJWebserviceClient
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformerConfig.fnTransformType
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, OptionsSparkDfTransformer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.{JsonMethods, Serialization}
import scalaj.http.{Http, HttpOptions}

import scala.util.{Failure, Success}

/**
 * Configuration of a custom Spark-DataFrame transformation between one input and one output (1:1) as Scala code which is compiled at runtime.
 * The code is loaded from a Notebook. It should define a transform function with a configurable name, which receives a DataObjectId, a DataFrame
 * and a map of options and has to return a DataFrame, see also ([[fnTransformType]]).
 * Notebook-cells starting with "//!IGNORE" will be ignored.
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param url            Url to download notebook in IPYNB-format, which defines transformation.
 * @param functionName   The notebook needs to contain a Scala-function with this name and type [[fnTransformType]].
 * @param authMode       optional authentication information for webservice, e.g. BasicAuthMode for user/pw authentication
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaNotebookDfTransformer(override val name: String = "scalaTransform", override val description: Option[String] = None, url: String, functionName: String, authMode: Option[AuthMode] = None, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsSparkDfTransformer {
  import ScalaNotebookDfTransformer._
  private var _fnTransform: Option[fnTransformType] = None
  override def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = {
    try {
      val notebookCode = prepareFunction(parseNotebook(downloadNotebook(url, authMode)), functionName)
      _fnTransform = Some(compileCode(notebookCode))
    } catch {
      case ex: Exception => throw new ConfigurationException(s"($actionId) " + ex.getMessage, None, ex)
    }
  }
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId, options: Map[String, String])(implicit context: ActionPipelineContext): DataFrame = {
    assert(_fnTransform.isDefined, s"($actionId) prepare() must be called before transformWithOptions()")
    _fnTransform.map(_(context.sparkSession, options, df, dataObjectId.id)).get
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = ScalaNotebookDfTransformer
}


object ScalaNotebookDfTransformer extends FromConfigFactory[GenericDfTransformer] {

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaNotebookDfTransformer = {
    extract[ScalaNotebookDfTransformer](config)
  }

  /**
   * Download Notebook content from url
   */
  def downloadNotebook(url: String, authMode: Option[AuthMode]): String = {
    import ScalaJWebserviceClient._
    val client = new ScalaJWebserviceClient(Http(url)
      .applyAuthMode(authMode)
      .option(HttpOptions.followRedirects(true))
      .header("Accept", "application/x-ipynb+json; application/json")
    )
    client.get() match {
      case Success(content) => new String(content)
      case Failure(ex) => throw new ConfigurationException(s"Could not read notebook code from url $url: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
    }
  }

  /**
   * Parse *.ipynb Notebook content
   * Get code from all cells with cell_type=code and language=scala, ignoring cells which start with "//!IGNORE" comment
   */
  def parseNotebook(notebookContent: String): String = {
    val notebookJson = JsonMethods.parse(notebookContent)
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val notebookCells = (notebookJson \ "cells")
      .filter(_ \ "cell_type" == JString("code"))
    val notebookCode = notebookCells
      .map(_ \ "source")
      .map {
        case JString(code) => code
        case JArray(codeList) => codeList.map{
          case JString(code) => code
        }.mkString(System.lineSeparator)
      }
      .filterNot(_.startsWith("//!IGNORE"))
      .mkString(System.lineSeparator)
    notebookCode
  }

  /**
   * Prepare function
   */
  def prepareFunction(notebookCode: String, functionName: String): String = {
    require(notebookCode.contains(functionName), s"Notebook code doesnt contain a function with name $functionName")
    val defaultImports = """
        |import org.apache.spark.sql.{DataFrame, SparkSession}
        |""".stripMargin
    // return function as last statement of notebook code block
    defaultImports + System.lineSeparator() + notebookCode + System.lineSeparator() + s"$functionName _"
  }

  def compileCode(code: String): fnTransformType = {
    CustomCodeUtil.compileCode[fnTransformType](code)
  }
}

