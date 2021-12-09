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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionHelper
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Configuration of a custom Spark-DataFrame transformation between one input and one output (1:1) as SQL code.
 * The input data is available as temporary view in SQL. As name for the temporary view the input DataObjectId is used
 * (special characters are replaces by underscores). A special token '%{inputViewName}' will be replaced with the name of
 * the temporary view at runtime.
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param code           SQL code for transformation.
 *                       Use tokens %{<key>} to replace with runtimeOptions in SQL code.
 *                       Example: "select * from test where run = %{runId}"
 *                       A special token %{inputViewName} can be used to insert the temporary view name.
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class SQLDfTransformer(override val name: String = "sqlTransform", override val description: Option[String] = None, code: String, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsDfTransformer {
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId, options: Map[String, String])(implicit context: ActionPipelineContext): DataFrame = {
    val inputViewName = ActionHelper.replaceSpecialCharactersWithUnderscore(dataObjectId.id)
    val preparedSql = SparkExpressionUtil.substituteOptions(actionId, Some(s"transformers.$name.sqlCode"), code, options + ("inputViewName" -> inputViewName))
    try {
      df.createOrReplaceTempView(s"$inputViewName")
      context.sparkSession.sql(preparedSql)
    } catch {
      case e: Throwable => throw new SQLTransformationException(s"($actionId.transformers.$name) Could not execute SQL query. Check your query and make sure to use '$inputViewName' or token '%{inputViewName}' as temporary view in the SQL statement (special characters are replaces by underscores). Error: ${e.getMessage}")
    }
  }
  override def factory: FromConfigFactory[ParsableDfTransformer] = SQLDfTransformer
}

object SQLDfTransformer extends FromConfigFactory[ParsableDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SQLDfTransformer = {
    extract[SQLDfTransformer](config)
  }
}


/**
 * Exception is thrown if the SQL transformation can not be executed correctly
 */
private[smartdatalake] class SQLTransformationException(message: String) extends RuntimeException(message) {}
