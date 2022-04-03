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

package io.smartdatalake.workflow.action.generic.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.action.ActionHelper
import io.smartdatalake.workflow.action.generic.transformer.OptionsGenericDfTransformer.PREVIOUS_TRANSFORMER_NAME
import io.smartdatalake.workflow.action.generic.transformer.SQLDfTransformer.INPUT_VIEW_NAME
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Configuration of a custom GenericDataFrame transformation between one input and one output (1:1) as SQL code.
 * The input data is available as temporary view in SQL. The inputs name is either the name of the DataObject,
 * or the name of the previous transformation, if this is not the first transformation of the chain. Also note that to create
 * the name of temporary view, special characters are replaces by underscores and a postfix "_sdltemp" is added.
 * It is therefore recommended to use special token %{inputViewName} or ${inputViewName_<input name>} that will be
 * replaced with the name of the temporary view at runtime.
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param code           SQL code for transformation.
 *                       Use tokens %{<key>} to replace with runtimeOptions in SQL code.
 *                       Example: "select * from test where run = %{runId}"
 *                       The special token %{inputViewName} or ${inputViewName_<input_name>} can be used to insert the temporary view name.
 *                       The input name is either the name of the DataObject, or the name of the previous transformation
 *                       if this is not the first transformation of the chain. Make sure to change the standard name of
 *                       the previous transformation in that case.
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class SQLDfTransformer(override val name: String = "sqlTransform", override val description: Option[String] = None, code: String, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map())
  extends OptionsGenericDfTransformer with SmartDataLakeLogger {
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, options: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    val function = DataFrameSubFeed.getFunctions(df.subFeedType)
    val inputName = options.getOrElse(PREVIOUS_TRANSFORMER_NAME, dataObjectId.id)
    val inputViewName = ActionHelper.createTemporaryViewName(inputName)
    val inputViewNameOptions = Map(INPUT_VIEW_NAME -> inputViewName, s"${INPUT_VIEW_NAME}_$inputName" -> inputViewName)
    var preparedSql = SparkExpressionUtil.substituteOptions(actionId, Some(s"transformers.$name.sqlCode"), code, options ++ inputViewNameOptions)
    try {
      // Using createTempView does not work because the same temporary view is created more than once (Init & Exec phase...)
      df.createOrReplaceTempView(s"$inputViewName")
      // for backward compatibility the temp view name from versions <= 2.2.x is replaced with the new temp view name including a postfix.
      if (Environment.replaceSqlTransformersOldTempViewName) {
        preparedSql = ActionHelper.replaceLegacyViewName(preparedSql, inputViewName)
      }
      // create DataFrame from SQL
      logger.info(s"($actionId.transformers.$name) Preparing DataFrame from SQL statement: $preparedSql")
      function.sql(preparedSql,dataObjectId)
    } catch {
      case e: Throwable => throw new SQLTransformationException(s"($actionId.transformers.$name) SQL query error: ${e.getMessage}. Also note to use token '%{inputViewName}' or '$inputViewName' as temporary view name in the SQL statement.", e)
    }
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = SQLDfTransformer
}

object SQLDfTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SQLDfTransformer = {
    extract[SQLDfTransformer](config)
  }
  private[smartdatalake] val INPUT_VIEW_NAME = "inputViewName"
}


/**
 * Exception is thrown if the SQL transformation can not be executed correctly
 */
private[smartdatalake] class SQLTransformationException(message: String, cause: Throwable) extends RuntimeException(message, cause) {}
