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
import io.smartdatalake.workflow.action.{Action, ActionHelper}
import io.smartdatalake.workflow.action.generic.transformer.SQLDfTransformer.INPUT_VIEW_NAME
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Configuration of a custom GenericDataFrame transformation between many inputs and many outputs (n:m) as SQL code.
 * The input data is available as temporary views in SQL. As name for the temporary views the input DataObjectId is used
 * (special characters are replaces by underscores).
 * The input data is available as temporary view in SQL. The input name is either an id of the input DataObject,
 * or the name of an output of the previous transformation if this is not the first transformation of the chain.
 * Also note that to create the name of temporary view, special characters are replaces by underscores and a postfix "_sdltemp" is added.
 * It is therefore recommended to use the special token ${inputViewName_<input name>}, that will be replaced with the name
 * of the temporary view at runtime.
 *
 * Note that you can access arbitrary tables from the metastore in the SQL code, but this is against the principle of SDLB
 * to access data through DataObjects. Access tables directly in SQL code has a negative impact on the maintainability of the project.
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param code           Map of output names and corresponding SQL code for transformation.
 *                       If this is the last transformation in the chain, the output name has to match an output DataObject id,
 *                       otherwise it can be any name which will then be available in the next transformation.
 *                       Use tokens %{<key>} to replace with runtimeOptions in SQL code.
 *                       Example: "select * from test where run = %{runId}"
 *                       The special token ${inputViewName_<input_name>} can be used to insert the name of temporary views.
 *                       The input name is either the id of an input DataObject, or the name of an output of the previous transformation
 *                       if this is not the first transformation of the chain.
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class SQLDfsTransformer(override val name: String = "sqlTransform", override val description: Option[String] = None, code: Map[String,String], options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map())
  extends OptionsGenericDfsTransformer with SmartDataLakeLogger {
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame], options: Map[String, String])(implicit context: ActionPipelineContext): Map[String,GenericDataFrame] = {
    val functions = DataFrameSubFeed.getFunctions(dfs.values.head.subFeedType)
    // register all inputs as temporary table
    val inputViewNameOptions = dfs.map {
      case (inputName,df) =>
        val inputViewName =  ActionHelper.createTemporaryViewName(inputName)
        // Using createTempView does not work because the same temporary view is created more than once (Init & Exec phase...)
        df.createOrReplaceTempView(inputViewName)
        (s"${INPUT_VIEW_NAME}_$inputName" -> inputViewName)
    }
    // get an output DataObject from the Action to use to creating DataFrame from SQL. Note that this DataObject is only used to get connection information.
    val outputDataObjectId = context.instanceRegistry.get[Action](actionId).outputs.head.id
    // execute all queries and return them under corresponding output name
    code.map {
      case (outputName,sql) =>
        val df = try {
          var preparedSql = SparkExpressionUtil.substituteOptions(actionId, Some(s"transformers.$name.code"), sql, options ++ inputViewNameOptions)
          // for backward compatibility the temp view name from versions <= 2.2.x is replaced with the new temp view name including a postfix.
          if (Environment.replaceSqlTransformersOldTempViewName) {
            inputViewNameOptions.values.foreach(inputViewName => preparedSql = ActionHelper.replaceLegacyViewName(preparedSql, inputViewName))
          }
          // create DataFrame from SQL
          logger.debug(s"($actionId.transformers.$name) Preparing DataFrame $outputName from SQL statement: $preparedSql")
          functions.sql(preparedSql, outputDataObjectId)
        } catch {
          case e: Throwable => throw new SQLTransformationException(s"($actionId.transformers.$name) SQL query error for $outputName: ${e.getMessage}. Also note to use tokens '%{inputViewName_<inputName>}' as temporary view names in the SQL statement.", e)
        }
        (outputName, df)
    }
  }
  override def factory: FromConfigFactory[GenericDfsTransformer] = SQLDfsTransformer
}

object SQLDfsTransformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SQLDfsTransformer = {
    extract[SQLDfsTransformer](config)
  }
}