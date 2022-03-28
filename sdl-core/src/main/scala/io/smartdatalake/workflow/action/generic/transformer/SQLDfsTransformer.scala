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
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.action.ActionHelper
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Configuration of a custom GenericDataFrame transformation between many inputs and many outputs (n:m) as SQL code.
 * The input data is available as temporary views in SQL. As name for the temporary views the input DataObjectId is used
 * (special characters are replaces by underscores).
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
case class SQLDfsTransformer(override val name: String = "sqlTransform", override val description: Option[String] = None, code: Map[DataObjectId,String], options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsGenericDfsTransformer {
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame], options: Map[String, String])(implicit context: ActionPipelineContext): Map[String,GenericDataFrame] = {
    val functions = DataFrameSubFeed.getFunctions(dfs.values.head.subFeedType)
    // register all input DataObjects as temporary table
    dfs.foreach {
      case (dataObjectId,df) =>
        val objectId =  ActionHelper.replaceSpecialCharactersWithUnderscore(dataObjectId)
        // Using createTempView does not work because the same data object might be created more than once
        df.createOrReplaceTempView(objectId)
    }
    // execute all queries and return them under corresponding dataObjectId
    code.map {
      case (dataObjectId,sql) =>
        val df = try {
          val preparedSql = SparkExpressionUtil.substituteOptions(dataObjectId, Some(s"transformers.$name.code"), sql, options)
          functions.sql(preparedSql,dataObjectId)
        } catch {
          case e: Throwable => throw new SQLTransformationException(s"($actionId.transformers.$name) Could not execute SQL query for $dataObjectId. Check your query and remember remember that special characters are replaced by underscores for temporary view names in the SQL statement. Error: ${e.getMessage}")
        }
        (dataObjectId.id, df)
    }
  }
  override def factory: FromConfigFactory[GenericDfsTransformer] = SQLDfsTransformer
}

object SQLDfsTransformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SQLDfsTransformer = {
    extract[SQLDfsTransformer](config)
  }
}