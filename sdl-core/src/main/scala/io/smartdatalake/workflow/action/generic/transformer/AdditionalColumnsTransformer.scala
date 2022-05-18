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
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Add additional columns to the DataFrame by extracting information from the context or derived from input columns.
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param additionalColumns optional tuples of [column name, spark sql expression] to be added as additional columns to the dataframe.
 *                          The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]] and added to the DataFrame as literal columns.
 *                          [[DefaultExpressionData]] contains informations from the context of the SDLB job, like runId or feed name.
 * @param additionalDerivedColumns optional tuples of [column name, spark sql expression] to be added as additional columns to the dataframe.
 *                                 The spark sql expressions are evaluated against the input DataFrame and added to the DataFrame as derived columns.
 */
case class AdditionalColumnsTransformer(override val name: String = "additionalColumns", override val description: Option[String] = None, additionalColumns: Map[String,String] = Map(), additionalDerivedColumns: Map[String,String] = Map()) extends GenericDfTransformer {
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    val data = DefaultExpressionData.from(context, partitionValues)
    val dfLit = additionalColumns.foldLeft(df){
      case (df, (colName, litExpr)) =>
        val value = SparkExpressionUtil.evaluate[DefaultExpressionData,Any](actionId, Some(name), litExpr, data)
        df.withColumn(colName, lit(value.orNull))
    }
    val dfDerived = additionalColumns.foldLeft(dfLit){
      case (df, (colName, deriveExpr)) => df.withColumn(colName, expr(deriveExpr))
    }
    dfDerived
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = AdditionalColumnsTransformer
}

object AdditionalColumnsTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AdditionalColumnsTransformer = {
    extract[AdditionalColumnsTransformer](config)
  }
}

