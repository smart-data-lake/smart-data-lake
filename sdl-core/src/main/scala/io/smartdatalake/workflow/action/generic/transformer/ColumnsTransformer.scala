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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Add, Rename or Drop columns from the Input DataFrame. The order of execution of the operations is the same as the parameters of the transformer: (1. add context info, 2. add custom sql, 3. rename, 4. drop).
 * Note that you can mix and match these operations to your liking and reference the same column from a previous operation if needed.
 *
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param additionalColumns optional tuples of [column name, spark sql expression] to be added as additional columns to the dataframe. This allows you to include commonly used context information available at runtime.
 *                          The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]] and added to the DataFrame as literal columns.
 *                          [[DefaultExpressionData]] contains informations from the context of the SDLB job, like runId or feed name.
 * @param additionalDerivedColumns optional tuples of [column name, spark sql expression] to be added as additional columns to the dataframe. This allows you to run custom sql code on the input DataFrame and save the result into a new column for which you define the name.
 * @param renamedColumns optional tuples of [old column name, new column name]. For each tuple, a column is renamed. A RuntimeError will occur if a column to be renamed does not exist after applying all previous operations.
 * @param droppedColumns optional list of column names to be dropped. A RuntimeError will occur if a column does not exist after applying all previous operations.
 *
 * Example Config:
 *multiply {
 *  type = CopyAction
 *  inputId = src1DO
 *  outputId = tgt1DO
 *  metadata.feed = test_feed_name
 *  transformers = [
 *    {type = ColumnsTransformer, additionalDerivedColumns = {rating_doubled = "rating * 2"}, renamedColumns = {rating_doubled = rating_doubled_renamed}, droppedColumns = [name]}
 *  ]
 *}
 */
case class ColumnsTransformer(override val name: String = "additionalColumns", override val description: Option[String] = None, additionalColumns: Map[String,String] = Map(), additionalDerivedColumns: Map[String,String] = Map(), renamedColumns: Map[String,String] = Map(), droppedColumns: Seq[String] = Seq()) extends GenericDfTransformer {
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    val data = DefaultExpressionData.from(context, partitionValues)
    val dfLit = additionalColumns.foldLeft(df){
      case (df, (colName, litExpr)) =>
        val value = SparkExpressionUtil.evaluate[DefaultExpressionData,Any](actionId, Some(name), litExpr, data)
        df.withColumn(colName, lit(value.orNull))
    }
    val dfDerived = additionalDerivedColumns.foldLeft(dfLit){
      case (df, (colName, deriveExpr)) => try {
        df.withColumn(colName, expr(deriveExpr))
      } catch {
        case e: Exception => throw ConfigurationException(s"""($actionId) Creating additional derived column $colName using expression "$deriveExpr" failed: ${e.getMessage}""", Some(s"$name.$colName"), e)
      }
    }
    val dfRenamed = renamedColumns.foldLeft(dfDerived){
      case (df, (colName, newName)) => try {
        df.withColumnRenamed(colName, newName)
      } catch {
        case e: Exception => throw ConfigurationException(s"""($actionId) Renaming column $colName to $newName failed: ${e.getMessage}""", Some(s"$name.$colName"), e)
      }
    }
    val dfDropped = droppedColumns.foldLeft(dfRenamed){
      case (df, colName) => try {
        df.drop(colName)
      } catch {
        case e: Exception => throw ConfigurationException(s"""($actionId) Dropping column $colName failed: ${e.getMessage}""", Some(s"$name.$colName"), e)
      }
    }
    dfDropped
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = ColumnsTransformer
}

object ColumnsTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ColumnsTransformer = {
    extract[ColumnsTransformer](config)
  }
}

