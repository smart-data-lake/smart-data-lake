/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSimpleDataType}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Replace null values in a DataFrame by a configurable placeholder value, so that downstream joins,
 * comparisons and primary keys do not have to deal with nulls.
 * String columns are filled with `valueForString` and numeric columns with `valueForNumber`; in both cases
 * the placeholder is cast to the data type of the column. Columns of any other data type (date, timestamp,
 * boolean, struct, array, ...) are left untouched.
 *
 * By default all columns of the DataFrame are converted. Limit the scope with either `includeColumns` or
 * `excludeColumns` - the two are mutually exclusive and configuring both fails the transformation.
 * All column names listed must exist in the DataFrame, otherwise the transformation fails.
 * Column names are matched case-insensitively, unless case sensitivity is enabled globally by setting
 * `global.environment.caseSensitive = true`.
 *
 * Example:
 * {{{
 * actions = {
 *   load-ratings {
 *     type = CopyAction
 *     inputId = stg-ratings
 *     outputId = int-ratings
 *     transformers = [{
 *       type = ConvertNullValuesTransformer
 *       excludeColumns = [comment]
 *       valueForString = "n/a"
 *       valueForNumber = 0
 *     }]
 *   }
 * }
 * }}}
 *
 * @param name              Name of the transformer
 * @param description       Optional description of the transformer
 * @param includeColumns   Optional list of columns to include into the transformation
 * @param excludeColumns   Optional list of columns to exclude from the transformation
 * @param valueForString    Value to add for string values, default value is "na"
 * @param valueForNumber    Value to add for number values, default value is -1
 */
case class ConvertNullValuesTransformer(override val name: String = "ConvertNullValuesTransformer", override val description: Option[String] = None, includeColumns: Seq[String] = Seq(), excludeColumns: Seq[String] = Seq(), valueForString: String = "na", valueForNumber: Int = -1 ) extends GenericDfTransformer {

  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    require((includeColumns.isEmpty != excludeColumns.isEmpty) || (includeColumns.isEmpty && excludeColumns.isEmpty), "Conflicting parameters. Please use either includeColumns or excludeColumns, as simultaneous application is not supported.")

    val functions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._

    // Filter column names
    val columnNames = (includeColumns, excludeColumns) match {
      case p if p._1.isEmpty && p._2.isEmpty => df.schema.columns
      case p if p._1.nonEmpty && p._2.isEmpty => {
        // Check if the columns exist and return the filtered list
        includeColumns.foreach(v => require(df.schema.columnExists(v), s"[${v}] does not exist in dataframe. Available columns are [${df.schema.columns.mkString(", ")}]."))
        df.schema.filterColumns(includeColumns)
      }
      case p if p._1.isEmpty && p._2.nonEmpty => {
        // Check if the columns exist and return the filtered list
        excludeColumns.foreach(v => require(df.schema.columnExists(v), s"[${v}] does not exist in dataframe. Available columns are [${df.schema.columns.mkString(", ")}]."))
        df.schema.filterColumns(excludeColumns, includeColumns = false)
      }
      case _ => throw new IllegalArgumentException("includeColumns and excludeColumns are set. Use only one of the parameters at a time. ")
    }

    // Iterate over the columns
    val dfNew = columnNames.foldLeft(df) {
      (acc, columnName) =>
        // Get correct substitution value
        val substitutionValue = df.schema.getDataType(columnName) match {
          case dt: GenericSimpleDataType if dt.isNumeric => Some(valueForNumber)
          case dt if dt.typeName.toLowerCase == "string" => Some(valueForString)
          case _ => None
        }
        substitutionValue
          .map(v => acc.withColumn(columnName, coalesce(col(columnName), lit(v).cast(df.schema.getDataType(columnName)))))
          .getOrElse(acc)
    }

    dfNew
  }
}

object ConvertNullValuesTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ConvertNullValuesTransformer = {
    extract[ConvertNullValuesTransformer](config)
  }
}

