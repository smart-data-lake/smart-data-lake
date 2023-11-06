/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import io.smartdatalake.workflow.dataframe.GenericDataFrame

/**
 * Convert null values in a dataframe
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
      case p if p._1.nonEmpty && p._2.isEmpty => df.schema.columns.filter(includeColumns.contains)
      case p if p._1.isEmpty && p._2.nonEmpty => df.schema.columns.filterNot(excludeColumns.contains)
      case _ => throw new IllegalArgumentException("includeColumns and excludeColumns are set. Use only one of the parameters at a time. ")
    }

    // Iterate over the columns
    val dfNew = columnNames.foldLeft(df) {
      (acc, columnName) =>
        // Get correct substitution value
        val substitutionValue = df.schema.getDataType(columnName).typeName match {
          case "integer" => valueForNumber
          case "string" => valueForString
          case "double" => valueForNumber
          case _ => None
        }
        if (substitutionValue != None) {
          acc.withColumn(columnName, coalesce(col(columnName), lit(substitutionValue).cast(df.schema.getDataType(columnName))))
        } else {
          acc
        }
    }

    dfNew
  }

  override def factory: FromConfigFactory[GenericDfTransformer] = ConvertNullValuesTransformer
}

object ConvertNullValuesTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ConvertNullValuesTransformer = {
    extract[ConvertNullValuesTransformer](config)
  }
}

