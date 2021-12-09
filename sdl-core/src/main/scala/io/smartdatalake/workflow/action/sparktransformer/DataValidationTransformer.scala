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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.functions.{array, expr, flatten, lit, when}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * Apply validation rules to a DataFrame and collect potential violation error messages in a new column.
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param rules        list of validation rules to apply to the DataFrame
 * @param errorsColumn Optional column name for the list of error messages. Default is "errors".
 */
case class DataValidationTransformer(override val name: String = "dataValidation", override val description: Option[String] = None, rules: Seq[ValidationRule], errorsColumn: String = "errors") extends ParsableDfTransformer {
  // check that rules are parsable
  rules.foreach(_.getValidationColumn)
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    df.withColumn(errorsColumn, flatten(array(rules.map(rule => array(rule.getValidationColumn)): _*))) // nested array and flatten is used to eliminate null entries
  }
  override def factory: FromConfigFactory[ParsableDfTransformer] = DataValidationTransformer
}

object DataValidationTransformer extends FromConfigFactory[ParsableDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DataValidationTransformer = {
    extract[DataValidationTransformer](config)
  }
}

sealed trait ValidationRule {
  def prepare(implicit context: ActionPipelineContext): Unit = Unit
  def getValidationColumn: Column
}

/**
 * Definition for a row level data validation rule.
 * @param condition a Spark SQL expression defining the condition to be tested.
 * @param errorMsg Optional error msg to be create if the condition fails. Default is to use a text representation of the condition.
 */
case class RowLevelValidationRule(condition: String, errorMsg: Option[String] = None) extends ValidationRule {
  override def getValidationColumn: Column = when(!expr(condition), lit(errorMsg.getOrElse(s"""validation rule "$condition" failed!"""")))
}