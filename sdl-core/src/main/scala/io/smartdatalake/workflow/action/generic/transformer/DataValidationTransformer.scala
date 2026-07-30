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
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Apply a list of validation rules to a DataFrame and collect the error messages of all violated rules in
 * a new array column, instead of failing the Action.
 * No rows are removed: the errors column stays empty for records that satisfy all rules. This allows to
 * keep bad records for reporting, or to split them off with a subsequent [[FilterTransformer]] on the
 * errors column. If the job should rather fail on bad data, use [[io.smartdatalake.workflow.dataobject.generic.Constraint]]
 * or an Expectation like [[io.smartdatalake.workflow.dataobject.expectation.SQLExpectation]] on the output DataObject.
 *
 * The rule expressions are already parsed when the configuration is read, so syntax errors are detected
 * before the pipeline is started.
 *
 * Example:
 * {{{
 * actions = {
 *   validate-ratings {
 *     type = CopyAction
 *     inputId = stg-ratings
 *     outputId = int-ratings
 *     transformers = [{
 *       type = DataValidationTransformer
 *       errorsColumn = validation_errors
 *       rules = [
 *         { type = RowLevelValidationRule, condition = "rating is not null", errorMsg = "rating should not be empty" }
 *         { type = RowLevelValidationRule, condition = "rating between 1 and 5" }
 *       ]
 *     }]
 *   }
 * }
 * }}}
 *
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param rules        list of validation rules to apply to the DataFrame
 * @param errorsColumn Optional column name for the list of error messages. Default is "errors".
 * @param subFeedTypeForValidation For validating the rule expression, the runtime subFeedType is not yet known.
 *                                 By default SparkSubFeed langauge is used, but you can configure a different one if needed.
 */
case class DataValidationTransformer(override val name: String = "dataValidation", override val description: Option[String] = None, rules: Seq[ValidationRule], errorsColumn: String = "errors", subFeedTypeForValidation: String = "io.smartdatalake.workflow.dataframe.spark.SparkSubFeed") extends GenericDfTransformer {
  private implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(subFeedTypeForValidation)
  // check that rules are parsable
  rules.foreach(_.getValidationColumn(functions))
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    import functions._
    df.withColumn(errorsColumn, array_construct_compact(rules.map(rule => rule.getValidationColumn).toIndexedSeq: _*))
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = DataValidationTransformer
}

object DataValidationTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DataValidationTransformer = {
    extract[DataValidationTransformer](config)
  }
}

sealed trait ValidationRule {
  def prepare(implicit context: ActionPipelineContext): Unit = ()
  def getValidationColumn(implicit helper: DataFrameFunctions): GenericColumn
}

/**
 * Definition for a row level data validation rule.
 * The condition is evaluated for every row on its own, so it can only reference columns of the current
 * record. Aggregations over the whole DataFrame are not possible, use an Expectation on the output
 * DataObject for that.
 *
 * Example:
 * {{{
 * actions = {
 *   validate-ratings {
 *     type = CopyAction
 *     inputId = stg-ratings
 *     outputId = int-ratings
 *     transformers = [{
 *       type = DataValidationTransformer
 *       rules = [
 *         { type = RowLevelValidationRule, condition = "rating between 1 and 5", errorMsg = "rating must be between 1 and 5" }
 *       ]
 *     }]
 *   }
 * }
 * }}}
 *
 * @param condition an SQL expression defining the condition to be tested. The condition should return true if the condition is satisfied.
 * @param errorMsg  Optional error msg to be create if the condition fails. Default is to use a text representation of the condition.
 */
case class RowLevelValidationRule(condition: String, errorMsg: Option[String] = None) extends ValidationRule {
  override def getValidationColumn(implicit functions: DataFrameFunctions): GenericColumn = {
    import functions._
    when(not(expr(condition)), lit(errorMsg.getOrElse(s"""validation rule "$condition" failed!""")))
  }
}