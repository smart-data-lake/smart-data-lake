/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.dataobject.expectation

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.spark.SparkExpressionUtil
import io.smartdatalake.workflow.action.ActionHelper
import io.smartdatalake.workflow.action.generic.transformer.SQLDfTransformer.INPUT_VIEW_NAME
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn, GenericDataFrame}
import io.smartdatalake.workflow.dataobject.expectation.ExpectationScope.{ExpectationScope, Job}
import io.smartdatalake.workflow.dataobject.expectation.ExpectationSeverity.ExpectationSeverity
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}


/**
 * Definition of an expectation based on a SQL query to be evaluate on dataset-level.
 * The SQL query will be evaluated in a separate Spark job against the DataFrame.
 * It supports scope Job and All, but not JobPartition.
 *
 * @param code a SQL query returning a single row. All column will be added as metrics.
 *             If there are more than one column, there has to be one column with the same name as this expectation. This column will be used to compare against a potential condition of the expectation.
 *             The special token %{inputViewName} must be used to insert the temporary view name used to provide the DataFrame to the query.
 * @param expectation Optional SQL comparison operator and literal to define expected value for validation, e.g. '= 0".
 *                    Together with the result of the aggExpression evaluation on the left side, it forms the condition to validate the expectation.
 *                    If no expectation is defined, the aggExpression evaluation result is just recorded in metrics.
 */
case class SQLQueryExpectation(override val name: String, override val description: Option[String] = None, code: String, override val expectation: Option[String] = None, override val scope: ExpectationScope = Job, override val failedSeverity: ExpectationSeverity = ExpectationSeverity.Error )
  extends Expectation with ExpectationOneMetricDefaultImpl {
  assert(scope != ExpectationScope.JobPartition, "scope=JobPartition not supported by SQLQueryExpectation for now")
  override def getAggExpressionColumns(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions, context: ActionPipelineContext): Seq[GenericColumn] = {
    // SQLQueryExpectation implements getCustomMetrics and does not need getAggExpressionColumns
    Seq()
  }
  override def getCustomMetrics(dataObjectId: DataObjectId, df: Option[GenericDataFrame])(implicit functions: DataFrameFunctions, context: ActionPipelineContext): Map[String, _] = {
    if (df.isEmpty) throw ConfigurationException(s"($dataObjectId) Can not calculate custom metrics for Expectations with scope=JobPartition/Job on read")
    val inputName = dataObjectId.id
    val inputViewName = ActionHelper.createTemporaryViewName(inputName)
    val inputViewNameOptions = Map(INPUT_VIEW_NAME -> inputViewName, s"${INPUT_VIEW_NAME}_$inputName" -> inputViewName)
    val preparedSql = SparkExpressionUtil.substituteOptions(dataObjectId, Some(s"expectations.$name.code"), code, inputViewNameOptions)
    try {
      df.get.createOrReplaceTempView(s"$inputViewName")
      // create DataFrame from SQL
      logger.debug(s"($dataObjectId.expectations.$name) Preparing DataFrame from SQL statement: $preparedSql")
      val dfMetrics = functions.sql(preparedSql, dataObjectId)
      if (dfMetrics.schema.columns.size > 1 && !dfMetrics.schema.columns.contains(name)) throw new RuntimeException(s"($dataObjectId) Query of SQLQueryExpectation $name returns more than one column and no column is named the same as the expectation")
      logger.info(s"($dataObjectId) collecting custom metrics for SQLQueryExpectation $name")
      val rows = dfMetrics.collect
      if (rows.isEmpty) throw new RuntimeException(s"($dataObjectId) Result of SQLQueryExpectation $name is empty")
      if (rows.size > 1) throw new RuntimeException(s"($dataObjectId) Result of SQLQueryExpectation $name has more than one row (${rows.size})")
      val resultRow = rows.head
      if (dfMetrics.schema.columns.size > 1) {
        dfMetrics.schema.columns.zipWithIndex.map {
          case (c, idx) => (c, Option(resultRow.get(idx)).getOrElse(None))
        }.toMap
      } else {
        Map(name -> Option(resultRow.get(0)).getOrElse(None))
      }
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Expectation $name: cannot parse SQL code '$code': ${e.getClass.getSimpleName} ${e.getMessage}", Some(s"expectations.$name.code"), e)
    }
  }
  override def factory: FromConfigFactory[Expectation] = SQLQueryExpectation
}

object SQLQueryExpectation extends FromConfigFactory[Expectation] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SQLQueryExpectation = {
    extract[SQLQueryExpectation](config)
  }
}
