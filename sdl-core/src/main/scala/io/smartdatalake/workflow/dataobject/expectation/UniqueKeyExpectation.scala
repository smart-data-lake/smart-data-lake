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
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.{SparkColumn, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn}
import io.smartdatalake.workflow.dataobject.expectation.ExpectationScope.{ExpectationScope, Job}
import io.smartdatalake.workflow.dataobject.expectation.ExpectationSeverity.ExpectationSeverity
import io.smartdatalake.workflow.dataobject.{DataObject, TableDataObject}

import scala.reflect.runtime.universe.typeOf


/**
 * Definition of expectation on uniqueness of a given key.
 * Uniqueness is calculated as the fraction of output count distinct on key columns over output count.
 * It supports scope Job and All, but not JobPartition.
 *
 * Note that for scope=Job with Spark the approx_count_distinct function is used, as count_distinct is not allowed to be calculated within observations.
 * In this case the relative standard deviation of approx_count_distinct is set to a high precision.
 *
 * @param key Optional list of key columns to evaluate uniqueness
 * @param expectation Optional SQL comparison operator and literal to define expected value for validation. Default is '= 1'.
 *                    Together with the result of the aggExpression evaluation on the left side, it forms the condition to validate the expectation.
 *                    If no expectation is defined, the aggExpression evaluation result is just recorded in metrics.
 * @param precision Number of digits to keep when calculating fraction. Default is 4.
 */
case class UniqueKeyExpectation(
                                 override val name: String, key: Seq[String] = Seq(),
                                 override val expectation: Option[String] = Some("= 1"),
                                 override val precision: Short = 4,
                                 override val scope: ExpectationScope = Job,
                                 override val failedSeverity: ExpectationSeverity = ExpectationSeverity.Error )
  extends Expectation with ExpectationFractionMetricDefaultImpl {
  assert(scope != ExpectationScope.JobPartition, "scope=JobPartition not supported by UniqueKeyExpectation for now")

  override def roundFunc(v: Double): Double = math.floor(v) // use floor to be more aggressive on detecting unique key violations.

  override val description: Option[String] = Some("fraction of output count-distinct over output count")
  private val countDistinctName = "countDistinct" + name.capitalize
  private val countName = if (scope == ExpectationScope.Job) "count" else "countAll"
  def getPrimaryKeyCols(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): Seq[String] = {
    context.instanceRegistry.get[DataObject](dataObjectId) match {
      case x: TableDataObject =>
        assert(x.table.primaryKey.nonEmpty, s"($dataObjectId) 'table.primaryKey' must be defined on DataObject if parameter 'UniqueKeyExpectation.columns' is empty")
        x.table.primaryKey.getOrElse(throw new IllegalStateException(s"($dataObjectId) 'table.primaryKey' is defined but empty..."))
      case _ => throw ConfigurationException(s"($dataObjectId) If parameter 'columns' is empty, UniqueKeyExpectation must be defined on a DataObject implementing TableDataObject in order to use primary key")
    }
  }
  override def getAggExpressionColumns(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions, context: ActionPipelineContext): Seq[GenericColumn] = {
    val colsToCheck = (if (key.isEmpty) getPrimaryKeyCols(dataObjectId) else key).map(functions.col)
    Seq(
      if (scope == ExpectationScope.Job && functions.getSubFeedType == typeOf[SparkSubFeed]) Some(functions.approxCountDistinct(functions.struct(colsToCheck:_*), Some(0.005)).as(countDistinctName))
      else Some(functions.countDistinct(colsToCheck:_*).as(countDistinctName)),
      if (scope == ExpectationScope.All) Some(functions.count(functions.col("*")).as(countName)) else None
    ).flatten
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, metrics: Map[String,_], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): (Seq[SparkColumn],Map[String,_]) = {
    val countDistinct = getMetric[Long](dataObjectId,metrics,countDistinctName)
    val count = getMetric[Long](dataObjectId,metrics,countName)
    val (col, pct) = getValidationErrorColumn(dataObjectId, countDistinct, count)
    val updatedMetrics = metrics + (name -> pct)
    (col.map(SparkColumn).toSeq, updatedMetrics)
  }
  override def factory: FromConfigFactory[Expectation] = UniqueKeyExpectation
}

object UniqueKeyExpectation extends FromConfigFactory[Expectation] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): UniqueKeyExpectation = {
    extract[UniqueKeyExpectation](config)
  }
}
