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
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion}

import scala.reflect.runtime.universe.typeOf
import scala.util.{Failure, Success, Try}

/**
 * Apply a filter condition to a DataFrame.
 *
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param filterClause Spark SQL expression to filter the DataFrame
 * @param subFeedTypeForValidation When parsing the configuration the runtime subFeedType for validating the filter expression is not yet known.
 *                                 By default SparkSubFeed langauge is used, but you can configure a different one if needed.
 */
case class FilterTransformer(override val name: String = "filter", override val description: Option[String] = None, filterClause: String, subFeedTypeForValidation: String = typeOf[SparkSubFeed].typeSymbol.fullName) extends GenericDfTransformer {
  private val validationHelper: DataFrameSubFeedCompanion = DataFrameSubFeed.getHelper(subFeedTypeForValidation)
  import validationHelper._
  private val filterClauseExpr = Try(expr(filterClause)) match {
    case Success(result) => result
    case Failure(e) => throw new ConfigurationException(s"Error parsing filterClause parameter as expression: ${e.getClass.getSimpleName}: ${e.getMessage}")
  }
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame = {
    val runtimeHelper = DataFrameSubFeed.getHelper(df.subFeedType)
    import runtimeHelper._
    df.filter(expr(filterClause))
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = FilterTransformer
}

object FilterTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): FilterTransformer = {
    extract[FilterTransformer](config)
  }
}

