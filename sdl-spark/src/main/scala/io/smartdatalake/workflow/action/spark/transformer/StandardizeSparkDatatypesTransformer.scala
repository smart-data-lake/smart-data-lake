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
package io.smartdatalake.workflow.action.spark.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.GenericDfTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Standardize datatypes of a Spark-DataFrame.
 * Current implementation converts all decimal datatypes to a corresponding integral or float datatype.
 *
 * Decimal columns are common when reading from JDBC sources or Parquet files written by other tools, but
 * they are inconvenient downstream: they compare and serialize differently and often carry an unnecessarily
 * wide precision. Adding this transformer as the first entry of a transformation chain gives all Actions a
 * uniform, narrow numeric type per column without listing columns explicitly. The transformer takes no
 * configuration attributes beyond the common `name`/`description`.
 *
 * Example:
 * {{{
 * actions = {
 *   copy-departures {
 *     type = CopyAction
 *     inputId = stg-departures
 *     outputId = int-departures
 *     transformers = [{ type = StandardizeSparkDatatypesTransformer }]
 *   }
 * }
 * }}}
 *
 * @note The conversion is lossy if a decimal column holds values that do not fit the derived integral or
 *       float type; check precision and scale of the source schema before enabling it.
 *
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 */
case class StandardizeSparkDatatypesTransformer(override val name: String = "standardizeSparkDatatypes",
                                                override val description: Option[String] = None)
  extends SparkDfTransformer with io.smartdatalake.util.spark.dataset.Transform {
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    df.castAllDecimal2IntegralFloat
  }
}

object StandardizeSparkDatatypesTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): StandardizeSparkDatatypesTransformer = {
    extract[StandardizeSparkDatatypesTransformer](config)
  }
}

