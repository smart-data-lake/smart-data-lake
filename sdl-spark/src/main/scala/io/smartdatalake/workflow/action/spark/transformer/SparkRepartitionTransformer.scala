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
import io.smartdatalake.util.spark.SparkRepartitionDef
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.GenericDfTransformer
import org.apache.spark.sql.DataFrame

/**
 * Repartition a Spark DataFrame in the middle of a transformation chain, controlling how many Spark
 * tasks - and therefore how many output files per partition value - the following steps work with.
 * Use it to avoid the small-files problem when writing many small partitions, or to increase
 * parallelism before an expensive transformation. Repartitioning triggers a shuffle, so apply it
 * deliberately.
 *
 * For a detailled description about repartitioning DataFrames see also [[SparkRepartitionDef]].
 *
 * Example:
 * {{{
 * actions = {
 *   copy-departures {
 *     type = CopyAction
 *     inputId = stg-departures
 *     outputId = int-departures
 *     transformers = [{
 *       type = SparkRepartitionTransformer
 *       numberOfTasksPerPartition = 10
 *       keyCols = [icao24]
 *     }]
 *   }
 * }
 * }}}
 *
 * @note If the output DataObject already defines `sparkRepartition`, prefer configuring it there;
 *       this transformer is for repartitioning between transformation steps.
 *
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param numberOfTasksPerPartition Number of Spark tasks to create per partition value by repartitioning the DataFrame.
 * @param keyCols  Optional key columns to distribute records over Spark tasks inside a partition value.
 */
case class SparkRepartitionTransformer(override val name: String = "sparkRepartition", override val description: Option[String] = None, numberOfTasksPerPartition: Int, keyCols: Seq[String] = Seq()) extends SparkDfTransformer {
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    SparkRepartitionDef.repartitionDataFrame(df, partitionValues, dataObjectId, keyCols, numberOfTasksPerPartition)
  }
}

object SparkRepartitionTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SparkRepartitionTransformer = {
    extract[SparkRepartitionTransformer](config)
  }
}


