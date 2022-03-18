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

package io.smartdatalake.workflow.action.spark.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.DataFrameUtil._
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, SparkDfTransformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Standardize datatypes of a Spark-DataFrame.
 * Current implementation converts all decimal datatypes to a corresponding integral or float datatype
 *
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 */
case class StandardizeDatatypesTransformer(override val name: String = "standardizeDatatypes", override val description: Option[String] = None) extends SparkDfTransformer {
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    df.castAllDecimal2IntegralFloat
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = StandardizeDatatypesTransformer
}

object StandardizeDatatypesTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): StandardizeDatatypesTransformer = {
    extract[StandardizeDatatypesTransformer](config)
  }
}

