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

package io.smartdatalake.workflow.action.spark.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, SparkDfTransformer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode_outer, get}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

import scala.annotation.tailrec

/**
 * Returns a flattened Dataframe from another DataFrame
 * with a nested schema. Schemas in the form:
 *
 * |---parent
 *
 * |------|child1
 *
 * |------|child2
 *
 * are transformed to the following flat columns:
 *
 * |---parent_child1
 *
 * |---parent_child2
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param enableExplode Enables exploding Arrays in the Dataframe, therefore potentially increasing the number of rows. Default is set to "false"
 */
case class SparkFlattenDfTransformer(override val name: String = "sparkFlattenDataFrame",
                                     override val description: Option[String] = None,
                                     enableExplode: Boolean = false) extends SparkDfTransformer {

  private def getComplexFields(df: DataFrame): Seq[(String, DataType)] = {
    df.schema.fields.map(f => (f.name, f.dataType)).collect {
      case (name, dataType: StructType) => (name,dataType)
      case (name, dataType: ArrayType) if enableExplode => (name,dataType)
    }.toSeq
  }

  private def flattenDf(df: DataFrame): DataFrame = {
    val complexFields = getComplexFields(df)
    complexFields.foldLeft(df){
      case (df, (colName, colType)) =>
        val newDf = colType match {
          case StructType(fields) =>
            fields.map(_.name).foldLeft(df) {
              case (df, name) => df.withColumn(colName + '_' + name, col(colName + '.' + name))
            }.drop(colName)
          case a: ArrayType => df.withColumn(colName, explode_outer(col(colName)))
        }
        flattenDf(newDf)
    }
  }
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame =
    flattenDf(df)
  override def factory: FromConfigFactory[GenericDfTransformer] = SparkFlattenDfTransformer
}
object SparkFlattenDfTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SparkFlattenDfTransformer = {
    extract[SparkFlattenDfTransformer](config)
  }
}
