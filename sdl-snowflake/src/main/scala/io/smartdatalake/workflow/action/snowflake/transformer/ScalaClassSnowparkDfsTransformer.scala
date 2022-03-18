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

package io.smartdatalake.workflow.action.snowflake.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry, ParsableFromConfig}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.spark.DefaultExpressionData
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformer, OptionsGenericDfsTransformer}
import io.smartdatalake.workflow.action.snowflake.customlogic.CustomSnowparkDfsTransformer
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.snowflake.SnowparkDataFrame
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject.SnowflakeTableDataObject

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Configuration of a custom Spark-DataFrame transformation between many inputs and many outputs (n:m)
 * Define a transform function which receives a map of input DataObjectIds with DataFrames and a map of options and as
 * to return a map of output DataObjectIds with DataFrames, see also trait [[CustomSnowparkDfsTransformer]].
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param className      class name implementing trait [[CustomSnowparkDfsTransformer]]
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaClassSnowparkDfsTransformer(name: String = "snowparkScalaTransform",
                                  description: Option[String] = None,
                                  className: String,
                                  options: Map[String, String] = Map(),
                                  runtimeOptions: Map[String, String] = Map()
                                 )
  extends OptionsGenericDfsTransformer {

  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomSnowparkDfsTransformer](className)

  def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame], options: Map[String,String])(implicit context: ActionPipelineContext): Map[String,GenericDataFrame] = {
    assert(dfs.values.forall(_.isInstanceOf[SnowparkDataFrame]), s"($actionId) Unsupported subFeedType(s) ${dfs.values.filterNot(_.isInstanceOf[SparkDataFrame]).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method transform")
    val action = context.instanceRegistry.get[Action](actionId)
    val snowparkSession = action.inputs.head.asInstanceOf[SnowflakeTableDataObject].snowparkSession
    val snowparkDfs = dfs.mapValues(_.asInstanceOf[SnowparkDataFrame].inner)
    customTransformer.transform(snowparkSession, options, snowparkDfs)
      .mapValues(SnowparkDataFrame)
  }

  override def getSubFeedSupportedType: Type = typeOf[SnowparkDataFrame]

  override def factory: FromConfigFactory[GenericDfsTransformer] = ScalaClassSnowparkDfsTransformer
}

// This companion object ensures that SnowparkDfsTransformer can be parsed from the configuration
object ScalaClassSnowparkDfsTransformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaClassSnowparkDfsTransformer = {
    extract[ScalaClassSnowparkDfsTransformer](config)
  }
}
