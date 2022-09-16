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

package io.smartdatalake.workflow.action.snowflake.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.spark.DefaultExpressionData
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, OptionsGenericDfTransformer}
import io.smartdatalake.workflow.action.snowflake.customlogic.CustomSnowparkDfTransformer
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.snowflake.{SnowparkDataFrame, SnowparkSubFeed}
import io.smartdatalake.workflow.dataobject.SnowflakeTableDataObject

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Configuration of a custom Snowpark-DataFrame transformation between one input and one output (1:1) as Java/Scala Class.
 * Define a transform function which receives a DataObjectId, a DataFrame and a map of options and has to return a
 * DataFrame. The Java/Scala class has to implement interface [[CustomSnowparkDfTransformer]].
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param className      class name implementing trait [[CustomSnowparkDfTransformer]]
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaClassSnowparkDfTransformer(override val name: String = "scalaSparkTransform", override val description: Option[String] = None, className: String, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsGenericDfTransformer {
  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomSnowparkDfTransformer](className)

  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = {
    customTransformer.transformPartitionValues(options, partitionValues)
  }

  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, options: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    val action = context.instanceRegistry.get[Action](actionId)
    val snowparkSession = action.inputs.head.asInstanceOf[SnowflakeTableDataObject].snowparkSession
    df match {
      case snowparkDf: SnowparkDataFrame => SnowparkDataFrame(customTransformer.transform(snowparkSession, options, snowparkDf.inner, dataObjectId.id))
      case _ => throw new IllegalStateException(s"($actionId) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method transformWithOptions")
    }
  }

  override def getSubFeedSupportedType: Type = typeOf[SnowparkSubFeed]
  override def factory: FromConfigFactory[GenericDfTransformer] = ScalaClassSnowparkDfTransformer
}

object ScalaClassSnowparkDfTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaClassSnowparkDfTransformer = {
    extract[ScalaClassSnowparkDfTransformer](config)
  }
}