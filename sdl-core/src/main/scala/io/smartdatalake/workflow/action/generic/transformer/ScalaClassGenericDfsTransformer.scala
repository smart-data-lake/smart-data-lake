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

import io.smartdatalake.config.FromConfigFactory
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.spark.DefaultExpressionData
import io.smartdatalake.workflow.action.generic.customlogic.CustomGenericDfsTransformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassDfsTransformer
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Configuration of a custom Spark-DataFrame transformation between many inputs and many outputs (n:m)
 * Define a transform function which receives a map of input DataObjectIds with DataFrames and a map of options and as
 * to return a map of output DataObjectIds with DataFrames, see also trait [[CustomGenericDfsTransformer]].
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param className      class name implementing trait [[CustomGenericDfsTransformer]]
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaClassGenericDfsTransformer(override val name: String = "scalaTransform", override val description: Option[String] = None, className: String, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsGenericDfsTransformer {
  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomGenericDfsTransformer](className)

  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String, GenericDataFrame], options: Map[String, String])(implicit context: ActionPipelineContext): Map[String, GenericDataFrame] = {
    val functions = DataFrameSubFeed.getFunctions(dfs.values.head.subFeedType)
    customTransformer.transform(functions, options, dfs)
  }

  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] = {
    customTransformer.transformPartitionValues(options, partitionValues)
  }

  override def factory: FromConfigFactory[GenericDfsTransformer] = ScalaClassDfsTransformer
}
