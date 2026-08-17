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
package io.smartdatalake.workflow.action.generic.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{CustomCodeUtil, DefaultExpressionData}
import io.smartdatalake.workflow.action.generic.customlogic.CustomGenericDfsTransformer
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Configuration of a custom Spark-DataFrame transformation between many inputs and many outputs (n:m)
 * Define a transform function which receives a map of input DataObjectIds with DataFrames and a map of options and as
 * to return a map of output DataObjectIds with DataFrames, see also trait [[CustomGenericDfsTransformer]].
 *
 * Use this transformer inside a [[io.smartdatalake.workflow.action.CustomDataFrameAction]] whenever a join, union or
 * fan-out is easier to implement in Scala/Java than in SQL. The class is instantiated by reflection through its
 * no-argument constructor and must be on the classpath of the SDLB job. If this is the last transformer of the chain,
 * the returned map must contain an entry for every outputId of the Action.
 *
 * Instead of overwriting the standard transform function, the class can also implement any transform method using
 * parameters of type DataFrameFunctions, Map[String,String], GenericDataFrame and any primitive data type. It is then
 * called dynamically by looking for the parameter values in the input DataFrames and Options, see
 * [[CustomGenericDfsTransformer]].
 *
 * Example:
 * {{{
 * actions = {
 *   join-departures-airports {
 *     type = CustomDataFrameAction
 *     inputIds = [stg-departures, int-airports]
 *     outputIds = [btl-departures-airports]
 *     transformers = [{
 *       type = ScalaClassGenericDfsTransformer
 *       className = com.sample.MyJoinTransformer
 *       options = { joinType = "left" }
 *     }]
 *   }
 * }
 * }}}
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param className      class name implementing trait [[CustomGenericDfsTransformer]]
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 * @param renamedInputIds  optional map of [input DataFrame name, renamed input DataFrame name]. Adapt names of input
 *                         DataFrames to the expected names in the transformation. This is useful if the transformation
 *                         expects specific input names, or if you want to use more generic names in the transformation
 *                         than the actual input DataObjectIds.
 * @param renamedOutputIds optional map of [output DataFrame name, renamed output DataFrame name]. Adapt names of output
 *                         DataFrames of the transformation to the expected names of the output DataObjects or the next
 *                         transformation.
 * @param overrideOutputId override name of output DataFrame, if the transformer returns a single DataFrame, and not a
 *                         Map of type String -> GenericDataFrame. By default, a single DataFrame is named after the
 *                         output DataObjectId of the Action if the action has only one output DataObject. This
 *                         parameter is ignored if the transformation returns multiple DataFrames.
 */
case class ScalaClassGenericDfsTransformer(override val name: String = "scalaTransform", override val description: Option[String] = None, className: String, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map(), renamedInputIds: Map[String, String] = Map(), renamedOutputIds: Map[String, String] = Map(), overrideOutputId: Option[String] = None) extends OptionsGenericDfsTransformer {
  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomGenericDfsTransformer](className)

  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String, GenericDataFrame], options: Map[String, String])(implicit context: ActionPipelineContext): Map[String, GenericDataFrame] = {
    val functions = DataFrameSubFeed.getFunctions(dfs.values.head.subFeedType)
    val mappedInputDfs = dfs.map {
      case (k, v) => (renamedInputIds.getOrElse(k, k), v)
    }
    val optionsPrep = options ++ overrideOutputId.map(OptionsGenericDfsTransformer.OPTION_OUTPUT_DATAOBJECT_ID -> _)
    val outputDfs = customTransformer.transform(functions, optionsPrep, mappedInputDfs)
    outputDfs.map {
      case (k, v) => (renamedOutputIds.getOrElse(k, k), v)
    }
  }

  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] = {
    customTransformer.transformPartitionValues(options, partitionValues)
  }
}

object ScalaClassGenericDfsTransformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaClassGenericDfsTransformer = {
    extract[ScalaClassGenericDfsTransformer](config)
  }
}
