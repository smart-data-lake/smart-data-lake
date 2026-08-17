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
package io.smartdatalake.workflow.action.snowflake.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{CustomCodeUtil, DefaultExpressionData}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformer, OptionsGenericDfsTransformer}
import io.smartdatalake.workflow.action.snowflake.customlogic.CustomSnowparkDfsTransformer
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.snowflake.{SnowparkDataFrame, SnowparkSubFeed}
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject.SnowflakeTableDataObject

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Configuration of a custom Snowpark-DataFrame transformation between many inputs and many outputs (n:m) as Java/Scala Class.
 * Define a transform function which receives a map of input DataObjectIds with DataFrames and a map of options and as
 * to return a map of output DataObjectIds with DataFrames, see also trait [[CustomSnowparkDfsTransformer]].
 *
 * Use this transformer to implement joins or splits between several Snowflake tables in Scala/Java code, which is
 * pushed down to Snowflake and executed by Snowpark. If a single input and output is sufficient, prefer the simpler
 * [[ScalaClassSnowparkDfTransformer]].
 *
 * Instead of overwriting the standard transform function, the class can also implement any transform method using
 * parameters of type Session, Map[String,String], DataFrame and any primitive data type. It is then called
 * dynamically by looking for the parameter values in the input DataFrames and Options, see
 * [[CustomSnowparkDfsTransformer]].
 *
 * Example:
 * {{{
 * actions = {
 *   join-snowpark {
 *     type = CustomDataFrameAction
 *     inputIds = [sf-airports, sf-departures]
 *     outputIds = [sf-departures-enriched]
 *     transformers = [{
 *       type = ScalaClassSnowparkDfsTransformer
 *       className = com.company.transformer.JoinDeparturesSnowparkTransformer
 *       options = { joinType = "left" }
 *     }]
 *   }
 * }
 * }}}
 *
 * @note All input and output DataObjects must be of type SnowflakeTableDataObject, as the Snowpark session is taken
 *       from the Action's first input. The returned map must be keyed by the output DataObject ids.
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param className      class name implementing trait [[CustomSnowparkDfsTransformer]]
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 * @param renamedInputIds  optional map of [input DataFrame name, renamed input DataFrame name]. Adapt names of input
 *                         DataFrames to the expected names in the transformation.
 * @param renamedOutputIds optional map of [output DataFrame name, renamed output DataFrame name]. Adapt names of output
 *                         DataFrames of the transformation to the expected names of the output DataObjects or the next
 *                         transformation.
 * @param overrideOutputId override name of output DataFrame, if the transformer returns a single DataFrame, and not a
 *                         Map of type String -> DataFrame. By default, a single DataFrame is named after the output
 *                         DataObjectId of the Action if the action has only one output DataObject.
 */
case class ScalaClassSnowparkDfsTransformer(name: String = "snowparkScalaTransform",
                                  description: Option[String] = None,
                                  className: String,
                                  options: Map[String, String] = Map(),
                                  runtimeOptions: Map[String, String] = Map(),
                                  renamedInputIds: Map[String, String] = Map(),
                                  renamedOutputIds: Map[String, String] = Map(),
                                  overrideOutputId: Option[String] = None
                                 )
  extends OptionsGenericDfsTransformer {

  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomSnowparkDfsTransformer](className)

  def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame], options: Map[String,String])(implicit context: ActionPipelineContext): Map[String,GenericDataFrame] = {
    assert(dfs.values.forall(_.isInstanceOf[SnowparkDataFrame]), s"($actionId) Unsupported subFeedType(s) ${dfs.values.filterNot(_.isInstanceOf[SparkDataFrame]).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method transform")
    val action = context.instanceRegistry.get[Action](actionId)
    val snowparkSession = action.inputs.head.asInstanceOf[SnowflakeTableDataObject].snowparkSession
    val snowparkDfs = dfs.map {
      case (k, v) => (renamedInputIds.getOrElse(k, k), v.asInstanceOf[SnowparkDataFrame].inner)
    }
    val optionsPrep = options ++ overrideOutputId.map(OptionsGenericDfsTransformer.OPTION_OUTPUT_DATAOBJECT_ID -> _)
    customTransformer.transform(snowparkSession, optionsPrep, snowparkDfs)
      .map { case (k, v) => (renamedOutputIds.getOrElse(k, k), SnowparkDataFrame(v)) }
  }

  override def getSubFeedSupportedType: Type = typeOf[SnowparkSubFeed]
}

// This companion object ensures that SnowparkDfsTransformer can be parsed from the configuration
object ScalaClassSnowparkDfsTransformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaClassSnowparkDfsTransformer = {
    extract[ScalaClassSnowparkDfsTransformer](config)
  }
}
