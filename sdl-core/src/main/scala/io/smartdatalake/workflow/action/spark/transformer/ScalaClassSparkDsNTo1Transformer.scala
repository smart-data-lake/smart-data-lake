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
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.spark.DefaultExpressionData
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformer, OptionsSparkDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.{CustomDfsTransformer, CustomDsNto1Transformer}
import io.smartdatalake.workflow.dataobject.DataObject
import org.apache.spark.sql.DataFrame

/**
 * Configuration of a custom Spark-Dataset transformation between N inputs and 1 outputs (N:1) as Java/Scala Class
 * Define a transform function that receives a SparkSession, a map of options and as many DataSets as you want and that has to return a Dataset.
 * The Java/Scala class has to implement interface [[CustomDsNto1Transformer]].
 * By default, the Parameter Names of the transform function must match the dataObjectIds provided in the the inputIds list of the config.
 * By setting the parameter parameterResolution= "dataObjectOrdering", you can name your dataObjects however you want,
 * but then the ordering in of the inputIds list must match the ordering of the parameters of the transform function.
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param className      class name implementing trait [[CustomDfsTransformer]]
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaClassSparkDsNTo1Transformer(override val name: String = "ScalaClassSparkDs2To1Transformer", override val description: Option[String] = None, className: String, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map(), parameterResolution: String = "dataObjectId") extends OptionsSparkDfsTransformer {
  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomDsNto1Transformer](className)

  override def transformSparkWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String, DataFrame], options: Map[String, String])(implicit context: ActionPipelineContext): Map[String, DataFrame] = {
    val thisAction: Action = context.instanceRegistry.getActions.find(_.id == actionId).get
    val inputDOs: Seq[DataObject] = thisAction.inputs
    val outputDO: DataObject = thisAction.outputs.head

    Map(outputDO.id.id -> customTransformer.transformWithParamMapping(context.sparkSession, options, dfs, inputDOs, parameterResolution))

  }

  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] = {
    customTransformer.transformPartitionValues(options, partitionValues)
  }

  override def factory: FromConfigFactory[GenericDfsTransformer] = ScalaClassSparkDsNTo1Transformer
}

object ScalaClassSparkDsNTo1Transformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaClassSparkDsNTo1Transformer = {
    extract[ScalaClassSparkDsNTo1Transformer](config)
  }
}
