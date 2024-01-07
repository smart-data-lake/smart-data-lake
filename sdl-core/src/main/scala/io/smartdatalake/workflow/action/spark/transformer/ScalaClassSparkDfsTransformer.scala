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
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformer, OptionsSparkDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import org.apache.spark.sql.DataFrame

/**
 * Configuration of a custom Spark-DataFrame transformation between many inputs and many outputs (n:m)
 *
 * To define the transformation a class implementing the trait [[CustomDfsTransformer]] has to be created.
 * There are two methods to define of defining transformation:
 *
 * 1) Overwrite the generic transform function of CustomDfsTransformer: Define a transform function which receives a map of input DataObjectIds with DataFrames and a map of options and has
 * to return a map of output DataObjectIds with DataFrames. The exact signature is `transform(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]): Map[String,DataFrame]`.
 *
 * 2) Implement any transform method using parameters of type SparkSession, Map[String,String], DataFrame, Dataset[<Product>] and any primitive data type (String, Boolean, Int, ...).
 * Primitive data types might also use default values or be enclosed in an Option[...] to mark it as non required.
 * The transform method is then called dynamically by looking for the parameter values in the input DataFrames and Options.
 * Using this method you can avoid writing code to prepre DataFrames and Options from the corresponding Map parameters.
 * It also allows the UI to display input parameter details of your transformation.
 *
 * Note that the following options are passed by default to the transformation:
 * - isExec: defined as `context.isExecPhase`
 * - outputDataObjectId: defined as outputDataObject.id if transformation has only one output DataObject configured.
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param className      class name implementing trait [[CustomDfsTransformer]]
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaClassSparkDfsTransformer(override val name: String = "scalaSparkTransform", override val description: Option[String] = None, className: String, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsSparkDfsTransformer {
  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomDfsTransformer](className)
  override def transformSparkWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,DataFrame], options: Map[String, String])(implicit context: ActionPipelineContext): Map[String,DataFrame] = {
    customTransformer.transform(context.sparkSession, options, dfs)
  }
  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = {
   customTransformer.transformPartitionValues(options, partitionValues)
  }
  override def factory: FromConfigFactory[GenericDfsTransformer] = ScalaClassSparkDfsTransformer
}


object ScalaClassSparkDfsTransformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaClassSparkDfsTransformer = {
    extract[ScalaClassSparkDfsTransformer](config)
  }
}
