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
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.spark.DefaultExpressionData
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, OptionsSparkDfTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDsTransformer
import org.apache.spark.sql._

//Prevents this Exception:
// java.lang.UnsupportedOperationException: Unable to find constructor for Product. This could happen if Product is an interface, or a trait without companion object constructor.
abstract class FakeProduct extends Product


/**
 * Configuration of a custom Spark-Dataset transformation between one input and one output (1:1) as Java/Scala Class.
 * Define a transform function which receives a DataObjectId, a Dataset and a map of options and has to return a Dataset.
 * The Java/Scala class has to implement interface [[CustomDsTransformer]].
 *
 * @param name                 name of the transformer
 * @param description          Optional description of the transformer
 * @param transformerClassName class name implementing trait [[CustomDsTransformer]]
 * @param options              Options to pass to the transformation
 * @param runtimeOptions       optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                             The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaClassSparkDsTransformer(override val name: String = "scalaSparkTransform", override val description: Option[String] = None, transformerClassName: String, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsSparkDfTransformer {
  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomDsTransformer[FakeProduct, FakeProduct]](transformerClassName)

  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId, options: Map[String, String])(implicit context: ActionPipelineContext): DataFrame = {
    customTransformer.transformWithTypeConversion(context.sparkSession, options, df, dataObjectId.id)
  }

  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] = {
    customTransformer.transformPartitionValues(options, partitionValues)
  }

  override def factory: FromConfigFactory[GenericDfTransformer] = ScalaClassSparkDsTransformer
}

object ScalaClassSparkDsTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaClassSparkDsTransformer = {
    extract[ScalaClassSparkDsTransformer](config)
  }
}