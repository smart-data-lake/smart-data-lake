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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.spark.DefaultExpressionData
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig.fnTransformType
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, OptionsSparkDfTransformer}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Configuration of a custom Spark-DataFrame transformation between one input and one output (1:1) as Scala code which is compiled at runtime.
 * Define a transform function which receives a DataObjectId, a DataFrame and a map of options and has to return a
 * DataFrame. The scala code has to implement a function of type [[fnTransformType]].
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param file           File where scala code for transformation is loaded from. The scala code in the file needs to be a function of type [[fnTransformType]].
 * @param code           Scala code for transformation. The scala code needs to be a function of type [[fnTransformType]].
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaCodeDfTransformer(override val name: String = "scalaTransform", override val description: Option[String] = None, code: Option[String] = None, file: Option[String] = None, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsSparkDfTransformer {
  private val fnTransform = {
    implicit val defaultHadoopConf: Configuration = new Configuration()
    file.map(file => CustomCodeUtil.compileCode[fnTransformType](HdfsUtil.readHadoopFile(file)))
      .orElse(code.map(code => CustomCodeUtil.compileCode[fnTransformType](code)))
      .getOrElse(throw ConfigurationException(s"Either file or code must be defined for ScalaCodeTransformer"))
  }
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId, options: Map[String, String])(implicit context: ActionPipelineContext): DataFrame = {
    fnTransform(context.sparkSession, options, df, dataObjectId.id)
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = ScalaCodeDfTransformer
}

object ScalaCodeDfTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaCodeDfTransformer = {
    extract[ScalaCodeDfTransformer](config)
  }
}