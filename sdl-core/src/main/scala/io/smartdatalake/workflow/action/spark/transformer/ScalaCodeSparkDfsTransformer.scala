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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.spark.DefaultExpressionData
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformer, OptionsSparkDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformerConfig.fnTransformType
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame

/**
 * Configuration of a custom Spark-DataFrame transformation between many inputs and many outputs (n:m) as Scala code which is compiled at runtime.
 * Define a transform function which receives a map of input DataObjectIds with DataFrames and a map of options and has
 * to return a map of output DataObjectIds with DataFrames. The scala code has to implement a function of type [[fnTransformType]].
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param file           File where scala code for transformation is loaded from. The scala code in the file needs to be a function of type [[fnTransformType]].
 * @param code           Scala code for transformation. The scala code needs to be a function of type [[fnTransformType]].
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaCodeSparkDfsTransformer(override val name: String = "scalaSparkTransform", override val description: Option[String] = None, code: Option[String] = None, file: Option[String] = None, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsSparkDfsTransformer {
  private lazy val fnTransform = {
    implicit val defaultHadoopConf: Configuration = new Configuration()
    file.map(file => CustomCodeUtil.compileCode[fnTransformType](HdfsUtil.readHadoopFile(file)))
      .orElse(code.map(code => CustomCodeUtil.compileCode[fnTransformType](code)))
      .getOrElse(throw ConfigurationException(s"Either file or code must be defined for ScalaCodeSparkDfsTransformer"))
  }
  if (!Environment.compileScalaCodeLazy) fnTransform
  override def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = {
    super.prepare(actionId)
    // check lazy parsed transform function
    fnTransform
  }
  override def transformSparkWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,DataFrame], options: Map[String, String])(implicit context: ActionPipelineContext): Map[String,DataFrame] = {
    fnTransform(context.sparkSession, options, dfs)
  }
  override def factory: FromConfigFactory[GenericDfsTransformer] = ScalaCodeSparkDfsTransformer
}

object ScalaCodeSparkDfsTransformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaCodeSparkDfsTransformer = {
    extract[ScalaCodeSparkDfsTransformer](config)
  }
}
