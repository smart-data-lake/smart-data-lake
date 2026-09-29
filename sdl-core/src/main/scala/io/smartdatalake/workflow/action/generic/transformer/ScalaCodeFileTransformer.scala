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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{CustomCodeUtil, DefaultExpressionData, FileUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.customlogic.{CustomFileTransformer, OutputStreamFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.Logger

import java.io.{InputStream, OutputStream}

/**
 * Configuration of a custom file transformation between one input and one output stream (1:1) as Scala code, which
 * is compiled at runtime. The code must either be a function of type
 * `(Map[String,String], InputStream, OutputStream) => Option[Exception]`, or evaluate to an instance of
 * [[CustomFileTransformer]]. Only the latter can also create multiple output files or transform partition values.
 *
 * The code is compiled on the driver and again on every Spark executor, as classes compiled at runtime are not
 * available on the classpath of the executors.
 *
 * Example:
 * {{{
 * actions = {
 *   transform-airports {
 *     type = CustomFileAction
 *     inputId = stg-airports
 *     outputId = int-airports
 *     transformer = {
 *       type = ScalaCodeFileTransformer
 *       code = """
 *         import java.io.{InputStream, OutputStream}
 *         (options: Map[String,String], input: InputStream, output: OutputStream) => {
 *           input.transferTo(output)
 *           None
 *         }
 *       """
 *     }
 *   }
 * }
 * }}}
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param code           Scala code for transformation. Either file or code must be defined.
 * @param file           File where Scala code for transformation is loaded from. Either file or code must be defined.
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaCodeFileTransformer(
    override val name: String = "scalaCodeFileTransform",
    override val description: Option[String] = None,
    code: Option[String] = None,
    file: Option[String] = None,
    options: Map[String, String] = Map(),
    runtimeOptions: Map[String, String] = Map()
) extends OptionsFileTransformer {
  assert(file.isEmpty || code.isEmpty, s"Only one of `file` or `code` must be defined for ScalaCodeFileTransformer")

  // Code is compiled lazily and not serialized, so that it is compiled again on every executor.
  @transient private lazy val customTransformer: CustomFileTransformer = {
    implicit val loggImp: Logger = logger
    val compiledCode = {
      implicit val defaultHadoopConf: Configuration = new Configuration()
      file.map(file => CustomCodeUtil.compileCode[Any](FileUtil.readFromPath(new Path(file))))
        .orElse(code.map(code => CustomCodeUtil.compileCode[Any](code)))
        .getOrElse(throw ConfigurationException(s"Either `file` or `code` must be defined for ScalaCodeFileTransformer"))
    }
    compiledCode match {
      case customTransformer: CustomFileTransformer => customTransformer
      case fn: Function3[_, _, _, _] => new CustomFileTransformerFunctionWrapper(fn.asInstanceOf[ScalaCodeFileTransformer.fnTransformType])
      case x => throw ConfigurationException(s"Code compiled for ScalaCodeFileTransformer must be a function of type" +
        s" (Map[String,String], InputStream, OutputStream) => Option[Exception] or an implementation of CustomFileTransformer," +
        s" but is ${x.getClass.getName}")
    }
  }
  if (!Environment.compileScalaCodeLazy) customTransformer

  override def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = {
    super.prepare(actionId)
    // check lazy parsed transform function
    customTransformer
  }

  override def transformToFiles(options: Map[String, String], input: InputStream, fileName: String, outputs: OutputStreamFactory): Option[Exception] =
    customTransformer.transformToFiles(options, input, fileName, outputs)

  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] =
    customTransformer.transformPartitionValues(options, partitionValues)
}

object ScalaCodeFileTransformer extends FromConfigFactory[GenericFileTransformer] {
  type fnTransformType = (Map[String, String], InputStream, OutputStream) => Option[Exception]

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaCodeFileTransformer =
    extract[ScalaCodeFileTransformer](config)
}

private class CustomFileTransformerFunctionWrapper(fnTransform: ScalaCodeFileTransformer.fnTransformType) extends CustomFileTransformer {
  override def transform(options: Map[String, String], input: InputStream, output: OutputStream): Option[Exception] =
    fnTransform(options, input, output)
}
