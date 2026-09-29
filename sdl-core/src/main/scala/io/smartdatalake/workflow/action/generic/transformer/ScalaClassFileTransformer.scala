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
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.customlogic.{CustomFileTransformer, OutputStreamFactory}

import java.io.InputStream

/**
 * Configuration of a custom file transformation as Java/Scala Class, creating one or more output files from one input file.
 * The Java/Scala class has to implement interface [[CustomFileTransformer]].
 * Besides transforming the content of a file, it can also transform the partition values of the output file by
 * implementing [[CustomFileTransformer.transformPartitionValues]], e.g. to standardize partition values extracted
 * from the file path.
 *
 * The class is instantiated by reflection through its no-argument constructor, so it must be on the classpath of the
 * SDLB job. It is serialized and shipped to the Spark executors, where the transformation is executed.
 *
 * Example:
 * {{{
 * actions = {
 *   transform-airports {
 *     type = CustomFileAction
 *     inputId = stg-airports
 *     outputId = int-airports
 *     transformer = {
 *       type = ScalaClassFileTransformer
 *       className = com.sample.CutColumnsFileTransformer
 *       options = { delimiter = "," }
 *     }
 *   }
 * }
 * }}}
 *
 * @param name           name of the transformer
 * @param description    Optional description of the transformer
 * @param className      class name implementing trait [[CustomFileTransformer]]
 * @param options        Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
case class ScalaClassFileTransformer(override val name: String = "scalaFileTransform", override val description: Option[String] = None, className: String, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsFileTransformer {
  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomFileTransformer](className)

  override def transformToFiles(options: Map[String, String], input: InputStream, fileName: String, outputs: OutputStreamFactory): Option[Exception] =
    customTransformer.transformToFiles(options, input, fileName, outputs)

  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] =
    customTransformer.transformPartitionValues(options, partitionValues)
}

object ScalaClassFileTransformer extends FromConfigFactory[GenericFileTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaClassFileTransformer = {
    extract[ScalaClassFileTransformer](config)
  }
}
