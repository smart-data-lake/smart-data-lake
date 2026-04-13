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
package io.smartdatalake.workflow.action.spark.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{CustomCodeUtil, DefaultExpressionData, FileUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.{CanRecompileFromSrc, GenericDfsTransformer, OptionsGenericDfsTransformer, OptionsSparkDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed.getSparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
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
 * Using this method you can avoid writing code to prepare DataFrames and Options from the corresponding Map parameters.
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
 * @param renamedInputIds optional map of [input DataFrame name, renamed input DataFrame name].
 *                        Adapt names of input DataFrames to the expected names in the transformation.
 *                        This is useful if the transformation expects specific input names, or if you want to use more
 *                        generic names in the transformation than the actual input DataObjectIds.
 * @param renamedOutputIds optional map of [output DataFrame name, renamed output DataFrame name].
 *                         Adapt names of output DataFrames of the transformation to the expected names of the output DataObjects or the next transformation.
 *                         This is useful if the transformation returns DataFrames with specific names, or if you want to use more
 *                         generic names in the transformation than the actual output DataObjectIds.
 * @param overrideOutputId override name of output DataFrame, if the transformer returns a single DataFrame/Dataset, and not a Map of type String -> DataFrame.
 *                         By default, a single DataFrame/Dataset is named after the output DataObjectId of the Action if the action has only one output DataObject.
 *                         With this parameter you can explicitly define the name of the output DataFrame/Dataset independent of the output DataObjectId of the Action.
 *                         This parameter is ignored if the transformation returns multiple DataFrames.
 */
case class ScalaClassSparkDfsTransformer(override val name: String = "scalaSparkTransform",
                                         override val description: Option[String] = None,
                                         className: String,
                                         options: Map[String, String] = Map(),
                                         runtimeOptions: Map[String, String] = Map(),
                                         renamedInputIds: Map[String, String] = Map(),
                                         renamedOutputIds: Map[String, String] = Map(),
                                         overrideOutputId: Option[String] = None
                                        ) extends OptionsSparkDfsTransformer with CanRecompileFromSrc with SmartDataLakeLogger {
  private var customTransformer = CustomCodeUtil.getClassInstanceByName[CustomDfsTransformer](className)

  override def recompileFromSrc(srcDir: String): Unit = {
    implicit val defaultHadoopConf: Configuration = new Configuration()
    val file = s"$srcDir/${className.replace('.', '/')}.scala"
    logger.info(s"recompiling $file")
    val code = (FileUtil.readFromPath(new Path(file)).linesIterator.toSeq :+ s"new ${className.split('.').last}()")
      .dropWhile(x => !(x.startsWith("import") || x.startsWith("class"))).mkString("\n")
    customTransformer = CustomCodeUtil.compileCode[CustomDfsTransformer](code)
  }

  override def transformSparkWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], inputDfs: Map[String, DataFrame], options: Map[String, String])(implicit context: ActionPipelineContext): Map[String, DataFrame] = {
    val mappedInputDfs = inputDfs.map {
      case (k,v) => (renamedInputIds.getOrElse(k, k), v)
    }
    val optionsPrep = options ++ overrideOutputId.map(OptionsGenericDfsTransformer.OPTION_OUTPUT_DATAOBJECT_ID -> _)
    val outputDfs = customTransformer.transform(getSparkSession, optionsPrep, mappedInputDfs)
    outputDfs.map {
      case (k,v) => (renamedOutputIds.getOrElse(k, k), v)
    }
  }

  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] = {
    customTransformer.transformPartitionValues(options, partitionValues)
  }

  override def factory: FromConfigFactory[GenericDfsTransformer] = ScalaClassSparkDfsTransformer
}

object ScalaClassSparkDfsTransformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaClassSparkDfsTransformer = {
    extract[ScalaClassSparkDfsTransformer](config)
  }
}
