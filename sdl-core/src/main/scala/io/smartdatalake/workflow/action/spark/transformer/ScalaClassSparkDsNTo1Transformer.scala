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
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{CustomCodeUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.{DefaultExpressionData, EncoderUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformer, OptionsSparkDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.{CustomDfsTransformer, CustomDsNto1Transformer}
import io.smartdatalake.workflow.action.spark.transformer.ParameterResolution.ParameterResolution
import io.smartdatalake.workflow.dataobject.DataObject
import javassist.bytecode.stackmap.TypeTag
import org.apache.hadoop.shaded.com.sun.jersey.server.impl.cdi.AnnotatedTypeImpl
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s.scalap.scalasig.AnnotatedType
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl

import java.lang.reflect.InvocationTargetException
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{MethodSymbol, typeOf}

/**
 * Configuration of a custom Spark-Dataset transformation between N inputs and 1 outputs (N:1) as Java/Scala Class
 * Define a transform function that receives a SparkSession, a map of options and as many DataSets as you want, and that has to return one Dataset.
 * The Java/Scala class has to implement interface [[CustomDsNto1Transformer]].
 *
 * @param description         Optional description of the transformer
 * @param className           Class name implementing trait [[CustomDfsTransformer]]
 * @param options             Options to pass to the transformation
 * @param runtimeOptions      Optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                            The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 * @param parameterResolution By default parameter resolution for transform function uses input Datasets id to match the corresponding parameter name.
 *                            But there are other options, see [[ParameterResolution]].
 * @param outputDatasetId     Optional id of the output Dataset. Default is the id of the Actions first output DataObject.
 */
case class ScalaClassSparkDsNTo1Transformer(override val description: Option[String] = None, className: String, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map(), parameterResolution: ParameterResolution = ParameterResolution.DataObjectId, strictInputValidation: Boolean = false, inputColumnAutoSelect: Boolean = true, outputColumnAutoSelect: Boolean = true, addPartitionValuesToOutput: Boolean = false, outputDatasetId: Option[String] = None) extends OptionsSparkDfsTransformer with SmartDataLakeLogger {
  private val customTransformer = CustomCodeUtil.getClassInstanceByName[CustomDsNto1Transformer](className)
  override val name: String = className

  override def transformSparkWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String, DataFrame], options: Map[String, String])(implicit context: ActionPipelineContext): Map[String, DataFrame] = {
    val thisAction: Action = context.instanceRegistry.getActions.find(_.id == actionId).get
    val inputDOs: Seq[DataObject] = thisAction.inputs
    val outputDO: DataObject = thisAction.outputs.head
    val outputDatasetId = this.outputDatasetId.getOrElse(outputDO.id.id)

    Map(outputDatasetId -> transformWithParamMapping(actionId, context.sparkSession, options, dfs, inputDOs, partitionValues))
  }

  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] = {
    customTransformer.transformPartitionValues(options, partitionValues)
  }

  private val expectedTransformMessage = "CustomDsNTo1Transformer implementations need to implement exactly one method with name 'transform' with this signature:" +
    s"def transform(session: SparkSession, options: Map[String, String], src1Ds: Dataset[A], src2Ds: Dataset[B], <more Datasets if needed>): Dataset[C]"

  private[smartdatalake] def transformWithParamMapping(actionId: ActionId, session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame], inputDOs: Seq[DataObject], partitionValues: Seq[PartitionValues]): DataFrame = {

    // lookup transform method
    val methodName = "transform"
    val mirror = scala.reflect.runtime.currentMirror
    val typeTagSubclass = mirror.classSymbol(customTransformer.getClass).toType
    val transformMethodsOfSubclass = typeTagSubclass.members.filter(_.isMethod).filter(_.name.toString == methodName)
    assert(transformMethodsOfSubclass.size == 1, s"($actionId) [transformers.$name] $expectedTransformMessage")
    val transformMethod = transformMethodsOfSubclass.head

    // prepare parameters.
    // We need to remember the original order of the method parameters to make sure to keep them in the same order in the output
    val transformParameters: Seq[(universe.Symbol, Int)] = transformMethod.info.paramLists.head.zipWithIndex
    val (datasetParams, nonDatasetParams) = transformParameters.partition(param => param._1.typeSignature <:< typeOf[Dataset[_]])
    if (strictInputValidation) {
      assert(datasetParams.size == dfs.size, s"($actionId) [transformers.$name] Number of Dataset-Parameters of transform function does not match number of input DataFrames! datasetParamsWithParamIndex: ${datasetParams.map(_._1.name).mkString(", ")}, dataFrames: ${dfs.keys.mkString(",")}")
    }

    val mappedNonDatasetParams: Seq[(Int, Object)] = nonDatasetParams.map {
      case (param, paramIndex) if param.typeSignature =:= typeOf[SparkSession] => (paramIndex, session)
      case (param, paramIndex) if param.typeSignature =:= typeOf[Map[String, String]] => (paramIndex, options)
      case (param, paramIndex) => throw new IllegalStateException(s"($actionId) [transformers.$name] Transform method parameter $param at index $paramIndex has unsupported type ${param.typeSignature.typeSymbol.name}. Only parameters of type Dataset, SparkSession and Map[String,String] are allowed. " + expectedTransformMessage)
    }

    val mappedDatasetParams: Seq[(Int, Dataset[_])] =
      parameterResolution match {
        case ParameterResolution.DataObjectId => getMappedDatasetParamsBasedOnDataObjectId(actionId, datasetParams, dfs)
        case ParameterResolution.DataObjectOrdering => getMappedDatasetParamsBasedOnOrdering(actionId, datasetParams, inputDOs, dfs)
      }

    // Sort by original parameterIndex and then forget about the index
    val allMappedParams: Seq[Object] = (mappedNonDatasetParams ++ mappedDatasetParams).sortBy(_._1).map(_._2)

    // call method dynamically
    val transformMethodInstance = customTransformer.getClass.getMethods.find(_.getName == methodName).get
    val res =
      try {
        transformMethodInstance.invoke(customTransformer, allMappedParams: _*)
      } catch {
        case e: InvocationTargetException =>
          // Simplify nested exception to hide reflection complexity in exceptions from custom transformer code.
          val targetException = e.getTargetException
          targetException.setStackTrace(e.getTargetException.getStackTrace ++ e.getStackTrace)
          throw targetException
      }

    val outputClassType = transformMethodInstance.getAnnotatedReturnType.getType.asInstanceOf[ParameterizedTypeImpl].getActualTypeArguments.head.getTypeName
    val columsFromCaseClass = getCaseClassColumnNames(outputClassType)

    val resAsDF = res.asInstanceOf[Dataset[_]].toDF
    val resWithSelect = if (outputColumnAutoSelect) resAsDF.select(columsFromCaseClass.map(col): _*) else resAsDF
    val resWithPartitionValues = if (addPartitionValuesToOutput) {
      assert(partitionValues.size == 1, s"When using addPartitionValuesToOutput you can only process one partition-key at a time, but ${partitionValues.size} where given: {${partitionValues.mkString(";")}}")
      partitionValues.head.elements.foldLeft(resWithSelect) {
        (dataframe, pair) => dataframe.withColumn(pair._1, lit(pair._2))
      }
    }
    else {
      resWithSelect
    }
    resWithPartitionValues
  }

  private def getCaseClassColumnNames(className: String): Seq[String] = {
    val outputCaseClass = Environment.classLoader.loadClass(className)
    outputCaseClass.getDeclaredFields.toSeq.map(_.getName)
  }

  private def getMappedDatasetParamsBasedOnOrdering(actionId: ActionId, datasetParamsWithParamIndex: Seq[(universe.Symbol, Int)], inputDOs: Seq[DataObject], dfs: Map[String, DataFrame]) = {
    if (strictInputValidation) {
      assert(datasetParamsWithParamIndex.size == inputDOs.size, s"($actionId) [transformers.$name] Number of Dataset-Parameters of transform function does not match number of input DataObjects! datasetParams: ${datasetParamsWithParamIndex.map(_._1.name).mkString(",")}, inputDOs: ${inputDOs.map(_.id).mkString(",")}")
    }
    datasetParamsWithParamIndex.zipWithIndex.map {
      case ((param, paramIndex), datasetIndex) if param.typeSignature <:< typeOf[Dataset[_]] =>
        val dsType = param.typeSignature.typeArgs.head
        val dataObjectAtThatIndexInConfig = inputDOs(datasetIndex)
        val df = dfs(dataObjectAtThatIndexInConfig.id.id)
        val ds = EncoderUtil.createDataset(df, dsType)
        (paramIndex, ds)
    }
  }

  private def getMappedDatasetParamsBasedOnDataObjectId(actionId: ActionId, datasetParamsWithParamIndex: Seq[(universe.Symbol, Int)], dfs: Map[String, DataFrame]) = {
    datasetParamsWithParamIndex.map {
      case (param, paramIndex) if param.typeSignature <:< typeOf[Dataset[_]] =>
        val paramName = param.name.toString
        val dsType = param.typeSignature.typeArgs.head
        val df = tolerantGet(dfs, paramName).getOrElse(throw new IllegalStateException(s"($actionId) [transformers.$name] DataFrame for DataObject $paramName not found in input DataFrames: ${dfs.keys.mkString(",")}"))
        val dfWithSelect =
          if (inputColumnAutoSelect) {
             val columnNames =  getCaseClassColumnNames(dsType.toString)
            df.select(columnNames.map(col) :_*)
          } else {
            df
          }
        val ds = EncoderUtil.createDataset(dfWithSelect, dsType)
        (paramIndex, ds)
    }
  }

  /**
   * Tolerant lookup of entry in map.
   * Comparison is made case-insensitive and without underscore and hyphen.
   */
  private def tolerantGet[T](map: Map[String, T], key: String): Option[T] = {
    def prepareKey(k: String) = k.toLowerCase.replace("-", "").replace("_", "")

    val tolerantMap = map.map { case (k, v) => (prepareKey(k), v) }
    tolerantMap.get(prepareKey(key))
  }

  override def factory: FromConfigFactory[GenericDfsTransformer] = ScalaClassSparkDsNTo1Transformer
}

object ScalaClassSparkDsNTo1Transformer extends FromConfigFactory[GenericDfsTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ScalaClassSparkDsNTo1Transformer = {
    extract[ScalaClassSparkDsNTo1Transformer](config)
  }
}

/**
 * Methods to match input Datasets to Dataset parameters of transformer function.
 */
object ParameterResolution extends Enumeration {
  type ParameterResolution = Value

  /**
   * Use input Datasets id to match the corresponding parameter name.
   * The Datasets id is equivalent to its input DataObjectId for the first transformer in the chain, later transformers can change the id.
   * The comparison is done case-insensitive without underscore and hyphen.
   */
  val DataObjectId: Value = Value("DataObjectId")

  /**
   * Use the order of the Actions input DataObjects to match the corresponding input Dataset parameters
   * This only works for the first transformer in the chain. It allows to create more generic transformers as the parameter names do not need to match Datasets names.
   */
  val DataObjectOrdering: Value = Value("DataObjectOrdering")
}

