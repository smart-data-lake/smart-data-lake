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
package io.smartdatalake.workflow.action.spark.customlogic

import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{CustomCodeUtil, MethodParameterInfo, ProductUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.DefaultExpressionData
import io.smartdatalake.workflow.action.generic.transformer.OptionsGenericDfsTransformer.OPTION_OUTPUT_DATAOBJECT_ID
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformerDef, SQLDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer.{extractOptionVal, getConverterFor}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformerConfig.fnTransformType
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDsNTo1Transformer.{prepareTolerantKey, tolerantGet}
import io.smartdatalake.workflow.action.spark.transformer.{ScalaClassSparkDfsTransformer, ScalaClassSparkDsNTo1Transformer, ScalaCodeSparkDfsTransformer}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.lang.reflect.InvocationTargetException
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf
import scala.reflect.runtime.universe.TypeTag

/**
 * Interface to define a custom Spark-DataFrame transformation (n:m)
 * Same trait as [[CustomDfTransformer]], but multiple input and outputs supported.
 */
trait CustomDfsTransformer extends CustomTransformMethodDef with Serializable with SmartDataLakeLogger {

  /**
   * Function to define the transformation between several input and output DataFrames (n:m).
   *
   * Note that the default implementation is looking for an implementation of a 'transform' function with custom parameters,
   * which it will call dynamically.
   *
   * @param session Spark Session
   * @param options Options specified in the configuration for this transformation
   * @param dfs DataFrames to be transformed
   * @return Map of transformed DataFrames
   */
  def transform(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]) : Map[String,DataFrame] = {
    require(customTransformMethod.isDefined, s"${this.getClass.getSimpleName} transform method is not overridden and no custom transform method is defined")
    val methodWrapper = new CustomTransformMethodWrapper(customTransformMethod.get)
    require(methodWrapper.returnsSingleDataset || methodWrapper.returnsMultipleDatasets, s"The return type of the transform method is ${customTransformMethod.get.returnType} but should be one of DataFrame, Dataset[_], Map[String,DataFrame] or Map[String,Dataset[_]]")
    methodWrapper.call(this, dfs, options)(session)
  }

  /**
   * Optional function to define the transformation of input to output partition values.
   * Use cases:
   * - implement aggregations where multiple input partitions are combined into one output partition
   * - add additional fixed partition values to write from different actions into the same target tables but separated by different partition values
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   *
   * @param partitionValues partition values to be transformed
   * @param options Options specified in the configuration for this transformation
   * @return a map of input partition values to output partition values
   */
  def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues,PartitionValues]] = None

  // lookup custom transform method
  @transient override private[smartdatalake] lazy val customTransformMethod = {
    val transformMethods = CustomCodeUtil.getClassMethodsByName(getClass, "transform")
      .filter(_.owner != typeOf[CustomDfsTransformer].typeSymbol) // remove default transform-method implementation of CustomDfsTransformer
    require(transformMethods.size == 1, """
                                                   | CustomDfsTransformer implementations need to implement exactly one method with name 'transform'.
                                                   | Traditionally the signature of the transform method is 'transform(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]): Map[String,DataFrame]',
                                                   | but since SDLB 2.6 you can also implement any transform method using parameters of type SparkSession, Map[String,String], DataFrame, Dataset[<Product>] and any primitive data type (String, Boolean, Int, ...).
                                                   | Primitive data types might also use default values or be enclosed in an Option[...] to mark it as non required.
                                                   | The transform method is then called dynamically by looking for the parameter values in the input DataFrames and Options.
      """)
    // if type signature is different from default method, this transformer has a custom transform method, otherwise return None.
    import scala.reflect.runtime.universe._
    val defaultTransformMethod = typeOf[CustomDfsTransformer].member(TermName("transform"))
    transformMethods.find(_.typeSignature != defaultTransformMethod.typeSignature)
  }
}

/**
 * Configuration of a custom Spark-DataFrame transformation between many inputs and many outputs (n:m).
 * Define a transform function which receives a map of input DataObjectIds with DataFrames and a map of options and has
 * to return a map of output DataObjectIds with DataFrames, see also trait [[CustomDfsTransformer]].
 *
 * @param className Optional class name implementing trait [[CustomDfsTransformer]]
 * @param scalaFile Optional file where scala code for transformation is loaded from. The scala code in the file needs to be a function of type [[fnTransformType]].
 * @param scalaCode Optional scala code for transformation. The scala code needs to be a function of type [[fnTransformType]].
 * @param sqlCode Optional map of output DataObject id and corresponding SQL Code.
 *                Use tokens %{<key>} to replace with runtimeOptions in SQL code.
 *                Example: "select * from test where run = %{runId}"
 * @param options Options to pass to the transformation
 * @param runtimeOptions optional tuples of [key, spark sql expression] to be added as additional options when executing transformation.
 *                       The spark sql expressions are evaluated against an instance of [[DefaultExpressionData]].
 */
@deprecated("use transformers instead")
case class CustomDfsTransformerConfig( className: Option[String] = None, scalaFile: Option[String] = None, scalaCode: Option[String] = None, sqlCode: Option[Map[String,String]] = None, options: Option[Map[String,String]] = None, runtimeOptions: Option[Map[String,String]] = None) {
  require(className.isDefined || scalaFile.isDefined || scalaCode.isDefined || sqlCode.isDefined, "Either className, scalaFile, scalaCode or sqlCode must be defined for CustomDfsTransformer")

  // Load Transformer code from appropriate location
  val impl: GenericDfsTransformerDef = className.map(clazz => ScalaClassSparkDfsTransformer(className = clazz, options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map())))
    .orElse {
      scalaFile.map(file => ScalaCodeSparkDfsTransformer(file = Some(file), options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map())))
    }.orElse{
    scalaCode.map(code => ScalaCodeSparkDfsTransformer(code = Some(code), options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map())))
  }.orElse {
    sqlCode.map(code => SQLDfsTransformer(code = code, options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map())))
  }.get

  override def toString: String = {
    if(className.isDefined)       "className: "+className.get
    else if(scalaFile.isDefined)  "scalaFile: "+scalaFile.get
    else if(scalaCode.isDefined)  "scalaCode: "+scalaCode.get
    else                          "sqlCode: "+sqlCode.get
  }
}

object CustomDfsTransformerConfig {
  type fnTransformType = (SparkSession, Map[String,String], Map[String,DataFrame]) => Map[String,DataFrame]
}

object CustomDfsTransformer {
  def extractOptionVal(options: Map[String,String], param: MethodParameterInfo, converter: String => Any): Any = {
    val v = options.getOrElse(param.name, throw NotFoundError(s"No value found in options for parameter ${param.name}"))
    try {
      converter(v)
    } catch {
      case e: Exception => throw new IllegalStateException(s"Could not convert value $v for parameter ${param.name} to ${param.tpe}: ${e.getClass.getSimpleName} - ${e.getMessage}")
    }
  }
  def getConverterFor(tpe: universe.Type): String => Any = {
    tpe match {
      case _ if tpe =:= typeOf[String] => (x: String) => x
      case _ if tpe =:= typeOf[Boolean] => _.toBoolean
      case _ if tpe =:= typeOf[Long] => _.toLong
      case _ if tpe =:= typeOf[Int] => _.toInt
      case _ if tpe =:= typeOf[Short] => _.toShort
      case _ if tpe =:= typeOf[Byte] => _.toByte
      case _ if tpe =:= typeOf[Double] => _.toDouble
      case _ if tpe =:= typeOf[Float] => _.toFloat
    }
  }
}

/**
 * A trait for all transformers having a custom transform method which allows to extract detailed transformation parameter informations.
 */
trait CustomTransformMethodDef {
  private[smartdatalake] def customTransformMethod: Option[universe.MethodSymbol]
}

class CustomTransformMethodWrapper(method: universe.MethodSymbol) {

  private[smartdatalake] final def getParameterInfo: Seq[MethodParameterInfo] = {
    CustomCodeUtil.analyzeMethodParameters(this, method)
  }

  private[smartdatalake] def returnsSingleDataset: Boolean = {
    method.returnType.exists(rt => rt =:= typeOf[DataFrame] || rt <:< typeOf[Dataset[_]])
  }

  private[smartdatalake] def returnsMultipleDatasets: Boolean = {
    method.returnType.exists(rt => rt =:= typeOf[Map[String, DataFrame]] || rt <:< typeOf[Map[String, Dataset[_]]])
  }

  private[smartdatalake] def getInputDataObjectNames[T : TypeTag]: Map[String, MethodParameterInfo] = {
    getParameterInfo.collect {
      case dfParam if dfParam.tpe <:< typeOf[DataFrame] => (prepareTolerantKey(dfParam.name.stripPrefix("df")), dfParam)
      case dsParam if dsParam.tpe <:< typeOf[Dataset[T]] => (prepareTolerantKey(dsParam.name.stripPrefix("ds")), dsParam)
    }.toMap
  }

  private[smartdatalake] def call(instance: AnyRef, dfs: Map[String,DataFrame], options: Map[String,String])(implicit session: SparkSession) = {
    val transformParameters = getParameterInfo
    val returnType = method.returnType
    val mappedParameters = transformParameters.map {
      case dfParam if dfParam.tpe <:< typeOf[DataFrame] =>
        val paramName = dfParam.name
        val dfName = paramName.stripPrefix("df")
        val df = tolerantGet(dfs, dfName)
          .getOrElse(throw NotFoundError(s"No DataFrame found with name $dfName for parameter $paramName"))
        (dfParam, df)
      case dsParam if dsParam.tpe <:< typeOf[Dataset[_]] =>
        val paramName = dsParam.name
        val dsName = paramName.stripPrefix("ds")
        val dsType = dsParam.tpe.typeArgs.head
        val df = tolerantGet(dfs, dsName)
          .getOrElse(throw NotFoundError(s"No DataFrame found with name $dsName for parameter $paramName"))
        val dfWithSelect = {
          val columnNames = ProductUtil.classAccessorNames(dsType)
          df.select(columnNames.map(col): _*)
        }
        val ds = ProductUtil.createDataset(dfWithSelect, dsType)
        (dsParam, ds)
      case sessionParam if sessionParam.tpe =:= typeOf[SparkSession] => (sessionParam, session)
      case optionsParam if optionsParam.tpe =:= typeOf[Map[String, String]] => (optionsParam, options)
      case optionalParam if optionalParam.tpe <:< typeOf[Option[_]] =>
        val optionVal = try {
          Some(extractOptionVal(options, optionalParam, getConverterFor(optionalParam.tpe.typeArgs.head)))
        } catch {
          case _: NotFoundError => optionalParam.defaultValue.map(_.asInstanceOf[Option[Any]]).getOrElse(None)
        }
        (optionalParam, optionVal)
      case defaultParam if defaultParam.defaultValue.isDefined =>
        val defaultVal = try {
          extractOptionVal(options, defaultParam, getConverterFor(defaultParam.tpe))
        } catch {
          case _: NotFoundError => defaultParam.defaultValue.get
        }
        (defaultParam, defaultVal)
      case otherParam => (otherParam, extractOptionVal(options, otherParam, getConverterFor(otherParam.tpe)))
    }

    // call transform method
    val transformResult = try {
      CustomCodeUtil.callMethod[Any](instance, method, mappedParameters.map(_._2))
    } catch {
      case e: InvocationTargetException =>
        // Simplify nested exception to hide reflection complexity in exceptions from custom transformer code.
        val targetException = e.getTargetException
        targetException.setStackTrace(e.getTargetException.getStackTrace ++ e.getStackTrace)
        throw targetException
    }

    // distinguish between returning single and multiple DataFrames.
    if (returnType =:= typeOf[Map[String, DataFrame]]) {
      transformResult.asInstanceOf[Map[String, DataFrame]]
    } else if (returnType <:< typeOf[Map[String, Dataset[_]]]) {
      transformResult.asInstanceOf[Map[String, Dataset[_]]].mapValues(_.toDF).toMap
    } else if (returnType =:= typeOf[DataFrame]) {
      require(options.isDefinedAt(OPTION_OUTPUT_DATAOBJECT_ID), "Custom transform function returns a single DataFrame, but outputDataObjectId is ambigous. Modify Action to have only one outputIds entry, or return a Map[String,DataFrame] from your custom transform function." )
      Map(options(OPTION_OUTPUT_DATAOBJECT_ID) -> transformResult.asInstanceOf[DataFrame])
    } else if (returnType <:< typeOf[Dataset[_]]) {
      require(options.isDefinedAt(OPTION_OUTPUT_DATAOBJECT_ID), "Custom transform function returns a single Dataset, but outputDataObjectId is ambigous. Modify Action to have only one outputIds entry, or return a Map[String,Dataset] from your custom transform function." )
      Map(options(OPTION_OUTPUT_DATAOBJECT_ID) -> transformResult.asInstanceOf[Dataset[_]].toDF)
    } else {
      throw new IllegalStateException(s"Custom transform function has unsupported return type ${returnType}")
    }
  }
 }

case class NotFoundError(msg: String) extends Exception(msg)