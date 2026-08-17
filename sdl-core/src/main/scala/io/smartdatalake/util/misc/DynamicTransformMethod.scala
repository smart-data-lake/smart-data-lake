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
package io.smartdatalake.util.misc

import java.lang.reflect.InvocationTargetException
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Values available to map the parameters of a custom transform method when calling it dynamically.
 *
 * @param dfs           input DataFrames of the engines DataFrame type, by name (DataObjectId or name of an
 *                      intermediate DataFrame)
 * @param options       options of the transformation, including runtimeOptions and default options like `isExec`
 * @param engineObjects engine specific objects which can be requested as parameter, e.g. SparkSession,
 *                      DataFrameFunctions or Snowpark Session
 * @param singleInput   if there is only one input DataFrame. In that case a DataFrame parameter is mapped to this
 *                      input DataFrame independent of the parameters name.
 * @param defaultOutputName name to use for a single DataFrame returned by the transform method, if option
 *                      `outputDataObjectId` is not defined.
 */
case class DynamicTransformContext(
    dfs: Map[String, Any] = Map(),
    options: Map[String, String] = Map(),
    engineObjects: Seq[Any] = Seq(),
    singleInput: Boolean = false,
    defaultOutputName: Option[String] = None
)

/**
 * Maps a parameter of a custom transform method to its value.
 *
 * Mappers are engine specific (DataFrame, Dataset, SparkSession, DataFrameFunctions, Snowpark Session, ...) or
 * generic (options, primitive data types, ...), see [[OptionsParameterMapper]]. They are combined into a list which
 * is searched in order for the first mapper handling a given parameter, see [[DynamicTransformMethodWrapper]].
 */
trait TransformParameterMapper {

  /**
   * Map the given parameter to its value, or return None if this mapper does not handle its type.
   */
  def mapParameter(param: MethodParameterInfo, ctx: DynamicTransformContext): Option[Any]

  /**
   * If the given parameter is an input DataFrame, return the name used to look it up in the input DataFrames.
   * This is used to report input DataFrame names of a transformation, see [[TransformInfo]].
   */
  def inputDataFrameName(param: MethodParameterInfo): Option[String] = None
}

/**
 * Converts the return value of a custom transform method into a Map of the engines DataFrame type.
 */
trait TransformReturnMapper {

  /**
   * If the transform method returns one DataFrame (or an engine specific typed variant like a Spark Dataset).
   */
  def returnsSingleDataFrame(returnType: universe.Type): Boolean

  /**
   * If the transform method returns a Map of DataFrames.
   */
  def returnsMultipleDataFrames(returnType: universe.Type): Boolean

  /**
   * Convert the return value into a Map of DataFrames.
   * @param outputName name to use for the entry if the transform method returns a single DataFrame.
   */
  def toDataFrameMap(result: Any, returnType: universe.Type, outputName: => String): Map[String, Any]
}

/**
 * Maps parameters which are independent of the DataFrame engine:
 * - `Map[String,String]` is mapped to the options of the transformation
 * - `Option[<primitive>]` is mapped to the corresponding option, or its default value if the option is not defined
 * - `Seq[<primitive>]` is mapped to the corresponding option split by comma, or its default value
 * - any primitive data type (String, Boolean, Int, ...) is mapped to the corresponding option, using the parameters
 *   default value if the option is not defined.
 *
 * Note that this mapper must be placed after the engine specific mappers, as `Map[String,String]` would also match
 * a Map of DataFrames if the engines DataFrame type is not known.
 */
object OptionsParameterMapper extends TransformParameterMapper {

  override def mapParameter(param: MethodParameterInfo, ctx: DynamicTransformContext): Option[Any] = param match {
    case optionsParam if optionsParam.tpe =:= typeOf[Map[String, String]] => Some(ctx.options)
    case optionalParam if optionalParam.tpe <:< typeOf[Option[_]] =>
      val optionVal = try {
        Some(extractOptionVal(ctx.options, optionalParam, getConverterFor(optionalParam.tpe.typeArgs.head)))
      } catch {
        case _: NotFoundError => optionalParam.defaultValue.flatMap(_.asInstanceOf[Option[Any]])
      }
      Some(optionVal)
    case seqParam if seqParam.tpe <:< typeOf[Seq[_]] =>
      val seqVal = try {
        extractSeqVal(ctx.options, seqParam, getConverterFor(seqParam.tpe.typeArgs.head))
      } catch {
        case ex: NotFoundError => seqParam.defaultValue.map(_.asInstanceOf[Seq[Any]]).getOrElse(throw ex)
      }
      Some(seqVal)
    case defaultParam if defaultParam.defaultValue.isDefined =>
      val defaultVal = try {
        extractOptionVal(ctx.options, defaultParam, getConverterFor(defaultParam.tpe))
      } catch {
        case _: NotFoundError => defaultParam.defaultValue.get
      }
      Some(defaultVal)
    case otherParam if isSupportedPrimitive(otherParam.tpe) =>
      Some(extractOptionVal(ctx.options, otherParam, getConverterFor(otherParam.tpe)))
    case _ => None
  }

  private[smartdatalake] def isSupportedPrimitive(tpe: universe.Type): Boolean = {
    Seq(typeOf[String], typeOf[Boolean], typeOf[Long], typeOf[Int], typeOf[Short], typeOf[Byte], typeOf[Double],
      typeOf[Float]).exists(_ =:= tpe)
  }

  private[smartdatalake] def extractOptionVal(options: Map[String, String], param: MethodParameterInfo, converter: String => Any): Any = {
    val v = options.getOrElse(param.name, throw NotFoundError(s"No value found in options for parameter ${param.name}"))
    try {
      converter(v)
    } catch {
      case e: Exception => throw new IllegalStateException(s"Could not convert value $v for parameter ${param.name} to ${param.tpe}: ${e.getClass.getSimpleName} - ${e.getMessage}")
    }
  }

  private[smartdatalake] def extractSeqVal(options: Map[String, String], param: MethodParameterInfo, converter: String => Any): Seq[Any] = {
    val v = options.getOrElse(param.name, throw NotFoundError(s"No value found in options for parameter ${param.name}"))
    try {
      v.split(",").map(_.trim).filter(_.nonEmpty).map(converter).toList
    } catch {
      case e: Exception => throw new IllegalStateException(s"Could not convert value $v for parameter ${param.name} to ${param.tpe}: ${e.getClass.getSimpleName} - ${e.getMessage}")
    }
  }

  private[smartdatalake] def getConverterFor(tpe: universe.Type): String => Any = {
    tpe match {
      case _ if tpe =:= typeOf[String] => (x: String) => x
      case _ if tpe =:= typeOf[Boolean] => _.toBoolean
      case _ if tpe =:= typeOf[Long] => _.toLong
      case _ if tpe =:= typeOf[Int] => _.toInt
      case _ if tpe =:= typeOf[Short] => _.toShort
      case _ if tpe =:= typeOf[Byte] => _.toByte
      case _ if tpe =:= typeOf[Double] => _.toDouble
      case _ if tpe =:= typeOf[Float] => _.toFloat
      case _ => throw new IllegalStateException(s"Unsupported data type $tpe for conversion from options")
    }
  }
}

/**
 * A wrapper around a custom transform method to analyze its parameters and to call it dynamically.
 *
 * @param method           the custom transform method to call
 * @param parameterMappers mappers to fill the parameters of the method, searched in the given order
 * @param returnMapper     mapper to convert the return value of the method
 */
class DynamicTransformMethodWrapper(
    val method: universe.MethodSymbol,
    parameterMappers: Seq[TransformParameterMapper],
    returnMapper: TransformReturnMapper
) {

  /**
   * Extract parameter info from method.
   * @param instance to extract default parameter values, an object instance implementing this.method has to be provided.
   */
  def getParameterInfo(instance: Option[AnyRef] = None): Seq[MethodParameterInfo] = {
    CustomCodeUtil.analyzeMethodParameters(instance, method)
  }

  def returnsSingleDataFrame: Boolean = returnMapper.returnsSingleDataFrame(method.returnType)

  def returnsMultipleDataFrames: Boolean = returnMapper.returnsMultipleDataFrames(method.returnType)

  /**
   * Names of the input DataFrames requested by the parameters of the transform method, tolerant keys, see
   * [[NameUtil.prepareTolerantKey]].
   */
  def getInputDataFrameNames: Map[String, MethodParameterInfo] = {
    getParameterInfo().flatMap { param =>
      parameterMappers.view.flatMap(_.inputDataFrameName(param)).headOption
        .map(name => (NameUtil.prepareTolerantKey(name), param))
    }.toMap
  }

  /**
   * Dynamically call the transform method.
   * @param instance object instance implementing method
   */
  def call(instance: AnyRef, ctx: DynamicTransformContext): Map[String, Any] = {
    val parameters = getParameterInfo(Some(instance))
    val parameterValues = parameters.map { param =>
      parameterMappers.view.flatMap(_.mapParameter(param, ctx)).headOption
        .getOrElse(throw new IllegalStateException(s"Parameter ${param.name} of transform method has unsupported type ${param.tpe}"))
    }

    // call transform method
    val transformResult = try {
      CustomCodeUtil.callMethod[Any](instance, method, parameterValues)
    } catch {
      case e: InvocationTargetException =>
        // Simplify nested exception to hide reflection complexity in exceptions from custom transformer code.
        val targetException = e.getTargetException
        targetException.setStackTrace(e.getTargetException.getStackTrace ++ e.getStackTrace)
        throw targetException
    }

    returnMapper.toDataFrameMap(transformResult, method.returnType, outputName(ctx))
  }

  private def outputName(ctx: DynamicTransformContext): String = {
    ctx.options.get(DynamicTransformMethodWrapper.OPTION_OUTPUT_DATAOBJECT_ID)
      .orElse(ctx.defaultOutputName)
      .getOrElse(throw new IllegalStateException("Custom transform function returns a single DataFrame/Dataset, but outputDataObjectId is ambiguous." +
        " Modify Action to have only one outputIds entry, use transformers parameter overrideOutputId," +
        " or return a Map[String,DataFrame] from your custom transform function."))
  }
}

object DynamicTransformMethodWrapper {
  // Copy of OptionsGenericDfsTransformer.OPTION_OUTPUT_DATAOBJECT_ID to avoid a dependency from util to workflow package.
  val OPTION_OUTPUT_DATAOBJECT_ID = "outputDataObjectId"

  /**
   * Search the custom transform method of a transformer class.
   * Returns None if the class implements the standard transform method only.
   *
   * @param cls          class of the transformer instance to search
   * @param methodName   name of the method implementing the transformation, normally 'transform'
   * @param isStdMethod  predicate to detect the standard transform method defined by the transformer interface,
   *                     which has to be filtered out.
   * @param helpMsg      message to show if the class has no method with the given name at all
   */
  def findCustomTransformMethod(cls: Class[_], methodName: String, isStdMethod: universe.MethodSymbol => Boolean, helpMsg: String): Option[universe.MethodSymbol] = {
    val transformMethods = CustomCodeUtil.getClassMethodsByName(cls, methodName)
    require(transformMethods.nonEmpty, helpMsg)
    // if there is a method with a different type signature than the standard method, this transformer has a custom transform method, otherwise return None.
    transformMethods.filterNot(isStdMethod).headOption
  }
}

case class NotFoundError(msg: String) extends Exception(msg)
