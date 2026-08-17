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
package io.smartdatalake.workflow.action.generic.customlogic

import io.smartdatalake.util.misc._
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Mappers to dynamically call a custom transform method working with engine independent GenericDataFrames,
 * see [[DynamicTransformMethodWrapper]].
 */
private[smartdatalake] object GenericTransformMappers {

  /**
   * Parameter mappers for generic transform methods.
   * Note that the order is important, e.g. `Map[String,GenericDataFrame]` must be checked before `Map[String,String]`
   * of [[OptionsParameterMapper]].
   */
  val parameterMappers: Seq[TransformParameterMapper] =
    Seq(GenericDataFrameMapper, DataFrameFunctionsMapper, GenericDataFrameMapMapper, OptionsParameterMapper)

  /**
   * Maps a parameter of type GenericDataFrame to the input DataFrame with the same name.
   * A `df` prefix is stripped from the parameter name before the lookup, and the lookup is tolerant, see
   * [[NameUtil.tolerantGet]]. If there is only one input DataFrame, it is used independent of the parameters name.
   */
  object GenericDataFrameMapper extends TransformParameterMapper {
    override def mapParameter(param: MethodParameterInfo, ctx: DynamicTransformContext): Option[Any] = {
      if (param.tpe <:< typeOf[GenericDataFrame]) {
        val dfName = param.name.stripPrefix("df")
        val df = if (ctx.singleInput) ctx.dfs.values.headOption else NameUtil.tolerantGet(ctx.dfs, dfName)
        Some(df.getOrElse(throw NotFoundError(s"No DataFrame found with name $dfName for parameter ${param.name}. DataFrames available are ${ctx.dfs.keys.mkString(", ")}.")))
      } else None
    }
    override def inputDataFrameName(param: MethodParameterInfo): Option[String] = {
      if (param.tpe <:< typeOf[GenericDataFrame]) Some(param.name.stripPrefix("df")) else None
    }
  }

  /**
   * Maps a parameter of type DataFrameFunctions, which provides the functions to work with the given
   * GenericDataFrames.
   */
  object DataFrameFunctionsMapper extends TransformParameterMapper {
    override def mapParameter(param: MethodParameterInfo, ctx: DynamicTransformContext): Option[Any] = {
      if (param.tpe =:= typeOf[DataFrameFunctions]) ctx.engineObjects.collectFirst { case functions: DataFrameFunctions => functions }
      else None
    }
  }

  /**
   * Maps a parameter of type Map[String,GenericDataFrame] to all input DataFrames.
   */
  object GenericDataFrameMapMapper extends TransformParameterMapper {
    override def mapParameter(param: MethodParameterInfo, ctx: DynamicTransformContext): Option[Any] = {
      if (param.tpe =:= typeOf[Map[String, GenericDataFrame]]) Some(ctx.dfs.view.mapValues(_.asInstanceOf[GenericDataFrame]).toMap)
      else None
    }
  }

  /**
   * Converts the return value of a generic transform method, which can be a GenericDataFrame or a
   * Map[String,GenericDataFrame].
   */
  object GenericReturnMapper extends TransformReturnMapper {
    override def returnsSingleDataFrame(returnType: universe.Type): Boolean = {
      returnType.exists(rt => rt <:< typeOf[GenericDataFrame])
    }
    override def returnsMultipleDataFrames(returnType: universe.Type): Boolean = {
      returnType.exists(rt => rt <:< typeOf[Map[String, GenericDataFrame]])
    }
    override def toDataFrameMap(result: Any, returnType: universe.Type, outputName: => String): Map[String, Any] = {
      if (returnType <:< typeOf[Map[String, GenericDataFrame]]) {
        result.asInstanceOf[Map[String, GenericDataFrame]]
      } else if (returnType <:< typeOf[GenericDataFrame]) {
        Map(outputName -> result.asInstanceOf[GenericDataFrame])
      } else {
        throw new IllegalStateException(s"Custom transform function has unsupported return type $returnType")
      }
    }
  }
}
