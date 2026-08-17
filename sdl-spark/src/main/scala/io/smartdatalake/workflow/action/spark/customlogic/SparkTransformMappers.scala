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
package io.smartdatalake.workflow.action.spark.customlogic

import io.smartdatalake.util.misc._
import io.smartdatalake.util.spark.SparkProductUtil
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Mappers to dynamically call a custom transform method working with Spark DataFrames and Datasets,
 * see [[DynamicTransformMethodWrapper]].
 */
private[smartdatalake] object SparkTransformMappers {

  /**
   * Parameter mappers for Spark transform methods.
   * Note that the order is important, e.g. `Map[String,DataFrame]` must be checked before `Map[String,String]`
   * of [[OptionsParameterMapper]].
   */
  val parameterMappers: Seq[TransformParameterMapper] =
    Seq(DataFrameMapper, DatasetMapper, SparkSessionMapper, DataFrameMapMapper, OptionsParameterMapper)

  /**
   * Maps a parameter of type DataFrame to the input DataFrame with the same name.
   * A `df` prefix is stripped from the parameter name before the lookup, and the lookup is tolerant, see
   * [[NameUtil.tolerantGet]]. If there is only one input DataFrame, it is used independent of the parameters name.
   */
  object DataFrameMapper extends TransformParameterMapper {
    override def mapParameter(param: MethodParameterInfo, ctx: DynamicTransformContext): Option[Any] = {
      if (param.tpe <:< typeOf[DataFrame]) Some(getInputDf(ctx, param, param.name.stripPrefix("df")))
      else None
    }
    override def inputDataFrameName(param: MethodParameterInfo): Option[String] = {
      if (param.tpe <:< typeOf[DataFrame]) Some(param.name.stripPrefix("df")) else None
    }
  }

  /**
   * Maps a parameter of type Dataset[<Product>] to the input DataFrame with the same name, converting it to a
   * typed Dataset. A `ds` prefix is stripped from the parameter name before the lookup.
   */
  object DatasetMapper extends TransformParameterMapper {
    override def mapParameter(param: MethodParameterInfo, ctx: DynamicTransformContext): Option[Any] = {
      if (param.tpe <:< typeOf[Dataset[_]]) {
        val dsType = param.tpe.typeArgs.head
        val df = getInputDf(ctx, param, param.name.stripPrefix("ds"))
        val columnNames = ProductUtil.classAccessorNames(dsType)
        val dfWithSelect = df.select(columnNames.map(col).toIndexedSeq: _*)
        Some(SparkProductUtil.createDataset(dfWithSelect, dsType))
      } else None
    }
    override def inputDataFrameName(param: MethodParameterInfo): Option[String] = {
      if (param.tpe <:< typeOf[Dataset[_]]) Some(param.name.stripPrefix("ds")) else None
    }
  }

  /**
   * Maps a parameter of type SparkSession.
   */
  object SparkSessionMapper extends TransformParameterMapper {
    override def mapParameter(param: MethodParameterInfo, ctx: DynamicTransformContext): Option[Any] = {
      if (param.tpe =:= typeOf[SparkSession]) ctx.engineObjects.collectFirst { case session: SparkSession => session }
      else None
    }
  }

  /**
   * Maps a parameter of type Map[String,DataFrame] to all input DataFrames.
   */
  object DataFrameMapMapper extends TransformParameterMapper {
    override def mapParameter(param: MethodParameterInfo, ctx: DynamicTransformContext): Option[Any] = {
      if (param.tpe =:= typeOf[Map[String, DataFrame]]) Some(ctx.dfs.view.mapValues(_.asInstanceOf[DataFrame]).toMap)
      else None
    }
  }

  /**
   * Converts the return value of a Spark transform method, which can be a DataFrame, a Dataset[<Product>],
   * a Map[String,DataFrame] or a Map[String,Dataset[<Product>]].
   */
  object SparkReturnMapper extends TransformReturnMapper {
    override def returnsSingleDataFrame(returnType: universe.Type): Boolean = {
      returnType.exists(rt => rt =:= typeOf[DataFrame] || rt <:< typeOf[Dataset[_]])
    }
    override def returnsMultipleDataFrames(returnType: universe.Type): Boolean = {
      returnType.exists(rt => rt =:= typeOf[Map[String, DataFrame]] || rt <:< typeOf[Map[String, Dataset[_]]])
    }
    override def toDataFrameMap(result: Any, returnType: universe.Type, outputName: => String): Map[String, Any] = {
      if (returnType =:= typeOf[Map[String, DataFrame]]) {
        result.asInstanceOf[Map[String, DataFrame]]
      } else if (returnType <:< typeOf[Map[String, Dataset[_]]]) {
        result.asInstanceOf[Map[String, Dataset[_]]].view.mapValues(_.toDF()).toMap
      } else if (returnType =:= typeOf[DataFrame]) {
        Map(outputName -> result.asInstanceOf[DataFrame])
      } else if (returnType <:< typeOf[Dataset[_]]) {
        Map(outputName -> result.asInstanceOf[Dataset[_]].toDF())
      } else {
        throw new IllegalStateException(s"Custom transform function has unsupported return type $returnType")
      }
    }
  }

  private def getInputDf(ctx: DynamicTransformContext, param: MethodParameterInfo, dfName: String): DataFrame = {
    val df = if (ctx.singleInput) ctx.dfs.values.headOption else NameUtil.tolerantGet(ctx.dfs, dfName)
    df.map(_.asInstanceOf[DataFrame])
      .getOrElse(throw NotFoundError(s"No DataFrame found with name $dfName for parameter ${param.name}. DataFrames available are ${ctx.dfs.keys.mkString(", ")}."))
  }
}
