/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.smartdatalake.util.evolution

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * With Spark 3.0 the API for Udf's was made more typesafe. It's no longer possible to create a Udf and give it's return type as StructType.
 * This implements an explicitly unsafe unary udf, which takes a function with signature Any -> Any as transformation.
 * UnsafeUnaryUdfExpression is the Udf expression that is embedded and executed in Spark logical plan.
 * Use UnsafeUnaryUdf to create the udf function to be used in Spark DataFrame API.
 */
case class UnsafeUnaryUdfExpression(child: Expression, udf: Any => Any, tgtDataType: DataType) extends UnaryExpression {
  override def dataType: DataType = tgtDataType
  override def nullable: Boolean = child.nullable
  override def nullSafeEval(input: Any): Any = udf(input)
  override def prettyName: String = "unsafe_unary_udf"
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input => s"(${CodeGenerator.boxedType(dataType)})$expr.nullSafeEval($input)")
  }
}


/**
 * This defines a helper method to create the Udf.
 * Note that a Udf is a function which takes Columns as input and returns an derived column.
 * The same UDF can be applied to several columns, each applications generates an UnsafeUnaryUdfExpression as derived column.
 */
object UnsafeUnaryUdf {
  def apply(udf: Any => Any, srcType: DataType, tgtType: DataType): (Column => Column) = {
    (col: Column) => {
      val rowConverter = CatalystTypeConverters.createToScalaConverter(srcType)
      val internalrowConverter = CatalystTypeConverters.createToCatalystConverter(tgtType)
      new Column(UnsafeUnaryUdfExpression(col.expr, v => internalrowConverter(udf(rowConverter(v))), tgtType))
    }
  }
}