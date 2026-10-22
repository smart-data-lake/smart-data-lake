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
package io.smartdatalake.workflow.dataframe.plainScala

import ch.zzeekk.spark.expressions.{ExpressionEvaluator, ExpressionEvaluatorFactory}
import io.smartdatalake.util.misc.ProductUtil

import scala.reflect.ClassTag
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.util.Try

/**
 * [[ExpressionEvaluatorFactory]] implementation based on the plain-Scala engine's [[ExpressionParser]].
 *
 * It is used as fallback if no Spark expression library (spark-extensions or spark-expressions-standalone)
 * is in the classpath. Note that the [[ExpressionParser]] only supports a limited subset of the
 * Spark SQL expression grammar, e.g. there is no support for UDFs or window functions.
 */
class ScalaExpressionEvaluatorFactory extends ExpressionEvaluatorFactory {

  override def getEvaluator[T <: Product : TypeTag, R : TypeTag : ClassTag](expression: String): ExpressionEvaluator[T, R] =
    new ScalaExpressionEvaluator[T, R](expression)

  override def registerUdf[RT: TypeTag](name: String, func: () => RT): Unit = throw udfsNotSupported
  override def registerUdf[RT: TypeTag, A1: TypeTag](name: String, func: A1 => RT): Unit = throw udfsNotSupported
  override def registerUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag](name: String, func: (A1, A2) => RT): Unit = throw udfsNotSupported
  override def registerUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](name: String, func: (A1, A2, A3) => RT): Unit = throw udfsNotSupported
  override def registerUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](name: String, func: (A1, A2, A3, A4) => RT): Unit = throw udfsNotSupported
  override def registerUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](name: String, func: (A1, A2, A3, A4, A5) => RT): Unit = throw udfsNotSupported
  override def registerUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](name: String, func: (A1, A2, A3, A4, A5, A6) => RT): Unit = throw udfsNotSupported
  override def registerUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](name: String, func: (A1, A2, A3, A4, A5, A6, A7) => RT): Unit = throw udfsNotSupported
  override def registerUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](name: String, func: (A1, A2, A3, A4, A5, A6, A7, A8) => RT): Unit = throw udfsNotSupported
  override def registerUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](name: String, func: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => RT): Unit = throw udfsNotSupported
  override def registerUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](name: String, func: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => RT): Unit = throw udfsNotSupported

  private def udfsNotSupported = new NotImplementedError("registerUdf is not supported by ScalaExpressionEvaluatorFactory")
}

object ScalaExpressionEvaluatorFactory extends ScalaExpressionEvaluatorFactory

/**
 * Evaluates an expression against a case class instance, using the plain-Scala engine's [[ExpressionParser]].
 *
 * The case class attributes are converted to a one-row [[ScalaDataFrame]] and the expression is evaluated
 * as additional column on it. Attributes with data types not supported by [[ScalaDataType]] are omitted
 * and can not be referenced in the expression.
 */
class ScalaExpressionEvaluator[T <: Product : TypeTag, R : TypeTag : ClassTag](expression: String) extends ExpressionEvaluator[T, R] {

  // parse eagerly to validate the expression syntax on creation
  ExpressionParser.parse(expression)(ScalaSubFeed)

  override def apply(data: T): R = {
    import ScalaExpressionEvaluator._
    val df = createDataFrameTolerant(data)
    // the expression is parsed again for every evaluation, as the parsed column tree is stateful
    val dfResult = df.withColumn(resultColName, ExpressionParser.parse(expression)(ScalaSubFeed))
    val row = dfResult.select(resultColName).collect.head.asInstanceOf[ScalaRow]
    row(0).orNull.asInstanceOf[R]
  }
}

object ScalaExpressionEvaluator {
  private val resultColName = "_expressionResult"

  /**
   * Same as [[ScalaDataFrame.fromData]] for a single case class instance,
   * but tolerates and omits attributes with data types not supported by [[ScalaDataType]].
   */
  private def createDataFrameTolerant[T <: Product : TypeTag](data: T): ScalaDataFrame = {
    val accessors = ProductUtil.classAccessors(typeTag[T].tpe)
    val colDefs = accessors.flatMap { acc =>
      Try(ScalaDataType.getFor(currentMirror.runtimeClass(acc.returnType)).createColumnDefinition(acc.name.toTermName.toString)).toOption
    }
    val schema = ScalaSchema(colDefs)
    ScalaDataFrame.fromRows(Seq(ScalaRow(schema.columns.map(ProductUtil.getRawFieldData(data, _)).toIndexedSeq)), Some(schema))
  }
}
