
package org.apache.spark.sql.avro.confluent

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.DataType

case class SetNullable(child: Expression, forcedNullable: Boolean) extends UnaryExpression {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = forcedNullable // override nullable
  override def nullSafeEval(input: Any): Any = input
  override def prettyName: String = "set_nullable"
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input => s"(${CodeGenerator.boxedType(dataType)})$expr.nullSafeEval($input)")
  }
}

object NullableHelper {
  def makeNotNullable(data: Column): Column = {
    new Column(SetNullable(data.expr, false))
  }
  def makeNullable(data: Column): Column = {
    new Column(SetNullable(data.expr, true))
  }
}