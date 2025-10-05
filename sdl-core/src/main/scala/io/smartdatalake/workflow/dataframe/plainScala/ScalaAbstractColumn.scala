/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataType}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Abstraction of columns that include data (ScalaColumn), and expressions that calculate data and can be evaluated to a column.
 */
abstract class ScalaAbstractColumn extends GenericColumn {
  def dataType: ScalaDataType[_]

  def data: Seq[_]

  def inputColumns: Set[String] = Set()

  def setInputData(inputData: Map[String, Seq[_]], size: Int): Unit = Unit

  def visit[X](func: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = func(this)

  override def subFeedType: universe.Type = typeOf[ScalaSubFeed]

  def toScalaColumn(df: ScalaDataFrame): ScalaColumn[_] = {
    // get input columns and data
    val inputColumns = visit[Set[String]](_.inputColumns, _ ++ _)
    val inputData = df.cols.filter(c => inputColumns.contains(c.getName.get))
      .map(c => (c.getName.get, c.data)).toMap
    assert(inputData.keySet == inputColumns, s"Missing input data for column(s): ${inputColumns.diff(inputData.keySet).mkString(", ")}")
    // set input data
    visit[Unit](expr => expr.setInputData(inputData, df.nrRows), (_, _) => Unit)
    // create column
    dataType
      .createColumnDefinition(getName.getOrElse(ScalaColumn.nextColName))
      .createColumn(data.toIndexedSeq)
  }

  override def ===(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "not equal", _ => (_ == _), Some(ScalaBooleanDataType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def =!=(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "not equal", _ => (_ != _), Some(ScalaBooleanDataType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def >(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "gt", _.numeric.gt, Some(ScalaBooleanDataType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def <(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "lt", _.numeric.lt, Some(ScalaBooleanDataType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def -(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "minus", _.numeric.minus)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def +(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "plus", _.numeric.plus)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def /(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "div", _.numericDiv)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def *(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "multiply", _.numeric.times)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def and(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "and", _ => (_.asInstanceOf[Boolean] && _.asInstanceOf[Boolean]), Some(ScalaBooleanDataType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def or(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "or", _ => (_.asInstanceOf[Boolean] || _.asInstanceOf[Boolean]), Some(ScalaBooleanDataType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def isin(list: Any*): GenericColumn = ???

  override def isNull: ScalaAbstractColumn = {
    ScalaUnaryExpr(this, "isNull", x => x == null, Some(ScalaBooleanDataType))
  }

  override def isNotNull: ScalaAbstractColumn = {
    ScalaUnaryExpr(this, "isNotNull", x => x != null, Some(ScalaBooleanDataType))
  }

  override def as(name: String): ScalaAbstractColumn = {
    ScalaNamedExpr(this, name)
  }

  override def cast(toDataType: GenericDataType): ScalaAbstractColumn = {
    if (toDataType.isSameType(this.dataType)) this
    else toDataType match {
      case scalaDataType: ScalaDataType[_] => ScalaUnaryExpr(this, "cast", scalaDataType.getCastFunction(this.dataType), Some(scalaDataType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(toDataType)
    }
  }

  override def exprSql: String = throw new NotImplementedError("The 'exprSql' method is not applicable")

  override def desc: GenericColumn = throw new NotImplementedError("Method 'desc' is not yet supported, as sorting of DataFrames is not yet implemented")

  override def getName: Option[String] = None // default is None. Name is given by ScalaNamedExpr or ScalaColumnReference.
}

/**
 * Operation between two columns/expressions
 *
 * @param left          left expression
 * @param right         right expression
 * @param opName        operation name for information purposes
 * @param funcCreator   function creator which creates the function for the operation based on the combined datatype (if needed)
 *                      Note: this is a creator function, which is called lazy, because the datatype is not known at construction time
 * @param fixedDataType optional fixed datatype, if the result datatype is known in advance (e.g. for boolean operations)
 */
case class ScalaBinaryExpr(left: ScalaAbstractColumn, right: ScalaAbstractColumn, opName: String, funcCreator: ScalaDataType[Any] => ((Any, Any) => Any), fixedDataType: Option[ScalaDataType[_]] = None) extends ScalaAbstractColumn {
  lazy val dataType: ScalaDataType[_] = fixedDataType.getOrElse(
    left.dataType.getGreaterType(right.dataType)
  )
  lazy val func: (Any, Any) => Any = funcCreator(dataType.asInstanceOf[ScalaDataType[Any]])
  override lazy val data: Seq[_] = {
    assert(left.data.size == right.data.size, s"Size of left data (${left.data.size}) must be equal to size of right data (${right.data.size})")
    val castLeft = if (dataType != left.dataType && fixedDataType.isEmpty) dataType.getCastFunction(left.dataType) else (x: Any) => x
    val castRight = if (dataType != right.dataType && fixedDataType.isEmpty) dataType.getCastFunction(right.dataType) else (x: Any) => x
    (left.data zip right.data).map(pair => func(castLeft(pair._1), castRight(pair._2)))
  }

  override def visit[X](visitorFunc: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = {
    Seq(
      visitorFunc(this),
      left.visit(visitorFunc, aggregator),
      right.visit(visitorFunc, aggregator)
    ).reduce(aggregator)
  }

  override def apply(extraction: Any): ScalaBinaryExpr = throw new NotImplementedError("The 'apply(extraction: Any)' method is not applicable for a ScalaBinaryExpr instance")
}

/**
 * Operation on one column/expression
 *
 * @param in            input expression
 * @param name          operation name for information purposes
 * @param func          function for the operation
 * @param fixedDataType optional fixed datatype, if the result datatype is different thant the dataType of the input column, and it is known in advance
 */
case class ScalaUnaryExpr(in: ScalaAbstractColumn, name: String, func: Any => Any, fixedDataType: Option[ScalaDataType[_]] = None) extends ScalaAbstractColumn {
  override def dataType: ScalaDataType[_] = fixedDataType.getOrElse(in.dataType)

  override def data: Seq[_] = in.data.map(func)

  override def getName: Option[String] = Some(name)

  override def visit[X](visitorFunc: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = {
    aggregator(visitorFunc(this), in.visit(visitorFunc, aggregator))
  }

  override def apply(extraction: Any): ScalaUnaryExpr = throw new NotImplementedError("The 'apply(extraction: Any)' method is not applicable for a ScalaUnaryExpr instance")
}

/**
 * Literal value expression
 * Note: for literal values the datatype is given by the class of the value
 *
 * @param value literal value
 * @tparam A type of the literal value
 */
case class ScalaLiteral[A: ClassTag](value: A) extends ScalaAbstractColumn {
  override val dataType: ScalaDataType[A] = ScalaDataType.getFor[A]
  private var colSize: Option[Int] = None

  override def setInputData(inputData: Map[String, Seq[_]], size: Int): Unit = {
    colSize = Some(size)
  }

  override def data: Seq[_] = {
    Seq.fill(colSize.getOrElse(throw new IllegalStateException("Literal is not initialized")))(value).view
  }

  override def apply(extraction: Any): ScalaUnaryExpr = throw new NotImplementedError("The 'apply(extraction: Any)' method is not applicable for a ScalaUnaryExpr instance")
}

/**
 * Column reference expression
 * Note: the datatype is only known after the input data has been set
 *
 * @param name name of the referenced column
 */
case class ScalaColumnReference(name: String) extends ScalaAbstractColumn {
  override def inputColumns: Set[String] = Set(name)

  private var resolvedData: Option[Seq[_]] = None

  def isResolved: Boolean = resolvedData.isDefined

  override def setInputData(inputData: Map[String, Seq[_]], size: Int): Unit = {
    resolvedData = inputData.get(name)
      .orElse(throw new IllegalStateException(s"Column with name '$name' not found in input data"))
  }

  override def dataType: ScalaDataType[_] = {
    assert(isResolved, s"Column reference with name '$name' is empty!")
    assert(resolvedData.get.nonEmpty, s"Data for column reference with name '$name' is empty!")
    ScalaDataType.getFor(resolvedData.get.head.getClass)
  }

  override def data: Seq[_] = {
    resolvedData.getOrElse(throw new IllegalStateException(s"Unresolved Column reference with name '$name'")).view
  }

  override def getName: Option[String] = Some(name)

  override def apply(extraction: Any): ScalaUnaryExpr = throw new NotImplementedError("The 'apply(extraction: Any)' method is not applicable for a ScalaUnaryExpr instance")
}

/**
 * Named expression, which gives a name to an expression
 * Note: the datatype is the same as the input expression
 *
 * @param in   input expression
 * @param name name to be given to the expression
 */
case class ScalaNamedExpr(in: ScalaAbstractColumn, name: String) extends ScalaAbstractColumn {
  override def dataType: ScalaDataType[_] = in.dataType

  override def data: Seq[_] = in.data

  override def getName: Option[String] = Some(name)

  override def visit[X](visitorFunc: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = {
    aggregator(visitorFunc(this), in.visit(visitorFunc, aggregator))
  }

  override def apply(extraction: Any): ScalaUnaryExpr = throw new NotImplementedError("The 'apply(extraction: Any)' method is not applicable for a ScalaUnaryExpr instance")
}