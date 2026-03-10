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
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataType, GenericWhen}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Abstraction of columns that include data (ScalaColumn and ScalaExplodingColumn), and expressions that calculate data and can be evaluated to a column.
 */
abstract class ScalaAbstractColumn extends GenericColumn {

  def dataType: ScalaDataType[_]

  def data: Seq[_]

  def inputColumns: Set[String] = Set()

  def setInputData(inputData: Map[String, ScalaColumn[_]], size: Int): Unit = ()

  def visit[X](func: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = func(this)

  override def subFeedType: universe.Type = typeOf[ScalaSubFeed]

  def toScalaColumn(df: ScalaDataFrame): ScalaColumn[_] = {
    // get input columns and data
    val inputColumns = visit[Set[String]](_.inputColumns, _ ++ _)
    val inputData = inputColumns.map(_.split('.')).map {
        case Array(alias, name) =>
          val dfCol = df.cols.find( c => (c.getName.contains(name) || name == "*") && c.definition.dataFrameAlias.contains(alias))
            .getOrElse(throw ColumnNotFoundException(s"$alias.$name", df.cols.map(_.definition.getFullName())))
          (s"$alias.$name", dfCol)
        case Array(name) =>
          val dfCol = df.cols.find(c => c.getName.contains(name) || name == "*")
            .getOrElse(throw ColumnNotFoundException(name, df.cols.map(_.definition.name)))
          (name, dfCol)
      }.toMap
    toScalaColumn(inputData, df.nrRows)
  }

  def toScalaColumn(inputData: Map[String,ScalaColumn[_]], nbOfRows: Int): ScalaColumn[_] = {
    // set input data
    visit[Unit](expr => expr.setInputData(inputData, nbOfRows), (_, _) => ())
    // create column
    toScalaColumn(data.toIndexedSeq)
  }

  def toScalaColumn(data: IndexedSeq[_]): ScalaColumn[_] = {
    val columnDefinition = getName.getOrElse(ScalaColumn.nextColName).split('.') match {
      case Array(alias, name) => dataType.createColumnDefinition(name).withDataFrameAlias(Some(alias))
      case Array(name) => dataType.createColumnDefinition(name)
    }
    columnDefinition.createColumn(data)
  }

  override def ===(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "equal", _ => (_ == _), Some(ScalaBooleanDataType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def =!=(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "not equal", _ => (_ != _), Some(ScalaBooleanDataType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def <=>(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "equal null", _ => (a,b) => (a == null && b == null) || (a == b), Some(ScalaBooleanDataType))
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

  override def >=(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "gteq", _.numeric.gteq, Some(ScalaBooleanDataType))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def <=(other: GenericColumn): ScalaAbstractColumn = {
    other match {
      case sc: ScalaAbstractColumn => ScalaBinaryExpr(this, sc, "lteq", _.numeric.lteq, Some(ScalaBooleanDataType))
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

  override def isin(list: Any*): ScalaAbstractColumn = {
    ScalaUnaryExpr(this, "isin", x => list.contains(x), Some(ScalaBooleanDataType))
  }

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

  override def apply(extraction: Any): ScalaAbstractColumn = throw new NotImplementedError("The 'apply' method is not applicable")

  /**
   * Name for this column/expression, which is used for the output column name in the resulting DataFrame.
   * It is optional, as Name is given only by ScalaNamedExpr or ScalaColumnReference.
   */
  override def getName: Option[String] = None
}

/**
 * Operation between many columns/expressions
 *
 * @param cols          expressions
 * @param opName        operation name for information purposes
 * @param funcCreator   function creator which creates the function for the operation based on the combined datatype (if needed)
 *                      Note: this is a creator function, which is called lazy, because the datatype is not known at construction time
 * @param fixedDataType optional fixed datatype, if the result datatype is known in advance (e.g. for boolean operations)
 */
case class ScalaManyExpr(cols: Seq[ScalaAbstractColumn], opName: String, funcCreator: ScalaDataType[Any] => (Seq[Any] => Any), fixedDataType: Option[ScalaDataType[_]] = None) extends ScalaAbstractColumn {
  lazy val dataType: ScalaDataType[_] = fixedDataType.getOrElse(
    cols.map(_.dataType).reduce((a, b) => a.getGreaterType(b))
  )
  lazy val func: Seq[Any] => Any = funcCreator(dataType.asInstanceOf[ScalaDataType[Any]])
  override def data: Seq[_] = {
    assert(cols.map(_.data.size).distinct.size == 1, s"Size of all columns must be equal, but got sizes: ${cols.map(c => s"'${c.getName.getOrElse("col")}': ${c.data.size}").mkString(", ")}")
    val colsDataCasted = cols.map { c =>
      val castFun = if (dataType != c.dataType && fixedDataType.isEmpty) dataType.getCastFunction(c.dataType) else (x: Any) => x
      c.data.map(castFun)
    }
    val opFun = funcCreator(dataType.asInstanceOf[ScalaDataType[Any]])
    colsDataCasted.transpose.map(opFun)
  }

  override def visit[X](visitorFunc: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = {
    (cols.map(_.visit(visitorFunc, aggregator)) :+ visitorFunc(this))
      .reduce(aggregator)
  }
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
  override def data: Seq[_] = {
    assert(left.data.size == right.data.size, s"Size of left data (${left.data.size}) must be equal to size of right data (${right.data.size})")
    val castLeft = if (dataType != left.dataType && fixedDataType.isEmpty) dataType.getCastFunction(left.dataType) else (x: Any) => x
    val castRight = if (dataType != right.dataType && fixedDataType.isEmpty) dataType.getCastFunction(right.dataType) else (x: Any) => x
    (left.data zip right.data).map(pair => func(castLeft(pair._1), castRight(pair._2)))
  }

  override def visit[X](visitorFunc: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = {
    Seq(
      left.visit(visitorFunc, aggregator),
      right.visit(visitorFunc, aggregator),
      visitorFunc(this)
    ).reduce(aggregator)
  }
}

/**
 * Operation on one column/expression
 *
 * @param in            input expression
 * @param opName        operation name for information purposes
 * @param func          function for the operation
 * @param fixedDataType optional fixed datatype, if the result datatype is different thant the dataType of the input column, and it is known in advance
 */
case class ScalaUnaryExpr(in: ScalaAbstractColumn, opName: String, func: Any => Any, fixedDataType: Option[ScalaDataType[_]] = None) extends ScalaAbstractColumn {
  override def dataType: ScalaDataType[_] = fixedDataType.getOrElse(in.dataType)

  override def data: Seq[_] = in.data.map(func)

  override def visit[X](visitorFunc: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = {
    aggregator(in.visit(visitorFunc, aggregator), visitorFunc(this))
  }
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

  override def setInputData(inputData: Map[String, ScalaColumn[_]], size: Int): Unit = {
    colSize = Some(size)
  }

  override def data: Seq[_] = {
    Seq.fill(colSize.getOrElse(throw new IllegalStateException("Literal is not initialized")))(value)
  }
}

/**
 * Column reference expression
 * Note: the datatype is only known after the input data has been set
 *
 * @param name name of the referenced column
 */
case class ScalaColumnReference(name: String) extends ScalaAbstractColumn {
  override def inputColumns: Set[String] = Set(name)

  private var resolvedColumn: Option[ScalaColumn[_]] = None

  def isResolved: Boolean = resolvedColumn.isDefined

  override def setInputData(inputData: Map[String, ScalaColumn[_]], size: Int): Unit = {
    resolvedColumn = if (name == "*") Some(inputData.head._2)
    else inputData.get(name).orElse(throw new IllegalStateException(s"Column with name '$name' not found in input data"))
  }

  override def dataType: ScalaDataType[_] = {
    assert(isResolved, s"Column reference with name '$name' is not resolved!")
    resolvedColumn.get.dataType
  }

  override def data: Seq[_] = {
    assert(isResolved, s"Column reference with name '$name' is not resolved!")
    resolvedColumn.get.data
  }

  override def getName: Option[String] = Some(name)
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
    aggregator(in.visit(visitorFunc, aggregator), visitorFunc(this))
  }
}

/**
 * Aggregate expression, which applies an aggregation function to an input expression and gives a name to the result
 * Note: the datatype is the same as the input expression
 * Note: the output data has always only one value, which is the result of the aggregation function applied to all values of the input expression
 *
 * @param in   input expression
 * @param opName operation name to be given to the expression
 * @param aggFunc aggregation function
 * @param outputDataType  output datatype
 */
case class ScalaAggregateExpr(in: ScalaAbstractColumn, opName: String, aggFunc: Seq[Any] => Any, outputDataType: ScalaDataType[_]) extends ScalaAbstractColumn {
  override def dataType: ScalaDataType[_] = outputDataType

  override def data: Seq[_] = {
    val result = aggFunc(in.data)
    Seq(result)
  }

  override def visit[X](visitorFunc: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = {
    aggregator(in.visit(visitorFunc, aggregator), visitorFunc(this))
  }
}


/**
 * Operation on one column/expression
 *
 * @param in            input expression
 * @param func          function for the operation
 * @param fixedDataType optional fixed datatype, if the result datatype is different thant the dataType of the input column, and it is known in advance
 */
case class ScalaWhenExpr(condition: ScalaAbstractColumn, in: ScalaAbstractColumn, prev: Option[ScalaAbstractColumn] = None) extends ScalaAbstractColumn with GenericWhen {
  override def dataType: ScalaDataType[_] = prev.getOrElse(in).dataType

  override def data: Seq[_] = {
    val inData = in.data
    val conditionData = condition.data.map(c => if (c != null && c.isInstanceOf[Boolean]) c.asInstanceOf[Boolean] else false)
    val outData = if (prev.isDefined) {
      val prevData = prev.get.data
      conditionData.zip(inData.zip(prevData)).map {
        case (predicate, (in, prev)) => Option(prev).getOrElse(if (predicate) in else null)
      }
    } else {
      conditionData.zip(inData).map {
        case (predicate, in) => if (predicate) in else null
      }
    }
    outData
  }

  override def setInputData(inputData: Map[String, ScalaColumn[_]], size: Int): Unit = {
    super.setInputData(inputData, size)
    assert(condition.dataType == ScalaBooleanDataType, s"The data type of the condition in a when expression must be boolean, but got ${condition.dataType.getClass.getSimpleName}")
    assert(prev.forall(_.dataType == in.dataType || in.dataType == ScalaNullDataType), s"The data type of the value in a when expression (${in.dataType.getClass.getSimpleName} must be the same as the data type of the value in the previous when expression (${prev.get.dataType.getClass.getSimpleName})")
  }

  override def visit[X](visitorFunc: ScalaAbstractColumn => X, aggregator: (X, X) => X): X = {
    Seq(
      Some(condition.visit(visitorFunc, aggregator)),
      Some(in.visit(visitorFunc, aggregator)),
      prev.map(_.visit(visitorFunc, aggregator)),
      Some(visitorFunc(this)),
    ).flatten.reduce(aggregator)
  }

  override def when(condition: GenericColumn, value: GenericColumn): ScalaAbstractColumn with GenericWhen = {
    (condition, value) match {
      case (scalaCondition: ScalaAbstractColumn, sparkValue: ScalaAbstractColumn) => ScalaWhenExpr(scalaCondition, sparkValue, Some(this))
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${condition.subFeedType.typeSymbol.name}, ${value.subFeedType.typeSymbol.name} in method when")
    }
  }

  override def otherwise(value: GenericColumn): ScalaAbstractColumn = {
    value  match {
      case scalaValue: ScalaAbstractColumn =>
        new ScalaBinaryExpr(this, scalaValue, "otherwise", _ => (a,b) => Option(a).getOrElse(b), Some(dataType)) {
          override def setInputData(inputData: Map[String, ScalaColumn[_]], size: Int): Unit = {
            super.setInputData(inputData, size)
            assert(scalaValue.dataType == dataType, s"The data type of the value in an otherwise expression (${scalaValue.dataType} must be the same as the data type of the value in the previous when expression (${dataType})")
          }
        }
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${value.subFeedType.typeSymbol.name} in method otherwise")
    }
  }
}

case class ColumnNotFoundException(columnName: String, dfCols: Seq[String]) extends Exception(s"Column with name '$columnName' not found in DataFrame with column(s): ${dfCols.mkString(", ")}")