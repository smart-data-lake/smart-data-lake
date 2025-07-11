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

import ScalaDataTypeEnum._
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataType}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf
import scala.util.{Failure, Success, Try}

case class ScalaColumn[A](metadata: ScalaColumnDefinition, inner: Seq[A]) extends GenericColumn with SmartDataLakeLogger{

  assert(inner.forall(field => ScalaDataType.fromValue(field) == metadata.dataType), s"One of the fields in the Column does not comply with the metadata data type ${metadata.dataType.inner}")

  def getOrdering = {
    if (this.isEmpty) throw new NoSuchElementException("The Column is empty, therefore no Ordering[A] object found")
    else metadata.dataType.inner match {
      case STRING => Ordering[String]
      case BOOLEAN => Ordering[Boolean]
      case NUMBER if this.inner.head.isInstanceOf[Int] => Ordering[Int]
      case NUMBER if this.inner.head.isInstanceOf[Double] => Ordering[Double]
      case _ => throw new IllegalStateException("Unknown datatype A in column. Cannot get Ordering[A]")
    }
  }


  //Workaround for casting cols in other methods.
  def getAType: String = {
    if (this.isEmpty) throw new NoSuchElementException("The Column is empty, therefore no Ordering[A] object found")
    else metadata.dataType.inner match {
      case STRING => "String"
      case BOOLEAN => "Boolean"
      case NUMBER if this.inner.head.isInstanceOf[Int] => "Int"
      case NUMBER if this.inner.head.isInstanceOf[Double] => "Double"
      case NOTHING => "Nothing"
      case _ => throw new IllegalStateException("Unknown datatype A in column")
    }
  }

  def explicitCast = getAType match {
    case "String" => this.asInstanceOf[ScalaColumn[String]]
    case "Boolean" => this.asInstanceOf[ScalaColumn[Boolean]]
    case "Int" => this.asInstanceOf[ScalaColumn[Int]]
    case "Double" => this.asInstanceOf[ScalaColumn[Double]]
    case _ => throw new IllegalStateException("Unknown datatype A in column. Cannot get Ordering[A]")
  }

  def doubleCasting: ScalaColumn[Double] = {
    if (this.isEmpty) ScalaColumn[Double](this.name, NUMBER, Seq())
    else metadata.dataType.inner match {
      case NUMBER if this.inner.head.isInstanceOf[Int] => this.copy(metadata.copy(dataType = ScalaDataType(NUMBER)), inner = inner.asInstanceOf[Seq[Int]].map(_.toDouble))
      case NUMBER if this.inner.head.isInstanceOf[Double] => this.asInstanceOf[ScalaColumn[Double]]
      case x => throw new IllegalStateException(f"Column is not of the NUMBER type, but $x and its values cannot be casted to Double")
    }
  }

  def combine[B, C](other: ScalaColumn[B], func: (A, B) => C, funcName: String, newType: ScalaDataTypeEnum): ScalaColumn[C] = {
    val newSeq: Seq[C] = (this.inner zip other.inner).map(pair => func(pair._1, pair._2))
    ScalaColumn[C](s"${this.metadata.name}_${funcName}_${other.metadata.name}", newType, newSeq)
  }

  def eq[B](other: ScalaColumn[B]): ScalaColumn[Boolean] = combine(other, (a: A, b: B) => a == b, "===", BOOLEAN)

  def neq[B](other: ScalaColumn[B]): ScalaColumn[Boolean] = combine(other, (a: A, b: B) => a != b, "=!=", BOOLEAN)

  def gt(other: ScalaColumn[A])(implicit evidenceA: Ordering[A]): ScalaColumn[Boolean] = combine(other, (a1: A, a2: A) => evidenceA.gt(a1, a2), ">", BOOLEAN)

  def lt(other: ScalaColumn[A])(implicit evidenceA: Ordering[A]): ScalaColumn[Boolean] = combine(other, (a1: A, a2: A) => evidenceA.lt(a1, a2), "<", BOOLEAN)

  def plus[B](other: ScalaColumn[B])(implicit evidenceA: Numeric[A], evidenceB: Numeric[B]): ScalaColumn[Double] = combine(other, (a: A, b: B) => evidenceA.toDouble(a) + evidenceB.toDouble(b), "+", NUMBER)

  def minus[B](other: ScalaColumn[B])(implicit evidenceA: Numeric[A], evidenceB: Numeric[B]): ScalaColumn[Double] = combine(other, (a: A, b: B) => evidenceA.toDouble(a) - evidenceB.toDouble(b), "-", NUMBER)

  def over[B](other: ScalaColumn[B])(implicit evidenceA: Numeric[A], evidenceB: Numeric[B]): ScalaColumn[Double] = combine(other, (a: A, b: B) => evidenceA.toDouble(a) / evidenceB.toDouble(b), "/", NUMBER)

  def times[B](other: ScalaColumn[B])(implicit evidenceA: Numeric[A], evidenceB: Numeric[B]): ScalaColumn[Double] = combine(other, (a: A, b: B) => evidenceA.toDouble(a) * evidenceB.toDouble(b), "-", NUMBER)

  def &&(other: ScalaColumn[A])(implicit evidenceA: A =:= Boolean): ScalaColumn[Boolean] = combine(other, (a: A, b: A) => a && b, "AND", BOOLEAN)

  def ||(other: ScalaColumn[A])(implicit evidenceA: A =:= Boolean): ScalaColumn[Boolean] = combine(other, (a: A, b: A) => a || b, "OR", BOOLEAN)

  def castScalaCol(dataType: ScalaDataType) = {
    if (dataType.inner == this.metadata.dataType.inner) this
    else {
      val stringSeq = inner.map(_.toString())
      val newSeq = dataType.inner match {
        case NUMBER => stringSeq.map(_.toDouble)
        case BOOLEAN => stringSeq.map(_.toBoolean)
        case _ => stringSeq
      }
      this.copy(metadata.copy(dataType = dataType), inner = newSeq)
    }
  }

  def descScala(implicit evidenceA: Ordering[A]) = this.copy(inner = inner.sortBy(id => id).reverse)

  def size: Int = inner.size

  def append(other: ScalaColumn[A]) = this.copy(inner = inner ++ other.inner)

  def name: String = metadata.name

  def isEmpty: Boolean = inner.isEmpty

  def checkSubFeedAndExec[A, B](other: GenericColumn, func: ScalaColumn[A] => ScalaColumn[B]): ScalaColumn[B] = other match {
    case scalaCol: ScalaColumn[_] => func(scalaCol.asInstanceOf[ScalaColumn[A]])
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }


  //trait implementation

  override def ===(other: GenericColumn): GenericColumn = checkSubFeedAndExec(other, eq)

  override def =!=(other: GenericColumn): GenericColumn = checkSubFeedAndExec(other, neq)

  override def >(other: GenericColumn): GenericColumn = {
    if (this.isEmpty) return ScalaColumn.emptyBoolColumn
    other match {
      case sc: ScalaColumn[_] => { //type erasure in JVM --> cannot match against type [A]
        Try(gt(sc.asInstanceOf[ScalaColumn[A]])(getOrdering.asInstanceOf[Ordering[A]])) match {
          case Success(v) => v
          case Failure(exception) =>
            logger.error("Problable cause: The '>' operation can only be called with a column of the same type!")
            throw exception
        }
      }
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def <(other: GenericColumn): GenericColumn = {
    if (this.isEmpty) return ScalaColumn.emptyBoolColumn
    other match {
      case sc: ScalaColumn[_] => { //type erasure in JVM --> cannot match against type [A]
        Try(lt(sc.asInstanceOf[ScalaColumn[A]])(getOrdering.asInstanceOf[Ordering[A]])) match {
          case Success(v) => v
          case Failure(exception) =>
            logger.error("Problable cause: The '<' operation can only be called with a column of the same type!")
            throw exception
        }
      }
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def +(other: GenericColumn): GenericColumn = {
    if (this.isEmpty) return ScalaColumn.emptyNumColumn
    other match {
      case sc: ScalaColumn[_] => this.doubleCasting.plus(sc.doubleCasting)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def -(other: GenericColumn): GenericColumn = {
    if (this.isEmpty) return ScalaColumn.emptyNumColumn
    other match {
      case sc: ScalaColumn[_] => this.doubleCasting.minus(sc.doubleCasting)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def /(other: GenericColumn): GenericColumn = {
    if (this.isEmpty) return ScalaColumn.emptyNumColumn
    other match {
      case sc: ScalaColumn[_] => this.doubleCasting.over(sc.doubleCasting)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def *(other: GenericColumn): GenericColumn = {
    if (this.isEmpty) return ScalaColumn.emptyNumColumn
    other match {
      case sc: ScalaColumn[_] => this.doubleCasting.times(sc.doubleCasting)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def and(other: GenericColumn): GenericColumn = {
    if (this.isEmpty) return ScalaColumn.emptyBoolColumn
    other match {
      case sc: ScalaColumn[_] => {
        Try(this.asInstanceOf[ScalaColumn[Boolean]] && other.asInstanceOf[ScalaColumn[Boolean]]) match {
          case Success(value) => value
          case Failure(exception) =>
            logger.error("Problable cause: The 'and' operation can only be called with two boolean columns")
            throw exception
        }
      }
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def or(other: GenericColumn): GenericColumn = {
    if (this.isEmpty) return ScalaColumn.emptyBoolColumn
    other match {
      case sc: ScalaColumn[_] => {
        Try(this.asInstanceOf[ScalaColumn[Boolean]] || other.asInstanceOf[ScalaColumn[Boolean]]) match {
          case Success(value) => value
          case Failure(exception) =>
            logger.error("Problable cause: The 'or' operation can only be called with two boolean columns")
            throw exception
        }
      }
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def isNotNull: GenericColumn = ScalaColumn(f"${this.metadata.name}_IS_NOT_NULL", BOOLEAN, inner.map(_ != null))

  override def isNull: GenericColumn = ScalaColumn(f"${this.metadata.name}_IS_NULL", BOOLEAN, inner.map(_ == null))

  override def as(name: String): ScalaColumn[A] = this.copy(metadata = metadata.copy(name = name))

  override def apply(extraction: Any): ScalaColumn[A] = throw new NotImplementedError("The 'apply' method is not applicable for a ScalaColumn instance")

  override def cast(dataType: GenericDataType): GenericColumn = {
    dataType match {
      case sc: ScalaDataType => this.castScalaCol(sc)
      case _ => ???
    }
  }

  override def getName: Option[String] = Some(metadata.name)

  override def desc: GenericColumn = descScala(getOrdering.asInstanceOf[Ordering[A]])

  override def exprSql: String = throw new NotImplementedError("The 'exprSql' method is not applicable for ScalaColumns")

  override def subFeedType: universe.Type = ???//typeOf[ScalaSubFeed]

  override def isin(list: Any*): GenericColumn = ScalaColumn(f"${this.metadata.name}_IS_IN_LIST", BOOLEAN, inner.map(list.contains(_)))
}


object ScalaColumn {

  def apply[A](name: String, dataType: ScalaDataTypeEnum, inner: Seq[A])  = new ScalaColumn[A](ScalaColumnDefinition(name = name, dataType = ScalaDataType(dataType)), inner = inner)

  def emptyColumn: ScalaColumn[String] = ScalaColumn[String](name = "emptyCol", dataType = STRING, inner = Seq())

  def emptyNumColumn: ScalaColumn[Int] = ScalaColumn[Int](name = "emptyCol", dataType = NUMBER, inner = Seq())

  def emptyBoolColumn: ScalaColumn[Boolean] = ScalaColumn[Boolean](name = "emptyCol", dataType = BOOLEAN, inner = Seq())
}

