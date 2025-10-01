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

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataType}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

case class ScalaColumn[A: ClassTag](definition: ScalaColumnDefinition[A], data: IndexedSeq[A]) extends GenericColumn with SmartDataLakeLogger {

  val ordering: Ordering[A] = definition.dataType.ordering
  lazy val numeric: Numeric[A] = definition.dataType.numeric // this is lazy as it is not implemented for all data types

  def combine[B, C: ClassTag](other: ScalaColumn[B], func: (A, B) => C, funcName: String): ScalaColumn[C] = {
    assert(data.size == other.data.size, "data size of columns is not equal!")
    val newSeq: Seq[C] = (data zip other.data).map(pair => func(pair._1, pair._2))
    ScalaColumn[C](s"${definition.name}_${funcName}_${other.definition.name}", newSeq)
  }

  def eq(other: ScalaColumn[A]): ScalaColumn[Boolean] = combine(other, (a: A, b: A) => a == b, "===")

  def neq(other: ScalaColumn[A]): ScalaColumn[Boolean] = combine(other, (a: A, b: A) => a != b, "=!=")

  def gt(other: ScalaColumn[A]): ScalaColumn[Boolean] = combine(other, (a1: A, a2: A) => ordering.gt(a1, a2), ">")

  def lt(other: ScalaColumn[A]): ScalaColumn[Boolean] = combine(other, (a1: A, a2: A) => ordering.lt(a1, a2), "<")

  def plus(other: ScalaColumn[A]): ScalaColumn[A] = combine(other, (a: A, b: A) => numeric.plus(a, b), "+")

  def minus(other: ScalaColumn[A]): ScalaColumn[A] = combine(other, (a: A, b: A) => numeric.minus(a, b), "-")

  def div(other: ScalaColumn[A]): ScalaColumn[A] = combine(other, (a: A, b: A) => definition.dataType.numericDiv(a, b), "/")

  def times(other: ScalaColumn[A]): ScalaColumn[A] = combine(other, (a: A, b: A) => numeric.times(a, b), "*")

  def &&(other: ScalaColumn[A])(implicit evidenceA: A =:= Boolean): ScalaColumn[Boolean] = combine(other, (a: A, b: A) => a && b, "AND")

  def ||(other: ScalaColumn[A])(implicit evidenceA: A =:= Boolean): ScalaColumn[Boolean] = combine(other, (a: A, b: A) => a || b, "OR")

  def castTo[B: ClassTag](toDataType: ScalaDataType[B]): ScalaColumn[B] = {
    if (implicitly[ClassTag[A]] == implicitly[ClassTag[B]]) this.asInstanceOf[ScalaColumn[B]]
    else toDataType.castColumn(this)
  }

  def unsafeCastTo(toDataType: ScalaDataType[_]): ScalaColumn[_] = {
    if (definition.dataType.scalaClass == toDataType.scalaClass) this.asInstanceOf[ScalaColumn[_]]
    else toDataType.castColumn(this)
  }

  def sortDesc: ScalaColumn[A] = copy(data = data.sorted(ordering).reverse)

  def size: Int = data.size

  def append(other: ScalaColumn[A]): ScalaColumn[A] = copy(data = data ++ other.data)

  def unsafeAppend(other: ScalaColumn[_]): ScalaColumn[A] = append(other.asInstanceOf[ScalaColumn[A]])

  def limit(n: Int): ScalaColumn[A] = copy(data = data.take(n))

  def isEmpty: Boolean = data.isEmpty

  //trait implementation

  override def ===(other: GenericColumn): GenericColumn = {
    other match {
      case sc: ScalaColumn[_] =>
        assert(definition.dataType == sc.definition.dataType, "types must be the same for '===' operation")
        eq(sc.asInstanceOf[ScalaColumn[A]])
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def =!=(other: GenericColumn): GenericColumn = {
    other match {
      case sc: ScalaColumn[_] =>
        assert(definition.dataType == sc.definition.dataType, "types must be the same for '=!=' operation")
        neq(sc.asInstanceOf[ScalaColumn[A]])
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def >(other: GenericColumn): GenericColumn = {
    other match {
      case sc: ScalaColumn[_] =>
        val types: Seq[ScalaDataType[_]] = Seq(definition.dataType, sc.definition.dataType)
        val resultType: ScalaDataType[_] = types.reduce(_ getGreaterType _)
        resultType.castColumn(this).gt(resultType.castColumn(sc))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def <(other: GenericColumn): GenericColumn = {
    other match {
      case sc: ScalaColumn[_] =>
        val types: Seq[ScalaDataType[_]] = Seq(definition.dataType, sc.definition.dataType)
        val resultType: ScalaDataType[_] = types.reduce(_ getGreaterType _)
        resultType.castColumn(this).lt(resultType.castColumn(sc))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def +(other: GenericColumn): GenericColumn = {
    other match {
      case sc: ScalaColumn[_] =>
        val types: Seq[ScalaDataType[_]] = Seq(definition.dataType, sc.definition.dataType)
        assert(types.forall(_.isNumeric), "types must be numeric for '+' operation")
        val resultType: ScalaDataType[_] = types.reduce(_ getGreaterType _)
        resultType.castColumn(this).plus(resultType.castColumn(sc))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def -(other: GenericColumn): GenericColumn = {
    other match {
      case sc: ScalaColumn[_] =>
        val types: Seq[ScalaDataType[_]] = Seq(definition.dataType, sc.definition.dataType)
        assert(types.forall(_.isNumeric), "types must be numeric for '-' operation")
        val resultType: ScalaDataType[_] = types.reduce(_ getGreaterType _)
        resultType.castColumn(this).minus(resultType.castColumn(sc))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def /(other: GenericColumn): GenericColumn = {
    other match {
      case sc: ScalaColumn[_] =>
        val types: Seq[ScalaDataType[_]] = Seq(definition.dataType, sc.definition.dataType)
        assert(types.forall(_.isNumeric), "types must be numeric for '/' operation")
        val resultType: ScalaDataType[_] = types.reduce(_ getGreaterType _)
        resultType.castColumn(this).div(resultType.castColumn(sc))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def *(other: GenericColumn): GenericColumn = {
    other match {
      case sc: ScalaColumn[_] =>
        val types: Seq[ScalaDataType[_]] = Seq(definition.dataType, sc.definition.dataType)
        assert(types.forall(_.isNumeric), "types must be numeric for '*' operation")
        val resultType: ScalaDataType[_] = types.reduce(_ getGreaterType _)
        resultType.castColumn(this).times(resultType.castColumn(sc))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def and(other: GenericColumn): GenericColumn = {
    other match {
      case sc: ScalaColumn[_] =>
        val types: Seq[ScalaDataType[_]] = Seq(definition.dataType, sc.definition.dataType)
        assert(types.forall(_ == ScalaBooleanDataType), "types must be boolean for '&&' operation")
        this.asInstanceOf[ScalaColumn[Boolean]] && sc.asInstanceOf[ScalaColumn[Boolean]]
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def or(other: GenericColumn): GenericColumn = {
    other match {
      case sc: ScalaColumn[_] =>
        val types: Seq[ScalaDataType[_]] = Seq(definition.dataType, sc.definition.dataType)
        assert(types.forall(_ == ScalaBooleanDataType), "types must be boolean for '||' operation")
        this.asInstanceOf[ScalaColumn[Boolean]] || sc.asInstanceOf[ScalaColumn[Boolean]]
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def isNotNull: GenericColumn = ScalaColumn(f"${definition.name}_IS_NOT_NULL", data.map(_ != null))

  override def isNull: GenericColumn = ScalaColumn(f"${definition.name}_IS_NULL", data.map(_ == null))

  override def as(name: String): ScalaColumn[A] = copy(definition = definition.copy(name = name))

  override def apply(extraction: Any): ScalaColumn[A] = throw new NotImplementedError("The 'apply' method is not applicable for a ScalaColumn instance")

  override def cast(dataType: GenericDataType): ScalaColumn[_] = {
    if (dataType.isSameType(definition.dataType)) this
    else dataType match {
      case scalaDataType: ScalaDataType[_] => unsafeCastTo(scalaDataType)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
    }
  }

  override def getName: Option[String] = Some(definition.name)

  override def desc: GenericColumn = sortDesc

  override def exprSql: String = throw new NotImplementedError("The 'exprSql' method is not applicable for ScalaColumns")

  override def subFeedType: universe.Type = typeOf[ScalaSubFeed]

  override def isin(list: Any*): GenericColumn = ScalaColumn[Boolean](f"${definition.name}_IS_IN_LIST", data.map(list.contains))

  def distinct: ScalaColumn[A] = copy(data = data.distinct)
}


object ScalaColumn {

  def apply[A: ClassTag](name: String, data: Seq[A]) = new ScalaColumn[A](ScalaColumnDefinition[A](name = name), data = data.toIndexedSeq)

}

