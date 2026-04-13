/*
 * sdl-core - Build your data lake the smart way.
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

import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{GenericDataType, GenericSimpleDataType}
import io.smartdatalake.util.misc.MetricsLog
import org.json4s.{JString, JValue}

import java.sql.Timestamp
import scala.math.Ordering
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Base type for DataTypes of ScalaDataFrame columns.
 * The supported types are Int, Double, String, Boolean, Timestamp and Array.
 * Additionally, there is Null type for special cases where expressions have no type, e.g. lit(null).
 */
abstract class ScalaDataType[A: ClassTag] extends GenericDataType with GenericSimpleDataType {

  val scalaClass: Class[_] = implicitly[ClassTag[A]].runtimeClass

  def typeName: String = scalaClass.getSimpleName.toLowerCase

  def isSortable: Boolean = true

  def ordering: Ordering[Option[A]]

  def numeric: Numeric[A] = throw new IllegalStateException("'numeric' not implemented for this DataType")

  def numericDiv: ((A, A) => A) = throw new IllegalStateException("'numericDiv' not implemented for this DataType")

  def getDecimalSpec: Option[(Int, Int)] = None; //not relevant as java.math.BigDecimal is not accepted as input

  override def isSimpleType: Boolean = true;

  def sql: String = typeName

  def makeNullable: ScalaDataType[A] = this // this is only for non-simple types

  def toLowerCase: ScalaDataType[A] = this // this is only for non-simple types

  def removeMetadata: ScalaDataType[A] = this // this is only for non-simple types

  def toJson: JValue = JString(typeName)

  override def isSameType(other: GenericDataType): Boolean = {
    other match {
      case scalaOther: ScalaDataType[A] => this == scalaOther // this works as trait implementations are case objects
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  def getCastFunction(fromDataType: ScalaDataType[_]): (Any => A)

  def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_]

  def createColumnDefinition(name: String, nullable: Boolean = true, comment: Option[String] = None): ScalaColumnDefinition[A] = {
    ScalaColumnDefinition[A](name, None, nullable, comment, Some(this))
  }

  def createLiteral(value: Any): ScalaLiteral[A] = {
    val litValue = value match {
      case x: Option[A] => x
      case x => Option(x).map(_.asInstanceOf[A])
    }
    ScalaLiteral(litValue)
  }

  def castColumnDefinition(fromColumnDefinition: ScalaColumnDefinition[_]): ScalaColumnDefinition[A] = {
    ScalaColumnDefinition[A](fromColumnDefinition.name, None, fromColumnDefinition.nullable, fromColumnDefinition.comment)
  }

  def castColumn(fromColumn: ScalaColumn[_]): ScalaColumn[A] = {
    if (fromColumn.definition.dataType.scalaClass == this.scalaClass) fromColumn.asInstanceOf[ScalaColumn[A]]
    else {
      val castFun = getCastFunction(fromColumn.definition.dataType)
      castColumnDefinition(fromColumn.definition)
        .createColumn(fromColumn.data.map(x => x.map(castFun)))
    }
  }

  override def subFeedType: Type = typeOf[ScalaSubFeed]
}

object ScalaStringDataType extends ScalaDataType[String] {
  def isNumeric: Boolean = false

  override def isImpreciseNumeric: Boolean = false

  def ordering: Ordering[Option[String]] = Ordering.Option(new Ordering[String] {
    def compare(x: String, y: String): Int = if (x == null) -1 else if (y == null) 1 else  x.compareTo(y)
  })

  def getCastFunction(fromDataType: ScalaDataType[_]): (Any => String) = {
    fromDataType match {
      case ScalaStringDataType => (x => x.asInstanceOf[String])
      case ScalaIntDataType => (x => x.asInstanceOf[Int].toString)
      case ScalaDoubleDataType => (x => x.asInstanceOf[Double].toString)
      case ScalaBooleanDataType => (x => x.asInstanceOf[Boolean].toString)
      case ScalaNullDataType => (_ => null)
    }
  }

  override def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_] = {
    other match {
      case _ => this
    }
  }
}

object ScalaDoubleDataType extends ScalaDataType[Double] {
  override def isNumeric: Boolean = true

  override def isImpreciseNumeric: Boolean = true

  private val fractional = implicitly[Fractional[Double]]

  override def numeric: Numeric[Double] = fractional

  override def numericDiv: ((Double, Double) => Double) = fractional.div

  def ordering: Ordering[Option[Double]] = Ordering.Option(Ordering[Double])

  override def getCastFunction(fromDataType: ScalaDataType[_]): (Any => Double) = {
    fromDataType match {
      case ScalaDoubleDataType => (x => x.asInstanceOf[Double])
      case ScalaStringDataType => (x => x.asInstanceOf[String].toDouble)
      case ScalaIntDataType => (x => x.asInstanceOf[Int].toDouble)
      case ScalaBooleanDataType => (x => if (x.asInstanceOf[Boolean]) 1d else 0d)
    }
  }

  override def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_] = {
    other match {
      case ScalaStringDataType => other
      case _ => this
    }
  }
}

object ScalaIntDataType extends ScalaDataType[Int] {
  override def isNumeric: Boolean = true

  override def isImpreciseNumeric: Boolean = false

  private val integral = implicitly[Integral[Int]]

  override def numeric: Numeric[Int] = integral

  override def numericDiv: ((Int, Int) => Int) = integral.quot

  def ordering: Ordering[Option[Int]] = Ordering.Option(Ordering[Int])

  override def getCastFunction(fromDataType: ScalaDataType[_]): (Any => Int) = {
    fromDataType match {
      case ScalaIntDataType => (x => x.asInstanceOf[Int])
      case ScalaStringDataType => (x => x.asInstanceOf[String].toInt)
      case ScalaDoubleDataType => (x => x.asInstanceOf[Double].toInt)
      case ScalaBooleanDataType => (x => if (x.asInstanceOf[Boolean]) 1 else 0)
    }
  }

  override def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_] = {
    other match {
      case ScalaStringDataType | ScalaDoubleDataType => other
      case _ => this
    }
  }
}

object ScalaBooleanDataType extends ScalaDataType[Boolean] {
  override def isNumeric: Boolean = false

  override def isImpreciseNumeric: Boolean = false

  def ordering: Ordering[Option[Boolean]] = Ordering.Option(Ordering[Boolean])

  override def getCastFunction(fromDataType: ScalaDataType[_]): (Any => Boolean) = {
    fromDataType match {
      case ScalaBooleanDataType => (x => x.asInstanceOf[Boolean])
      case ScalaStringDataType => (x => x.asInstanceOf[String].toLowerCase == "true")
      case ScalaIntDataType => (x => x.asInstanceOf[Int] > 0)
      case ScalaDoubleDataType => (x => x.asInstanceOf[Double] > 0d)
    }
  }

  override def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_] = {
    other match {
      case ScalaStringDataType | ScalaIntDataType | ScalaDoubleDataType => other
      case _ => this
    }
  }
}


object ScalaTimestampDataType extends ScalaDataType[Timestamp] {
  override def isNumeric: Boolean = false

  override def isImpreciseNumeric: Boolean = false

  // TODO: only needed in Scala 2.12, can be removed when we drop Scala 2.12.
  implicit val timestampOrdering: Ordering[Timestamp] = Ordering.fromLessThan(_.compareTo(_) < 0)

  def ordering: Ordering[Option[Timestamp]] = Ordering.Option(Ordering[Timestamp])

  override def numeric: Numeric[Timestamp] = new Numeric[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = ordering.compare(Option(x),Option(y)) // this allows for comparision in ScalaBinaryExpr
    // Timestamp arithmetic is intentionally unsupported.
    def plus(x: Timestamp, y: Timestamp): Timestamp = throw new UnsupportedOperationException("Timestamp plus is not supported")
    def minus(x: Timestamp, y: Timestamp): Timestamp = throw new UnsupportedOperationException("Timestamp minus is not supported")
    def times(x: Timestamp, y: Timestamp): Timestamp = throw new UnsupportedOperationException("Timestamp times is not supported")
    def negate(x: Timestamp): Timestamp = throw new UnsupportedOperationException("Timestamp negate is not supported")
    def fromInt(x: Int): Timestamp = throw new UnsupportedOperationException("Timestamp fromInt is not supported")
    def parseString(str: String): Option[Timestamp] = throw new UnsupportedOperationException("Timestamp parseString is not supported")
    def toInt(x: Timestamp): Int = throw new UnsupportedOperationException("Timestamp toInt is not supported")
    def toLong(x: Timestamp): Long = throw new UnsupportedOperationException("Timestamp toLong is not supported")
    def toFloat(x: Timestamp): Float = throw new UnsupportedOperationException("Timestamp toFloat is not supported")
    def toDouble(x: Timestamp): Double = throw new UnsupportedOperationException("Timestamp toDouble is not supported")
  }

  override def getCastFunction(fromDataType: ScalaDataType[_]): (Any => Timestamp) = {
    fromDataType match {
      case ScalaNullDataType => (_ => null)
    }
  }

  override def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_] = this
}

case class ScalaArrayDataType(elementType: Option[ScalaDataType[_]]) extends ScalaDataType[Seq[_]] {

  override def typeName: String = "array"

  private object SeqOrdering extends Ordering[Seq[_]] {
    // TODO: this is a simplistic implementation, we should implement a more complete one if we want to support ordering on Seq types
    def compare(x: Seq[_], y: Seq[_]): Int = if (x.head == y.head) 0 else 1
  }

  def ordering: Ordering[Option[Seq[_]]] = Ordering.Option(SeqOrdering)

  def getCastFunction(fromDataType: ScalaDataType[_]): Any => Seq[_] = {
    fromDataType match {
      case ScalaNullDataType => (_ => null)
    }
  }

  def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_] = this

  override def isNumeric: Boolean = false

  override def isImpreciseNumeric: Boolean = false

  override def isSimpleType: Boolean = false
}

/**
 * This is a special DataType for the case where expressions have no type, e.g. lit(null).
 * It is not intended to be used as a column data type, but only as a placeholder for null literals in expressions.
 * It can be cast to any other DataType and is considered smaller than all other DataTypes when comparing types.
 */
object ScalaNullDataType extends ScalaDataType[Null] {
  override def isNumeric: Boolean = false

  override def isImpreciseNumeric: Boolean = false

  def ordering: Ordering[Option[Null]] = Ordering.Option(Ordering[Null])

  override def getCastFunction(fromDataType: ScalaDataType[_]): (Any => Null) = {
    throw new UnsupportedOperationException("A ScalaTimestampDataType cannot be cast from other types supported in ScalaDataFrame")
  }

  override def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_] = other
}

object ScalaDataType {
  def getFor[A](implicit ct: ClassTag[A]): ScalaDataType[A] = getFor(ct.runtimeClass).asInstanceOf[ScalaDataType[A]]

  def getFor(cls: Class[_]): ScalaDataType[_] = {
    cls match {
      case cls if cls == classOf[Null] || cls == null => ScalaNullDataType
      case cls if cls == classOf[String] => ScalaStringDataType
      case cls if cls == classOf[Int] || cls == classOf[java.lang.Integer] => ScalaIntDataType
      case cls if cls == classOf[Double] || cls == classOf[java.lang.Double] => ScalaDoubleDataType
      case cls if cls == classOf[Boolean] || cls == classOf[java.lang.Boolean] => ScalaBooleanDataType
      case cls if cls == classOf[Timestamp] => ScalaTimestampDataType
      case cls if classOf[Iterable[_]].isAssignableFrom(cls) => ScalaArrayDataType(None)
      case _ =>
        throw new Exception(s"A ScalaDataframe only accepts values of type Int, Double, String, Boolean, Timestamp and Array. Could not match with class ${cls.getName}")
    }
  }

  def getFor(sqlType: String): ScalaDataType[_] = {
    sqlType.toLowerCase match {
      case "null" => ScalaNullDataType
      case "string" => ScalaStringDataType
      case "int" | "integer" => ScalaIntDataType
      case "double" => ScalaDoubleDataType
      case "bool" => ScalaBooleanDataType
      case "timestamp" => ScalaTimestampDataType
      case _ =>
        throw new Exception(s"A ScalaDataframe only accepts values of type Int, Double, String, Boolean, Timestamp to create SQL types. Could not match with SQL type $sqlType")
    }
  }
}

