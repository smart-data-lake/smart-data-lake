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
import io.smartdatalake.workflow.dataframe.{GenericDataType, GenericSimpleDataType}
import io.smartdatalake.util.misc.MetricsLog
import org.json4s.{JString, JValue}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object ScalaStringDataType extends ScalaDataType[String] {
  def isNumeric: Boolean = false

  def ordering: Ordering[String] = Ordering[String]

  def getCastFunction(fromDataType: ScalaDataType[_]): (Any => String) = {
    fromDataType match {
      case ScalaStringDataType => (x => x.asInstanceOf[String])
      case ScalaIntDataType => (x => x.asInstanceOf[Int].toString)
      case ScalaDoubleDataType => (x => x.asInstanceOf[Double].toString)
      case ScalaBooleanDataType => (x => x.asInstanceOf[Boolean].toString)
    }
  }

  override def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_] = {
    other match {
      case _ => this
    }
  }
}

object ScalaDoubleDataType extends ScalaDataType[Double] {
  def isNumeric: Boolean = true

  private val fractional = implicitly[Fractional[Double]]

  override def numeric: Numeric[Double] = fractional

  override def numericDiv: ((Double, Double) => Double) = fractional.div

  def ordering: Ordering[Double] = Ordering[Double]

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
  def isNumeric: Boolean = true

  private val integral = implicitly[Integral[Int]]

  override def numeric: Numeric[Int] = integral

  override def numericDiv: ((Int, Int) => Int) = integral.quot

  def ordering: Ordering[Int] = Ordering[Int]

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
  def isNumeric: Boolean = false

  def ordering: Ordering[Boolean] = Ordering[Boolean]

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

object ScalaSeqDataType extends ScalaDataType[Seq[_]] {

  private object SeqOrdering extends Ordering[Seq[_]] {
    def compare(x: Seq[_], y: Seq[_]): Int = if (x.head == y.head) 0 else 1
  }

  def ordering: Ordering[Seq[_]] = SeqOrdering

  def getCastFunction(fromDataType: ScalaDataType[_]): Any => Seq[_] = {
    fromDataType match {
      case ScalaIntDataType => (x => Seq(x.asInstanceOf[Int]))
      case ScalaStringDataType => (x => Seq(x.asInstanceOf[String].toInt))
      case ScalaDoubleDataType => (x => Seq(x.asInstanceOf[Double].toInt))
      case ScalaBooleanDataType => (x => Seq(if (x.asInstanceOf[Boolean]) 1 else 0))
    }
  }

  def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_] = this

  def isNumeric: Boolean = false

  override def isSimpleType: Boolean = false
}

object ScalaMetricsLogDataType extends ScalaDataType[MetricsLog] {

  private object MetricsLogOrdering extends Ordering[MetricsLog] {
    def compare(x: MetricsLog, y: MetricsLog): Int = x.start_tstmp.compareTo(y.start_tstmp)
  }

  def ordering: Ordering[MetricsLog] = MetricsLogOrdering

  def getCastFunction(fromDataType: ScalaDataType[_]): Any => MetricsLog = throw new UnsupportedOperationException("A MetricLog object cannot be cast from the types supported in ScalaDataFrame")

  def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_] = {
      other match {
        case ScalaStringDataType => other
        case _ => this
      }
  }

  def isNumeric: Boolean = false

  override def isSimpleType: Boolean = false

}

abstract class ScalaDataType[A: ClassTag] extends GenericDataType with GenericSimpleDataType {

  val scalaClass: Class[_] = implicitly[ClassTag[A]].runtimeClass

  def typeName: String = scalaClass.getSimpleName.toLowerCase

  def isSortable: Boolean = true

  def ordering: Ordering[A]

  def numeric: Numeric[A] = throw new IllegalStateException("'numeric' not implemented for this DataType")

  def numericDiv: ((A, A) => A) = throw new IllegalStateException("'numericDiv' not implemented for this DataType")

  def getDecimalSpec: Option[(Int, Int)] = None; //not relevant as java.math.BigDecimal is not accepted as input

  override def isSimpleType: Boolean = true;

  def sql: String = typeName

  def makeNullable: ScalaDataType[A] = this // this is for only for non-simple types

  def toLowerCase: ScalaDataType[A] = this // this is for only for non-simple types

  def removeMetadata: ScalaDataType[A] = this // this is for only for non-simple types

  def toJson: JValue = JString(typeName)

  override def isSameType(other: GenericDataType): Boolean = {
    other match {
      case scalaOther: ScalaDataType[A] => this == scalaOther // this works as trait implementations are case objects
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  def getCastFunction(fromDataType: ScalaDataType[_]): (Any => A)

  def getGreaterType(other: ScalaDataType[_]): ScalaDataType[_]

  def createColumnDefinition(name: String, nullable: Boolean = false, comment: Option[String] = None): ScalaColumnDefinition[A] = {
    ScalaColumnDefinition[A](name, nullable, comment)
  }

  def createLiteral(value: Any): ScalaLiteral[A] = {
    ScalaLiteral(value.asInstanceOf[A])
  }

  def castColumnDefinition(fromColumnDefinition: ScalaColumnDefinition[_]): ScalaColumnDefinition[A] = {
    ScalaColumnDefinition[A](fromColumnDefinition.name, fromColumnDefinition.nullable, fromColumnDefinition.comment)
  }

  def castColumn(fromColumn: ScalaColumn[_]): ScalaColumn[A] = {
    if (fromColumn.definition.dataType.scalaClass == this.scalaClass) fromColumn.asInstanceOf[ScalaColumn[A]]
    else {
      val castFun = getCastFunction(fromColumn.definition.dataType)
      castColumnDefinition(fromColumn.definition)
        .createColumn(fromColumn.data.map(x => castFun(x)))
    }
  }

  override def subFeedType: Type = typeOf[ScalaSubFeed]
}

object ScalaDataType {
  def getFor[A](implicit ct: ClassTag[A]): ScalaDataType[A] = getFor(ct.runtimeClass).asInstanceOf[ScalaDataType[A]]

  def getFor(cls: Class[_]): ScalaDataType[_] = {
    val isSequenceOrList = cls.getName == "scala.collection.Seq" || cls.getName == "scala.collection.immutable.$colon$colon"
    cls match {
      case cls if cls == classOf[String] => ScalaStringDataType
      case cls if cls == classOf[Int] || cls == classOf[java.lang.Integer] => ScalaIntDataType
      case cls if cls == classOf[Double] => ScalaDoubleDataType
      case cls if cls == classOf[Boolean] || cls == classOf[java.lang.Boolean] => ScalaBooleanDataType
      case _ if isSequenceOrList => ScalaSeqDataType
      case _ =>
        println(cls.getName)
        throw new Exception(s"A ScalaDataframe only accepts values of type Int, Double, String or Boolean. Could not match with class ${cls.getSimpleName}")
    }
  }
}

