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
import io.smartdatalake.workflow.dataframe.plainScala.ScalaDataTypeEnum.{NUMBER, STRING, ScalaDataTypeEnum}
import org.json4s.{JString, JValue}

import scala.reflect.runtime.universe


case class ScalaDataType(inner: ScalaDataTypeEnum) extends GenericDataType with GenericSimpleDataType{
  def isSortable: Boolean = !Seq(STRING, NUMBER).contains(inner)

  def isNumeric: Boolean = inner == ScalaDataTypeEnum.NUMBER

  def isBoolean: Boolean = inner == ScalaDataTypeEnum.BOOLEAN

  def getDecimalSpec: Option[(Int, Int)] = None; //not relevant as java.math.BigDecimal is not accepted as input

  override def isSimpleType: Boolean = true; //only simple types as of now

  def typeName: String = inner.toString;

  def sql: String = throw new NotImplementedError("The 'sql' operation for the ScalaDataType is not applicable");

  def makeNullable: ScalaDataType = this;

  def toLowerCase: ScalaDataType = this;

  def removeMetadata: ScalaDataType = this;

  def toJson: JValue = JString(inner.toString);

  override def isSameType(other: GenericDataType): Boolean = {
    other match {
      case scalaOther: ScalaDataType => inner == scalaOther.inner
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
    }
  }

  override def subFeedType: universe.Type = ???//universe.typeOf[ScalaSubFeed]
}

object ScalaDataType {
  def fromValue(v: Any): ScalaDataType = {
    val enumType = v match {
      case _: Int | _: Double => ScalaDataTypeEnum.NUMBER
      case _: String => ScalaDataTypeEnum.STRING
      case _: Boolean => ScalaDataTypeEnum.BOOLEAN
      case _ => throw new Exception(s"A ScalaDataframe only accepts values of type Int, Double, String or Boolean. Could not match with value $v")
    }
    ScalaDataType(enumType)
  }
}

