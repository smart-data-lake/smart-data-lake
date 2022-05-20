/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.spark

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.Type

object EncoderUtil {

  def createProductEncoder(tpe: Type): ExpressionEncoder[_] = {
    val mirror = ScalaReflection.mirror
    val cls = mirror.runtimeClass(tpe)
    val serializer = ScalaReflection.serializerForType(tpe)
    val deserializer = ScalaReflection.deserializerForType(tpe)
    new ExpressionEncoder(serializer, deserializer, ClassTag(cls))
  }

  def createDataset(df: DataFrame, tpe: Type): Dataset[_] = {
    df.as(createProductEncoder(tpe))
  }
}