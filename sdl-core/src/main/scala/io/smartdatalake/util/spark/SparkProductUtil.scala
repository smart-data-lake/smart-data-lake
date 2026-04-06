/*
 * Smart Data Lake - Build your data lake the smart way.
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

package io.smartdatalake.util.spark

import io.smartdatalake.util.misc.SchemaUtil.enrichSchemaCommentsFromCaseClass
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.apache.spark.sql.catalyst.{DeserializerBuildHelper, ScalaReflection, SerializerBuildHelper}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

object SparkProductUtil {

  def getSchemaFromCaseClass[T <: Product : TypeTag]: StructType = {
    val schema = Encoders.product[T].schema
    enrichSchemaCommentsFromCaseClass(schema, typeOf[T])
  }

  def getSchemaFromCaseClass(tpe: Type): StructType = {
    val schema = SparkProductUtil.createSchema(tpe)
    enrichSchemaCommentsFromCaseClass(schema, tpe)
  }

  /**
   * Create an Schema for a product based on it's type given as parameter (not as type parameter).
   */
  def createSchema(tpe: Type): StructType = {
    val mirror = ScalaReflection.mirror
    val cls = mirror.runtimeClass(tpe)
    ScalaReflection.encoderFor(tpe).schema
  }

  /**
   * Create an Encoder for a product based on it's type given as parameter (not as type parameter).
   */
  def createEncoder(tpe: Type): ExpressionEncoder[_] = {
    val mirror = ScalaReflection.mirror
    val cls = mirror.runtimeClass(tpe)
    val encoder = ScalaReflection.encoderFor(tpe)
    val serializer = SerializerBuildHelper.createSerializer(encoder)
    val deserializer = DeserializerBuildHelper.createDeserializer(encoder)
    new ExpressionEncoder(serializer, deserializer, ClassTag(cls))
  }

  /**
   * Create a Dataset based on the given type of a product.
   */
  def createDataset(df: DataFrame, tpe: Type): Dataset[_] = {
    df.as(SparkProductUtil.createEncoder(tpe))
  }
}
