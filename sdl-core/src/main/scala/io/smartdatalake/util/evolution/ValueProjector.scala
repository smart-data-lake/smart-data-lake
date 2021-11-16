/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.util.evolution

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Trait for projection of a value. Value might be a simple type but also a struct, array or map.
 * The ValueProjector is responsible for the conversion of the value.
 */
private[smartdatalake] trait ValueProjector[T] {
  def getWithCast(in: Any): T = get(in.asInstanceOf[T])
  def get(in: T): T
}

private[smartdatalake] case class CopyValueProjector(path: Seq[String] ) extends ValueProjector[Any] {
  def get(in: Any): Any = in
}

private[smartdatalake] case class SimpleTypeValueProjector(srcType: DataType, tgtType: DataType, path: Seq[String] ) extends ValueProjector[Any] {
  private val converter = ValueProjector.getSimpleTypeConverter(srcType, tgtType, path)
  def get(in: Any): Any = converter(in)
}

private[smartdatalake] case class StructTypeValueProjector(srcSchema: StructType, tgtSchema: StructType, path: Seq[String] ) extends ValueProjector[Row] {
  private val projection = ValueProjector.getStructTypeProjection(srcSchema, tgtSchema, path)
  def get(in: Row): Row = ValueProjector.applyStructTypeProjection(in, projection)
}

private[smartdatalake] case class ArrayTypeValueProjector(srcType: ArrayType, tgtType: ArrayType, path: Seq[String] ) extends ValueProjector[Seq[Any]] {
  private val elementProjection = ValueProjector.getProjection(srcType.elementType, tgtType.elementType, path)
  def get(in: Seq[Any]): Seq[Any] = in.map( e => elementProjection.getWithCast(e))
}

private[smartdatalake] case class MapTypeValueProjector(srcType: MapType, tgtType: MapType, path: Seq[String] ) extends ValueProjector[Map[Any,Any]] {
  private val keyProjection = ValueProjector.getProjection(srcType.keyType, tgtType.keyType, path)
  private val valueProjection = ValueProjector.getProjection(srcType.valueType, tgtType.valueType, path)
  def get(in: Map[Any,Any]): Map[Any,Any] = in.map{
    case (k, v) => (keyProjection.getWithCast(k) ->  valueProjection.getWithCast(v))
  }
}

private[smartdatalake] object ValueProjector {

  private[evolution] def getProjection(srcType: DataType, tgtType: DataType, path: Seq[String]): ValueProjector[_] = {
    (srcType, tgtType) match {
      case (_, _) if srcType.simpleString == tgtType.simpleString => // data type equal
        CopyValueProjector(path)
      case (srcType: StructType, tgtType:StructType) => // struct type changed
        StructTypeValueProjector(srcType, tgtType, path)
      case (srcType: ArrayType, tgtType:ArrayType) => // array type changed
        ArrayTypeValueProjector(srcType, tgtType, path)
      case (srcType: MapType, tgtType:MapType) => // map type changed
        MapTypeValueProjector(srcType, tgtType, path)
      case (_,_) => // try simple type changed
        SimpleTypeValueProjector(srcType, tgtType, path)
    }
  }

  private[evolution] def getSimpleTypeConverter(srcType: DataType, tgtType: DataType, path: Seq[String]): (Any => Any) = {
    val converterFunc: (Any => Any) = (srcType, tgtType) match {
      // numeric types to string
      case (_: NumericType, _: StringType) => (x => x.toString)
      case (_: CharType, _: StringType) => (x => x.asInstanceOf[Char].toString)
      case (_: BooleanType, _: StringType) => (x => x.asInstanceOf[Boolean].toString)
      // numbers to other numbers
      case (_: ByteType, _: LongType) => (x => x.asInstanceOf[Byte].toLong)
      case (_: ShortType, _: LongType) => (x => x.asInstanceOf[Short].toLong)
      case (_: IntegerType, _: LongType) => (x => x.asInstanceOf[Int].toLong)
      case (_: ByteType, _: IntegerType) => (x => x.asInstanceOf[Byte].toInt)
      case (_: ShortType, _: IntegerType) => (x => x.asInstanceOf[Short].toInt)
      case (_: ByteType, _: ShortType) => (x => x.asInstanceOf[Byte].toShort)
      case (_: FloatType, _: DoubleType) => (x => x.asInstanceOf[Float].toDouble)
      case (d: DecimalType, _: ByteType) if d.scale == 0 && d.precision <= 2 => (x => x.asInstanceOf[BigDecimal].toByte)
      case (d: DecimalType, _: ShortType) if d.scale == 0 && d.precision <= 4 => (x => x.asInstanceOf[BigDecimal].toShort)
      case (d: DecimalType, _: IntegerType) if d.scale == 0 && d.precision <= 9 => (x => x.asInstanceOf[BigDecimal].toInt)
      case (d: DecimalType, _: LongType) if d.scale == 0 && d.precision <= 18 => (x => x.asInstanceOf[BigDecimal].toLong)
      case (d: DecimalType, _: FloatType) if d.precision <= 7 => (x => x.asInstanceOf[BigDecimal].toFloat)
      case (d: DecimalType, _: DoubleType) if d.precision <= 16 => (x => x.asInstanceOf[BigDecimal].toDouble)
      case _ => throw SchemaEvolutionException(s"""schema evolution from $srcType to $tgtType not supported (field ${path.mkString(",")})""")
    }
    // add null check
    (x => if (x != null) converterFunc(x) else x)
  }

  private[evolution] def getStructTypeProjection(srcSchema: StructType, tgtSchema: StructType, path: Seq[String] = Seq()): Seq[FieldProjector] = {
    tgtSchema.map {
      tgtField =>
        val name = tgtField.name
        val srcTypeOpt = srcSchema.find( _.name == name).map(_.dataType)
        val tgtType = tgtField.dataType
        val newPath = path :+ name
        if (srcTypeOpt.isDefined) {
          val srcIdx = srcSchema.fieldIndex(name)
          FieldProjector.getFieldProjection(srcIdx, srcTypeOpt.get, tgtType, newPath)
        } else {
          NewFieldProjector(tgtType, newPath)
        }
    }
  }

  private[evolution] def applyStructTypeProjection(inRow: Row, projection: Seq[FieldProjector]): Row = {
    Row.fromSeq( projection.map( _.get(inRow)))
  }
}
