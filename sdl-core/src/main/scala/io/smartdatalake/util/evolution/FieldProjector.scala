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
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/**
 * Trait for projection of the value of a row field. The value might be a simple type but also a struct, array or map.
 * The FieldProjector is responsible to extract the value from the row field, then it uses a [[ValueProjector]] for the conversion.
 */
private[smartdatalake] trait FieldProjector {
  def get(inRow: Row): Any
}

private[smartdatalake] case class NewFieldProjector(tgtType: DataType, path: Seq[String]) extends FieldProjector {
  def get(inRow: Row): Any = null
}

private[smartdatalake] case class CopyFieldProjector(srcIdx: Int, path: Seq[String] ) extends FieldProjector {
  def get(inRow: Row): Any = {
    if (inRow==null) inRow
    else inRow.get(srcIdx)
  }
}

private[smartdatalake] case class SimpleTypeFieldProjector(srcIdx: Int, srcType: DataType, tgtType: DataType, path: Seq[String] ) extends FieldProjector {
  private val projector = SimpleTypeValueProjector(srcType, tgtType, path)
  def get(inRow: Row): Any = {
    if (inRow==null) inRow
    else projector.get(inRow.get(srcIdx))
  }
}

private[smartdatalake] case class StructTypeFieldProjector(srcIdx: Int, srcSchema: StructType, tgtSchema: StructType, path: Seq[String] ) extends FieldProjector {
  private val projector = StructTypeValueProjector(srcSchema, tgtSchema, path)
  def get(inRow: Row): Any = {
    if (inRow==null) inRow
    else projector.get(inRow.getStruct(srcIdx))
  }
}

private[smartdatalake] case class ArrayTypeFieldProjector(srcIdx: Int, srcType: ArrayType, tgtType: ArrayType, path: Seq[String] ) extends FieldProjector {
  private val projector = ArrayTypeValueProjector(srcType, tgtType, path)
  def get(inRow: Row): Any = {
    if (inRow==null) inRow
    else projector.get(inRow.getSeq[Any](srcIdx))
  }
}

private[smartdatalake] case class MapTypeFieldProjector(srcIdx: Int, srcType: MapType, tgtType: MapType, path: Seq[String] ) extends FieldProjector {
  private val projector = MapTypeValueProjector(srcType, tgtType, path)
  def get(inRow: Row): Any = {
    if (inRow==null) inRow
    else projector.get(inRow.getMap[Any,Any](srcIdx).toMap)
  }
}

private[smartdatalake] object FieldProjector {

  private[evolution] def getFieldProjection(srcIdx: Int, srcType: DataType, tgtType: DataType, path: Seq[String]): FieldProjector = {
    (srcType, tgtType) match {
      case (_, _) if srcType.simpleString == tgtType.simpleString => // data type equal
        CopyFieldProjector(srcIdx, path)
      case (srcType: StructType, tgtType:StructType) => // struct type changed
        StructTypeFieldProjector(srcIdx, srcType, tgtType, path)
      case (srcType: ArrayType, tgtType:ArrayType) => // array type changed
        ArrayTypeFieldProjector(srcIdx, srcType, tgtType, path)
      case (srcType: MapType, tgtType:MapType) => // map type changed
        MapTypeFieldProjector(srcIdx, srcType, tgtType, path)
      case (_,_) => // simple type changed
        SimpleTypeFieldProjector(srcIdx, srcType, tgtType, path)
    }
  }
}
