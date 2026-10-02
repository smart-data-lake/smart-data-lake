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

import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, HashExpression, InterpretedHashFunction, Murmur3HashFunction}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.unsafe.hash.Murmur3_x86_32


/**
 * A null aware MurMur3 Hash expression.
 * Copied from org.apache.spark.sql.catalyst.expressions.Murmur3Hash, but with the following changes:
 * - It is null aware, i.e. it treat null values as regular values, which influence hash value of the row.
 *   The original Murmur3Hash ignores null values, e.g. a row with an additional null column has the same hash as without.
 */
case class NullAwareMurmur3HashExpr(children: Seq[Expression], seed: Int) extends HashExpression[Int] {
  def this(arguments: Seq[Expression]) = this(arguments, 42)

  override def dataType: DataType = IntegerType

  override def prettyName: String = "null_aware_hash"

  override protected def hasherClassName: String = classOf[Murmur3_x86_32].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Int): Int = {
    NullAwareMurmur3HashExpr.hash(value, dataType, seed).toInt
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): NullAwareMurmur3HashExpr =
    copy(children = newChildren)
}

object NullAwareMurmur3HashExpr extends InterpretedHashFunction {

  override def hash(value: Any, dataType: DataType, seed: Long): Long = {
    value match {
      /** here comes the change for treating null */
      case null => seed + 1 // add a constant value
      case _ => super.hash(value, dataType, seed)
    }
  }

  override protected def hashInt(i: Int, seed: Long): Long = {
    Murmur3_x86_32.hashInt(i, seed.toInt)
  }

  override protected def hashLong(l: Long, seed: Long): Long = {
    Murmur3_x86_32.hashLong(l, seed.toInt)
  }

  override protected def hashUnsafeBytes(base: AnyRef, offset: Long, len: Int, seed: Long): Long = {
    Murmur3_x86_32.hashUnsafeBytes(base, offset, len, seed.toInt)
  }
}