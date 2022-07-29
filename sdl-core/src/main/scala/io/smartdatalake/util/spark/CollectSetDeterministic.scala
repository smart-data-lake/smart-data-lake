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

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{Collect, ImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, BinaryType, ByteType}

import scala.collection.mutable

/**
 * To collect filenames via observable metrics we need the collect_set function.
 * Unfortunately collect_set is not deterministic because the order in the set can be arbitrary. This is a strange interpretation as a set normally is unordered.
 * For observable metrics only deterministic aggregate functions can be used. This class is copied from Spark source code to make this operator deterministic.
 * Link to original Spark source code:
 * https://github.com/apache/spark/blob/58e07e0f4cca1e3a6387a7e0c57faeb6c5ec9ef5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/collect.scala#L145
 *
 * Note: There is a Spark problem (NullPointerException with TypedImperativeAggregate (like CollectSetDeterministic) in observe if there is no data! As a workaround we have to check for no data in SparkFileDataObject.
 * see also https://issues.apache.org/jira/browse/SPARK-39044
 *
 * Note: Another try was to create user defined aggregate function (see MyCollectSet below), but they do not work in observable metrics as well (strange serialization exception).
 */
case class CollectSetDeterministic(
                                    child: Expression,
                                    mutableAggBufferOffset: Int = 0,
                                    inputAggBufferOffset: Int = 0) extends Collect[mutable.HashSet[Any]] {

  def this(child: Expression) = this(child, 0, 0)

  override lazy val bufferElementType = child.dataType match {
    case BinaryType => ArrayType(ByteType)
    case other => other
  }

  // SDL-Change: In contrast to CollectSet we mark CollectSetDeterministic here as deterministic.
  override lazy val deterministic: Boolean = true

  override def convertToBufferElement(value: Any): Any = child.dataType match {
    /*
     * collect_set() of BinaryType should not return duplicate elements,
     * Java byte arrays use referential equality and identity hash codes
     * so we need to use a different catalyst value for arrays
     */
    case BinaryType => UnsafeArrayData.fromPrimitiveArray(value.asInstanceOf[Array[Byte]])
    case _ => InternalRow.copyValue(value)
  }

  override def eval(buffer: mutable.HashSet[Any]): Any = {
    val array = child.dataType match {
      case BinaryType =>
        buffer.iterator.map(_.asInstanceOf[ArrayData].toByteArray).toArray
      case _ => buffer.toArray
    }
    new GenericArrayData(array)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_set"

  override def createAggregationBuffer(): mutable.HashSet[Any] = mutable.HashSet.empty

  override protected def withNewChildInternal(newChild: Expression): CollectSetDeterministic =
    copy(child = newChild)
}

object CollectSetDeterministic {
  def collect_set_deterministic(e: Column): Column = new Column(CollectSetDeterministic(e.expr).toAggregateExpression(false))
}

/*
// Alternative approach for a custom aggregator. But this one has strange serialization errors when used with df.observe :-(
// It is left as comment here for possible future tests.
object MyCollectSet extends Aggregator[String, Array[String], Array[String]] with Serializable {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Array[String] = Array()
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Array[String], in: String): Array[String] = if (buffer.contains(in)) buffer else buffer :+ in
  // Merge two intermediate values
  def merge(b1: Array[String], b2: Array[String]): Array[String] = (b1 ++ b2).distinct
  // Transform the output of the reduction
  def finish(reduction: Array[String]): Array[String] = reduction
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Array[String]] = Encoders.javaSerialization
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Array[String]] = Encoders.javaSerialization
}
*/