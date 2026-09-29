/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.util.evolution

import io.smartdatalake.util.spark.evolution.TypeEvolutionUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class TypeEvolutionUtilTest extends AnyFunSuite {
  
  test("simple schema, no changes") {
    val srcSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    val srcRows = Seq(Row(1, "test"))
    val tgtRows = TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, srcSchema).toSeq
    assert(srcRows==tgtRows)
  }

  test("simple schema, new field") {
    val srcSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    val srcRows = Seq(Row(1, "test"))
    val tgtSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType), StructField("c", StringType)))
    val tgtRows = TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    assert(srcRows.size==tgtRows.size)
    assert(tgtRows.head.size==tgtSchema.size)
  }

  test("simple schema, removed field") {
    val srcSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    val srcRows = Seq(Row(1, "test"))
    val tgtSchema = StructType(Seq(StructField("a", IntegerType)))
    val tgtRows = TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    assert(srcRows.size==tgtRows.size)
    assert(tgtRows.head.size==tgtSchema.size)
  }

  test("simple schema, new and removed field") {
    val srcSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    val srcRows = Seq(Row(1, "test"))
    val tgtSchema = StructType(Seq(StructField("a", IntegerType), StructField("c", StringType)))
    val tgtRows = TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    assert(srcRows.size==tgtRows.size)
    assert(tgtRows.head.size==tgtSchema.size)
  }

  test("simple schema, unsupported data type change") {
    val srcSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    val srcRows = Seq(Row(1, "test"))
    val tgtSchema = StructType(Seq(StructField("a", IntegerType), StructField("b", DoubleType)))
    intercept[SchemaEvolutionException]{
      TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    }
  }

  test("nested schema, no changes") {
    val srcSchema = StructType(Seq(StructField("a", IntegerType), StructField("nested", StructType(Seq(StructField("a1", IntegerType), StructField("b1", StringType))))))
    val srcRows = Seq(Row(1, Row(11, "test")))
    val tgtRows = TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, srcSchema).toSeq
    tgtRows.foreach(println)
    assert(srcRows==tgtRows)
  }

  test("nested schema, new field") {
    val srcSchema = StructType(Seq(StructField("a", IntegerType), StructField("nested", StructType(Seq(StructField("a1", IntegerType), StructField("b1", StringType))))))
    val srcRows = Seq(Row(1, Row(11, "test")))
    val tgtSchema = StructType(Seq(StructField("a", IntegerType), StructField("nested", StructType(Seq(StructField("a1", IntegerType), StructField("b1", StringType), StructField("c1", StringType))))))
    val tgtRows = TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    val tgtRowsExpected = Seq(Row(1, Row(11,"test",null)))
    tgtRows.foreach(println)
    assert(tgtRows==tgtRowsExpected)
  }

  test("nested schema, removed field") {
    val srcSchema = StructType(Seq(StructField("a", IntegerType), StructField("nested", StructType(Seq(StructField("a1", IntegerType), StructField("b1", StringType))))))
    val srcRows = Seq(Row(1, Row(11, "test")))
    val tgtSchema = StructType(Seq(StructField("a", IntegerType), StructField("nested", StructType(Seq(StructField("a1", IntegerType))))))
    val tgtRows = TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    val tgtRowsExpected = Seq(Row(1, Row(11)))
    tgtRows.foreach(println)
    assert(tgtRows==tgtRowsExpected)
  }

  test("nested schema, new and removed field") {
    val srcSchema = StructType(Seq(StructField("a", IntegerType), StructField("nested", StructType(Seq(StructField("a1", IntegerType), StructField("b1", StringType))))))
    val srcRows = Seq(Row(1, Row(11, "test")))
    val tgtSchema = StructType(Seq(StructField("a", IntegerType), StructField("nested", StructType(Seq(StructField("a1", IntegerType), StructField("c1", StringType))))))
    val tgtRows = TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    val tgtRowsExpected = Seq(Row(1, Row(11,null)))
    tgtRows.foreach(println)
    assert(tgtRows==tgtRowsExpected)
  }

  test("nested schema, unsupported data type change") {
    val srcSchema = StructType(Seq(StructField("a", IntegerType), StructField("nested", StructType(Seq(StructField("a1", IntegerType), StructField("b1", StringType))))))
    val srcRows = Seq(Row(1, "test"))
    val tgtSchema = StructType(Seq(StructField("a", IntegerType), StructField("nested", StructType(Seq(StructField("a1", IntegerType), StructField("b1", DoubleType))))))
    intercept[SchemaEvolutionException]{
      TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    }
  }

  test("wider simple type") {
    import TypeEvolutionUtil.widerSimpleType
    def assertWider(l: DataType, r: DataType, expected: Option[DataType]): Unit = {
      assert(widerSimpleType(l, r) == expected, s"$l / $r")
      assert(widerSimpleType(r, l) == expected, s"$r / $l")
    }
    assertWider(IntegerType, IntegerType, Some(IntegerType))
    assertWider(IntegerType, LongType, Some(LongType))
    assertWider(ByteType, ShortType, Some(ShortType))
    assertWider(ShortType, FloatType, Some(FloatType))
    assertWider(IntegerType, FloatType, Some(DoubleType))
    assertWider(LongType, DoubleType, Some(DoubleType))
    assertWider(FloatType, DoubleType, Some(DoubleType))
    assertWider(DecimalType(38, 10), DecimalType(3, 0), Some(DecimalType(38, 10)))
    assertWider(DecimalType(10, 2), DecimalType(5, 4), Some(DecimalType(12, 4)))
    // integral digits are kept if the maximum precision is exceeded
    assertWider(DecimalType(38, 0), DecimalType(10, 5), Some(DecimalType(38, 0)))
    assertWider(DecimalType(38, 0), IntegerType, Some(DecimalType(38, 0)))
    assertWider(DecimalType(3, 0), IntegerType, Some(DecimalType(10, 0)))
    assertWider(DecimalType(5, 2), LongType, Some(DecimalType(22, 2)))
    assertWider(DecimalType(10, 2), DoubleType, Some(DoubleType))
    assertWider(DecimalType(20, 2), DoubleType, None)
    assertWider(IntegerType, StringType, Some(StringType))
    assertWider(DecimalType(10, 2), StringType, Some(StringType))
    assertWider(BooleanType, IntegerType, None)
    assertWider(DateType, TimestampType, None)
  }

  test("nested decimal and integral are converted to wider decimal") {
    val srcSchema = StructType(Seq(StructField("a", DecimalType(3, 0)), StructField("b", IntegerType), StructField("c", IntegerType)))
    val tgtSchema = StructType(Seq(StructField("a", DecimalType(38, 10)), StructField("b", DecimalType(38, 0)), StructField("c", DoubleType)))
    val srcRows = Seq(Row(new java.math.BigDecimal(123), 5, 7), Row(null, null, null))
    val tgtRows = TypeEvolutionUtil.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    assert(tgtRows.head == Row(new java.math.BigDecimal(123).setScale(10), new java.math.BigDecimal(5), 7d))
    assert(tgtRows(1) == Row(null, null, null))
  }

  test("nested decimal can not be converted to narrower decimal") {
    val srcSchema = StructType(Seq(StructField("a", DecimalType(38, 10))))
    val tgtSchema = StructType(Seq(StructField("a", DecimalType(3, 0))))
    intercept[SchemaEvolutionException] {
      TypeEvolutionUtil.schemaEvolution(Iterator(Row(new java.math.BigDecimal(1))), srcSchema, tgtSchema).toSeq
    }
  }
}
