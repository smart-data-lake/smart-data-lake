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
import org.scalatest.FunSuite

class ComplexTypeEvolutionTest extends FunSuite {
  
  test("simple schema, no changes") {
    val srcSchema = StructType(Seq(StructField("a",IntegerType),StructField("b",StringType)))
    val srcRows = Seq(Row(1,"test"))
    val tgtRows = ComplexTypeEvolution.schemaEvolution(srcRows.iterator, srcSchema, srcSchema).toSeq
    assert(srcRows==tgtRows)
  }

  test("simple schema, new field") {
    val srcSchema = StructType(Seq(StructField("a",IntegerType),StructField("b",StringType)))
    val srcRows = Seq(Row(1,"test"))
    val tgtSchema = StructType(Seq(StructField("a",IntegerType),StructField("b",StringType),StructField("c",StringType)))
    val tgtRows = ComplexTypeEvolution.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    assert(srcRows.size==tgtRows.size)
    assert(tgtRows.head.size==tgtSchema.size)
  }

  test("simple schema, removed field") {
    val srcSchema = StructType(Seq(StructField("a",IntegerType),StructField("b",StringType)))
    val srcRows = Seq(Row(1,"test"))
    val tgtSchema = StructType(Seq(StructField("a",IntegerType)))
    val tgtRows = ComplexTypeEvolution.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    assert(srcRows.size==tgtRows.size)
    assert(tgtRows.head.size==tgtSchema.size)
  }

  test("simple schema, new and removed field") {
    val srcSchema = StructType(Seq(StructField("a",IntegerType),StructField("b",StringType)))
    val srcRows = Seq(Row(1,"test"))
    val tgtSchema = StructType(Seq(StructField("a",IntegerType),StructField("c",StringType)))
    val tgtRows = ComplexTypeEvolution.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    assert(srcRows.size==tgtRows.size)
    assert(tgtRows.head.size==tgtSchema.size)
  }

  test("simple schema, unsupported data type change") {
    val srcSchema = StructType(Seq(StructField("a",IntegerType),StructField("b",StringType)))
    val srcRows = Seq(Row(1,"test"))
    val tgtSchema = StructType(Seq(StructField("a",IntegerType),StructField("b",DoubleType)))
    intercept[SchemaEvolutionException]{
      ComplexTypeEvolution.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    }
  }

  test("nested schema, no changes") {
    val srcSchema = StructType(Seq(StructField("a",IntegerType),StructField("nested",StructType(Seq(StructField("a1",IntegerType),StructField("b1",StringType))))))
    val srcRows = Seq(Row(1, Row(11,"test")))
    val tgtRows = ComplexTypeEvolution.schemaEvolution(srcRows.iterator, srcSchema, srcSchema).toSeq
    tgtRows.foreach(println)
    assert(srcRows==tgtRows)
  }

  test("nested schema, new field") {
    val srcSchema = StructType(Seq(StructField("a",IntegerType),StructField("nested",StructType(Seq(StructField("a1",IntegerType),StructField("b1",StringType))))))
    val srcRows = Seq(Row(1, Row(11,"test")))
    val tgtSchema = StructType(Seq(StructField("a",IntegerType),StructField("nested",StructType(Seq(StructField("a1",IntegerType),StructField("b1",StringType),StructField("c1",StringType))))))
    val tgtRows = ComplexTypeEvolution.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    val tgtRowsExpected = Seq(Row(1, Row(11,"test",null)))
    tgtRows.foreach(println)
    assert(tgtRows==tgtRowsExpected)
  }

  test("nested schema, removed field") {
    val srcSchema = StructType(Seq(StructField("a",IntegerType),StructField("nested",StructType(Seq(StructField("a1",IntegerType),StructField("b1",StringType))))))
    val srcRows = Seq(Row(1, Row(11,"test")))
    val tgtSchema = StructType(Seq(StructField("a",IntegerType),StructField("nested",StructType(Seq(StructField("a1",IntegerType))))))
    val tgtRows = ComplexTypeEvolution.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    val tgtRowsExpected = Seq(Row(1, Row(11)))
    tgtRows.foreach(println)
    assert(tgtRows==tgtRowsExpected)
  }

  test("nested schema, new and removed field") {
    val srcSchema = StructType(Seq(StructField("a",IntegerType),StructField("nested",StructType(Seq(StructField("a1",IntegerType),StructField("b1",StringType))))))
    val srcRows = Seq(Row(1, Row(11,"test")))
    val tgtSchema = StructType(Seq(StructField("a",IntegerType),StructField("nested",StructType(Seq(StructField("a1",IntegerType),StructField("c1",StringType))))))
    val tgtRows = ComplexTypeEvolution.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    val tgtRowsExpected = Seq(Row(1, Row(11,null)))
    tgtRows.foreach(println)
    assert(tgtRows==tgtRowsExpected)
  }

  test("nested schema, unsupported data type change") {
    val srcSchema = StructType(Seq(StructField("a",IntegerType),StructField("nested",StructType(Seq(StructField("a1",IntegerType),StructField("b1",StringType))))))
    val srcRows = Seq(Row(1,"test"))
    val tgtSchema = StructType(Seq(StructField("a",IntegerType),StructField("nested",StructType(Seq(StructField("a1",IntegerType),StructField("b1",DoubleType))))))
    intercept[SchemaEvolutionException]{
      ComplexTypeEvolution.schemaEvolution(srcRows.iterator, srcSchema, tgtSchema).toSeq
    }
  }
}
