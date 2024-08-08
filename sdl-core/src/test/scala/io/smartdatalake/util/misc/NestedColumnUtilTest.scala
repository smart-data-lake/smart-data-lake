/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.misc

import io.smartdatalake.testutils.TestUtil.session
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema, SparkSimpleDataType, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import java.sql.Timestamp
import java.time.LocalDateTime

class NestedColumnUtilTest extends FunSuite {

  implicit val spark = session
  import spark.implicits._

  val currentTstmp = Timestamp.valueOf(LocalDateTime.now)
  val testSchema = new StructType()
    .add("a", new StructType()
      .add("tstmp", TimestampType)
      .add("str1", StringType)
    )
    .add("b", new ArrayType(
      new StructType()
        .add("long", LongType)
        .add("str2", StringType)
      , false
    ))
  private val testDf = spark.createDataFrame(Seq(((currentTstmp,"test1"), Seq((-1L, "test2")))).toDF.rdd, testSchema)

  test("select with reduced schema") {
    val selectSchema = new StructType()
      .add("a", new StructType()
        .add("tstmp", TimestampType)
      )
      .add("b", new ArrayType(
        new StructType()
          .add("str2", StringType)
        , false
      ))
    val dfTransformed = NestedColumnUtil.selectSchema(SparkDataFrame(testDf), SparkSchema(selectSchema))
    assert(dfTransformed.select(SparkSubFeed.expr("a.tstmp")).collect.head.get(0).asInstanceOf[Timestamp].getTime/1000 == currentTstmp.getTime/1000)
    assert(dfTransformed.schema.equalsSchema(SparkSchema(selectSchema)))
  }

  test("select with reduced schema and cast datatypes") {
    val selectSchema = new StructType()
      .add("a", new StructType()
        .add("tstmp", LongType)
      )
      .add("b", new ArrayType(
        new StructType()
          .add("str2", StringType)
        , false
      ))
    val dfTransformed = NestedColumnUtil.selectSchema(SparkDataFrame(testDf), SparkSchema(selectSchema))
    assert(dfTransformed.select(SparkSubFeed.expr("a.tstmp")).collect.head.get(0).asInstanceOf[Long] == currentTstmp.getTime/1000)
    assert(dfTransformed.schema.equalsSchema(SparkSchema(selectSchema)))
  }


  test("select with changed column order") {
    val selectSchema = new StructType()
      .add("b", new ArrayType(
        new StructType()
          .add("str2", StringType)
          .add("long", LongType)
        , false)
      )
      .add("a", new StructType()
        .add("str1", StringType)
        .add("tstmp", TimestampType)
      )
    val dfTransformed = NestedColumnUtil.selectSchema(SparkDataFrame(testDf), SparkSchema(selectSchema))
    assert(dfTransformed.select(SparkSubFeed.expr("b")).collect.head.getAs[Seq[Row]](0).head == Row("test2",-1))
    assert(dfTransformed.schema.equalsSchema(SparkSchema(selectSchema)))
  }

  test("transform nested column datatype") {
    val visitorFunc = (dataType: GenericDataType, column: GenericColumn, path: Seq[String]) => {
      dataType match {
        case SparkSimpleDataType(x: TimestampType) => TransformColumn(column.cast(SparkSimpleDataType(LongType)))
        case _ => KeepColumn
      }
    }
    val dfTransformed = NestedColumnUtil.transformColumns(SparkDataFrame(testDf), visitorFunc)
    assert(dfTransformed.select(SparkSubFeed.expr("a.tstmp")).collect.head.get(0).asInstanceOf[Long] == currentTstmp.getTime/1000)
  }

  test("remove nested column from array") {
    val visitorFunc = (dataType: GenericDataType, column: GenericColumn, path: Seq[String]) => {
      path match {
        case Seq("b","str2") => RemoveColumn
        case _ => KeepColumn
      }
    }
    val dfTransformed = NestedColumnUtil.transformColumns(SparkDataFrame(testDf), visitorFunc).asInstanceOf[SparkDataFrame]
    assert(dfTransformed.select(SparkSubFeed.expr("b")).collect.head.getAs[Seq[Row]](0).head.toSeq == Seq(-1L))
  }
}
