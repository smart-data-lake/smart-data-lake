/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.spark.transformer

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.config.SdlConfigObject.ActionId
import org.apache.spark.sql.{SparkSession, Row}
import org.scalatest.FunSuite
import org.apache.spark.sql.types.{StructType, IntegerType, StringType, ArrayType}
import java.util.ArrayList



class SparkFlattenDFTransformerTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  test("Nested DataFrame returns a flat Dataframe") {
    val flattenDfTransformer = SparkFlattenDfTransformer()
    val nestedSchema: StructType = new StructType()
      .add("name", StringType, true)
      .add("info", new StructType()
        .add("age", IntegerType, true)
        .add("address", new StructType()
          .add("street", StringType)
          .add("number", StringType)
        ))

    //createDataFrame requires Java Lists
    val nestedData = new ArrayList[Row]()
    nestedData.add(Row("Michael", Row(30, Row("Main St","123"))))
    nestedData.add(Row("Bob", Row(25, Row("Elm St","456"))))

    val nested_df = session.createDataFrame(nestedData, nestedSchema)
    val new_df = flattenDfTransformer.transform(ActionId("ActionId"), Seq(), nested_df, DataObjectId("dataObjectId"))
    val expectedSchema = new StructType()
      .add("name", StringType, true)
      .add("info_age", IntegerType, true)
      .add("info_address_street", StringType)
      .add("info_address_number", StringType)
    assert(new_df.schema.equals(expectedSchema))
  }


  test("Flat schema remains unchanged") {
    val flattenDfTransformer = SparkFlattenDfTransformer()

    val normalSchema: StructType = new StructType()
      .add("name", StringType, true)
      .add("age", IntegerType, true)

    val unNestedData = new ArrayList[Row]()
    unNestedData.add(Row("Michael", 30))
    unNestedData.add(Row("Bob", 25))
    val unNested_df = session.createDataFrame(unNestedData, normalSchema)
    val new_df = flattenDfTransformer.transform(ActionId("ActionId"), Seq(), unNested_df, DataObjectId("dataObjectId"))
    assert(new_df.schema.equals(normalSchema))
  }


  test("Array explode works when enabled") {
    val flattenDfTransformer = SparkFlattenDfTransformer(enableExplode = true)

    val arraySchema: StructType = new StructType()
      .add("name", StringType, true)
      .add("hobbies", ArrayType(StringType), true)

    val arrayData = new ArrayList[Row]()
    arrayData.add(Row("Michael", Seq("football", "programming")))
    arrayData.add(Row("Bob", Seq("playing piano", "reading")))

    val array_df = session.createDataFrame(arrayData, arraySchema)
    val exploded_df = flattenDfTransformer.transform(ActionId("ActionId"), Seq(), array_df, DataObjectId("dataObjectId"))
    assert(exploded_df.count() == 4)
  }

  test("Array explode disabled by default") {
    val flattenDfTransformerNoExplode = SparkFlattenDfTransformer()

    val arraySchema: StructType = new StructType()
      .add("name", StringType, true)
      .add("hobbies", ArrayType(StringType), true)

    val arrayData = new ArrayList[Row]()
    arrayData.add(Row("Michael", Seq("football", "programming")))
    arrayData.add(Row("Bob", Seq("playing piano", "reading")))

    val array_df = session.createDataFrame(arrayData, arraySchema)
    val non_exploded_df = flattenDfTransformerNoExplode.transform(ActionId("ActionId2"), Seq(), array_df, DataObjectId("dataObjectId2"))
    assert(non_exploded_df.count() == 2)
  }



}
