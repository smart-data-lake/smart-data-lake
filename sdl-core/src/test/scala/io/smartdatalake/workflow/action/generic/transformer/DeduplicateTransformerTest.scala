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

package io.smartdatalake.workflow.action.generic.transformer

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
class DeduplicateTransformerTest extends FunSuite{

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  test("deduplication test with primary key") {

    // prepare
    val deduplicateTransformer = DeduplicateTransformer(rankingExpression = "coalesce(updated_at, created_at)", primaryKeyColumns = Some(Seq("id")))

    val df = SparkDataFrame(Seq(
      (1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, "2019-04-25 12:23:29", "2019-05-26 13:37:09"),
      (2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("id", "created_at", "updated_at").select($"id", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType)))

    val resultDf = SparkDataFrame(Seq(
      (1, "2019-04-25 12:23:29", "2020-06-21 22:51:48"),
      (2, "2019-05-26 13:37:10", "2023-06-16 01:55:49"),
    ).toDF("id", "created_at", "updated_at").select($"id", $"created_at".cast(TimestampType), $"updated_at".cast(TimestampType)))

    // execute
    val transformedDf = deduplicateTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    // check
    assert(transformedDf.collect == resultDf.collect)
  }

}
