/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.{DataValidationTransformer, RowLevelValidationRule}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DataValidationTransformerTest extends FunSuite {

  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  test("RowLevelDataValidation") {
    val df = Seq(("jonson","rob",Some(5)),("doe","bob",None)).toDF("lastname", "firstname", "rating")
    val validator = DataValidationTransformer(rules = Seq(
      RowLevelValidationRule("rating is not null", Some("rating should not be empty")),
      RowLevelValidationRule("firstname != 'bob'", Some("first should not be 'bob'"))
    ))
    val dfValidated = validator.transform("testAction", Seq(), SparkDataFrame(df), "testDO", None, Map()).asInstanceOf[SparkDataFrame]
    import SparkSubFeed._
    assert(dfValidated.filter(col("firstname") === lit("bob")).select(explode(col("errors"))).asInstanceOf[SparkDataFrame].inner.as[String].collect().toSet == Set("rating should not be empty", "first should not be 'bob'"))
  }

}
