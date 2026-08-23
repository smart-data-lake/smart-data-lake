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
package io.smartdatalake.workflow.dataobject.expectations

import io.smartdatalake.config.{ConfigurationException, InstanceRegistry}
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject.expectation.{ExpectationScope, SQLExpectation}
import io.smartdatalake.workflow.dataobject.generic.Constraint
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test that a failed setup or evaluation of constraints and expectations tells that its origin is a
 * constraint or expectation definition, and which one, see issue #982.
 */
class ConstraintAndExpectationSetupErrorTest extends AnyFunSuite {

  protected implicit val session: SparkSession = SparkTestUtil.session

  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext

  private val df = SparkDataFrame(Seq(("jonson", "rob", 5), ("doe", "bob", 3)).toDF("lastname", "firstname", "rating"))

  test("failed setup of a constraint tells which constraint failed") {
    val dataObject = MockSparkDataObject("tgt1",
      constraints = Seq(Constraint("ratingNotNull", expression = "unknownColumn is not null"))
    )

    val ex = intercept[ConfigurationException](dataObject.setupConstraintsAndJobExpectations(df))
    assert(ex.getMessage.contains("Setting up constraint 'ratingNotNull' failed"))
    assert(ex.getConfigurationPath.contains("constraints.ratingNotNull"))
    assert(ex.getCause.getMessage.contains("unknownColumn"))
  }

  test("failed setup of a constraint tells which constraint failed if there are multiple constraints") {
    val dataObject = MockSparkDataObject("tgt2",
      constraints = Seq(
        Constraint("firstnameNotNull", expression = "firstname is not null"),
        Constraint("ratingNotNull", expression = "unknownColumn is not null")
      )
    )

    val ex = intercept[ConfigurationException](dataObject.setupConstraintsAndJobExpectations(df))
    assert(ex.getMessage.contains("Setting up constraint 'ratingNotNull' failed"))
    assert(ex.getConfigurationPath.contains("constraints.ratingNotNull"))
  }

  test("failed setup of an expectation with scope=Job tells which expectation failed") {
    val dataObject = MockSparkDataObject("tgt3",
      expectations = Seq(
        SQLExpectation("avgRating", aggExpression = "avg(rating)"),
        SQLExpectation("countUnknown", aggExpression = "count(unknownColumn)")
      )
    )

    // note that the default expectation 'count' is set up as well, so the failing expectation must be identified
    val ex = intercept[ConfigurationException](dataObject.setupConstraintsAndJobExpectations(df))
    assert(ex.getMessage.contains("Setting up expectation 'countUnknown' failed"))
    assert(ex.getConfigurationPath.contains("expectations.countUnknown"))
    assert(ex.getCause.getMessage.contains("unknownColumn"))
  }

  test("failed evaluation of an expectation with scope=All tells which expectation failed") {
    val expectations = Seq(
      SQLExpectation("avgRating", aggExpression = "avg(rating)", scope = ExpectationScope.All),
      SQLExpectation("countUnknown", aggExpression = "count(unknownColumn)", scope = ExpectationScope.All)
    )
    val dataObject = MockSparkDataObject("tgt4", expectations = expectations)

    val ex = intercept[ConfigurationException](dataObject.getScopeAllAggMetrics(df, expectations, Map()))
    assert(ex.getMessage.contains("Evaluating expectation 'countUnknown' failed"))
    assert(ex.getConfigurationPath.contains("expectations.countUnknown"))
  }
}
