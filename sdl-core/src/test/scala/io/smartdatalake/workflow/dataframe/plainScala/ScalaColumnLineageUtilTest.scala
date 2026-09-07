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
package io.smartdatalake.workflow.dataframe.plainScala

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.ColumnLineageBehaviour
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.ColumnTransformation.Transformation
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Test the column level lineage extracted from a ScalaDataFrame, see issue #867 and
 * [[ScalaColumnLineageUtil]].
 *
 * The engine independent cases are covered by [[ColumnLineageBehaviour]], which the Spark engine runs as
 * well. Only transformations specific to the plain-Scala engine are tested here.
 */
class ScalaColumnLineageUtilTest extends AnyFunSuite with ColumnLineageBehaviour {

  override def subFeedType: Type = typeOf[ScalaSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  import helper._

  test("a column selected unchanged is an identity") {
    testAColumnSelectedUnchangedIsAnIdentity()
  }

  test("a renamed column keeps the name of the input column") {
    testARenamedColumnKeepsTheNameOfTheInputColumn()
  }

  test("a calculated column depends on all columns of its expression") {
    testACalculatedColumnDependsOnAllColumnsOfItsExpression()
  }

  test("a constant column is reported without input columns") {
    testAConstantColumnIsReportedWithoutInputColumns()
  }

  test("a column of a DataFrame which is not an input is reported as unresolved") {
    testAColumnOfADataFrameWhichIsNotAnInputIsReportedAsUnresolved()
  }

  test("a join keeps the lineage of the columns of both inputs") {
    testAJoinKeepsTheLineageOfTheColumnsOfBothInputs()
  }

  test("an aggregated column depends on the aggregated input column") {
    testAnAggregatedColumnDependsOnTheAggregatedInputColumn()
  }

  test("a union keeps the lineage of the columns of all inputs") {
    testAUnionKeepsTheLineageOfTheColumnsOfAllInputs()
  }

  test("filtering and deduplicating a DataFrame keeps the lineage of its columns") {
    testFilteringAndDeduplicatingADataFrameKeepsTheLineageOfItsColumns()
  }

  test("aliasing a DataFrame keeps the lineage of its columns") {
    testAliasingADataFrameKeepsTheLineageOfItsColumns()
  }

  test("a cast column depends on the column it casts") {
    testACastColumnDependsOnTheColumnItCasts()
  }

  test("no lineage is extracted without input DataFrames") {
    testNoLineageIsExtractedWithoutInputDataFrames()
  }

  test("an input column used twice is reported once as transformation") {
    testAnInputColumnUsedTwiceIsReportedOnceAsTransformation()
  }

  test("the description of a transformation is cut off if it is too long") {
    testTheDescriptionOfATransformationIsCutOffIfItIsTooLong()
  }

  test("the transformation type is DIRECT for all detected lineage") {
    testTheTransformationTypeIsDirectForAllDetectedLineage()
  }

  test("a column created by explode depends on the exploded column") {
    val src = cities
    // array() resolves the data type of its arguments at construction time and therefore needs the columns
    // of the DataFrame, not unresolved column references
    val scalaSrc = src.asInstanceOf[ScalaDataFrame]
    val df = src.withColumn("tags", array(scalaSrc("name"), scalaSrc("country"))).withColumn("tag", explode(col("tags")))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "tag") == Seq(("src1", "country", Transformation), ("src1", "name", Transformation)))
  }

  test("a column defined by a when expression depends on the columns of all its branches") {
    val src = cities
    val scalaSrc = src.asInstanceOf[ScalaDataFrame]
    // otherwise() resolves the data type of the when expression and therefore needs resolved columns as well
    val df = src.withColumn("label", when(scalaSrc("country") === lit("CH"), scalaSrc("name")).otherwise(scalaSrc("country")))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "label") == Seq(("src1", "country", Transformation), ("src1", "name", Transformation)))
  }
}
