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
package io.smartdatalake.workflow.dataframe.sql

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.ColumnLineageBehaviour
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.sql.SQLTestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.ColumnTransformation.Identity
import org.scalatest.Outcome
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Test the column level lineage extracted from an SQLDataFrame with SQLGlot.
 *
 * The engine independent cases are covered by [[ColumnLineageBehaviour]], which the other engines run as well.
 * The input DataFrames are created from values, only tests specific to the SQL engine read tables.
 */
class SQLColumnLineageTest extends AnyFunSuite with ColumnLineageBehaviour {

  override def subFeedType: Type = typeOf[SQLSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  instanceRegistry.register(SQLTestUtil.createEngineConnection("SQLColumnLineageTest"))
  implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  override def withFixture(test: NoArgTest): Outcome = {
    val reason = SQLTestUtil.pythonUnavailableReason
    assume(reason.isEmpty, reason.getOrElse(""))
    super.withFixture(test)
  }

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

  test("the debug switch records why a column could not be traced back") {
    testTheDebugSwitchRecordsWhyAColumnCouldNotBeTracedBack()
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

  test("the lineage of an SQL transformation is traced back to the input tables") {
    import SQLSubFeed._
    val schema = SQLSchema(Seq(SQLField("id", SQLSimpleDataType("INT")), SQLField("name", SQLSimpleDataType("TEXT"))))
    val src1 = table("db.a", schema)
    val src2 = table("db.b", schema)
    src1.createOrReplaceTempView("a")
    src2.createOrReplaceTempView("b")
    val df = SQLSubFeed.sql("select a.id, upper(b.name) as name, row_number() over (partition by a.name order by a.id) as rn, max(b.name) over (partition by a.id) as mx from a join b on a.id = b.id", DataObjectId("do1"))
    val lineage = extract(df, "src1" -> src1, "src2" -> src2)
    assert(inputsOf(lineage, "id") == Seq(("src1", "id", Identity)))
    assert(inputsOf(lineage, "name") == Seq(("src2", "name", "TRANSFORMATION")))
    // the partition and order columns of a window are INDIRECT lineage and not reported
    assert(lineage.get("rn").exists(_.inputFields.isEmpty))
    assert(inputsOf(lineage, "mx") == Seq(("src2", "name", "TRANSFORMATION")))
  }
}
