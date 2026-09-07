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
package io.smartdatalake.util.spark

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.ColumnLineageBehaviour
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.ColumnLineage
import io.smartdatalake.workflow.dataframe.ColumnTransformation.{Identity, Transformation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

case class TestLineageCity(name: String, country: String)

/** A geo location computed by a user defined function. */
case class TestLineageGeo(lat: Double, lon: Double)

/**
 * Test the column level lineage extracted from a Spark DataFrame, see issue #867 and
 * [[SparkColumnLineageUtil]].
 *
 * The engine independent cases are covered by [[ColumnLineageBehaviour]], which the plain-Scala engine runs
 * as well. Only transformations specific to the Spark engine are tested here, plus the exact format of the
 * description of a transformation, which is Sparks SQL representation of the expression.
 */
class SparkColumnLineageUtilTest extends AnyFunSuite with ColumnLineageBehaviour {

  override def subFeedType: Type = typeOf[io.smartdatalake.workflow.dataframe.spark.SparkSubFeed]
  implicit val session: SparkSession = SparkTestUtil.session
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext

  import session.implicits._

  private def sparkCities: DataFrame = Seq(("Bern", "CH"), ("Zurich", "CH")).toDF("name", "country")

  private def sparkCountries: DataFrame = Seq(("CH", "Switzerland", 9)).toDF("code", "label", "population")

  private def extract(df: DataFrame, inputs: (String, DataFrame)*): ColumnLineage = {
    SparkColumnLineageUtil.extractColumnLineage(df, inputs.map { case (id, df) => (DataObjectId(id), df) })
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

  test("the description of a transformation is the SQL representation of the expression") {
    val src = sparkCities
    val df = src.withColumn("description", concat(upper($"name"), lit(" / "), $"country"))
    val lineage = extract(df, "src1" -> src)
    val description = lineage.get("description").get.inputFields.head.transformation.description
    assert(description.contains("concat(upper(name), ' / ', country)"))
  }

  test("a column created by explode depends on the exploded column") {
    val src = Seq(("CH", Seq("Bern", "Zurich"))).toDF("country", "cities")
    val df = src.select($"country", explode($"cities").as("city"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "city") == Seq(("src1", "cities", Transformation)))
  }

  test("a column created by a grouping set depends on the grouped input column") {
    val src = sparkCities
    val df = src.cube($"country", $"name").agg(count("*").as("cnt")).select($"country", $"name")
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "country") == Seq(("src1", "country", Transformation)))
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Transformation)))
  }

  test("a column of a typed Dataset transformation depends on the columns it reads") {
    val src = sparkCities
    val df = src.as[TestLineageCity].map(c => TestLineageCity(c.name.toUpperCase, c.country)).toDF()
    val lineage = extract(df, "src1" -> src)
    // the Scala function is opaque, so every output column depends on every column read by the transformation
    assert(inputsOf(lineage, "name") == Seq(("src1", "country", Transformation), ("src1", "name", Transformation)))
  }

  test("a column created by a user defined function depends on the columns it reads") {
    val src = sparkCities
    val combineUdf = udf((city: String, country: String) => s"$city/$country")
    val df = src.select(combineUdf($"name", $"country").as("combined"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "combined") == Seq(("src1", "country", Transformation), ("src1", "name", Transformation)))
    // the description names the function, so it is visible that the column is created by a UDF
    assert(lineage.get("combined").get.inputFields.head.transformation.description.exists(_.contains("UDF")))
  }

  test("a column created from an attribute of a user defined function result is traced back") {
    val src = sparkCities
    val geoUdf = udf((city: String) => TestLineageGeo(1.0, 2.0))
    val df = src.select(geoUdf($"name").getField("lat").as("lat"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "lat") == Seq(("src1", "name", Transformation)))
  }

  test("the lineage of a user defined function survives adding column comments from its ScalaDoc") {
    val src = sparkCities
    val geoUdf = udf((city: String) => TestLineageGeo(1.0, 2.0))
    // enrichColumnCommentsFromUdfs rewrites the output attributes of the plan to add the column comments,
    // see issue #765. The lineage must still be found afterwards.
    val df = SparkColumnCommentUtil.enrichColumnCommentsFromUdfs(src.withColumn("geo", geoUdf($"name")))
    assert(SparkSchemaUtil.columnsComments(df.schema).nonEmpty, "no column comments were added, test is pointless")
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "geo") == Seq(("src1", "name", Transformation)))
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
    assert(lineage.unresolvedColumns.isEmpty)
  }

  test("a window function keeps the lineage of the column it is calculated on") {
    val src = sparkCountries
    val df = src.withColumn("populationShare", $"population" / sum($"population").over())
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "populationShare") == Seq(("src1", "population", Transformation)))
  }

  test("a column created by a scalar subquery is traced back into the subquery") {
    val src1 = sparkCities
    val src2 = sparkCountries
    src1.createOrReplaceTempView("lineage_cities")
    src2.createOrReplaceTempView("lineage_countries")
    // the plan of a subquery expression is not a child of the plan node holding it, so it needs its own traversal
    val df = session.sql(
      "select name, (select max(population) from lineage_countries) as maxPopulation from lineage_cities"
    )
    val lineage = extract(df, "src1" -> src1, "src2" -> src2)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
    assert(inputsOf(lineage, "maxPopulation") == Seq(("src2", "population", Transformation)))
    assert(lineage.unresolvedColumns.isEmpty)
  }

  test("a column created by a correlated scalar subquery is traced back into the subquery") {
    val src1 = sparkCities
    val src2 = sparkCountries
    src1.createOrReplaceTempView("lineage_cities")
    src2.createOrReplaceTempView("lineage_countries")
    val df = session.sql(
      "select name, (select max(c.label) from lineage_countries c where c.code = t.country) as countryName from lineage_cities t"
    )
    val lineage = extract(df, "src1" -> src1, "src2" -> src2)
    assert(inputsOf(lineage, "countryName") == Seq(("src2", "label", Transformation)))
    assert(lineage.unresolvedColumns.isEmpty)
  }
}
