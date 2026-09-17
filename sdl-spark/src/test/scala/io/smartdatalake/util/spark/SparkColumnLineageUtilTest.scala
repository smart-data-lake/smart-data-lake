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
import io.smartdatalake.testutils.ColumnLineageBehaviour.withColumnLineageDebug
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.ColumnLineage
import io.smartdatalake.workflow.dataframe.ColumnTransformation.{Identity, Transformation}
import org.apache.spark.sql.expressions.Window
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

  test("the debug output names the plan node which created a column that could not be traced back") {
    withColumnLineageDebug {
      val src = sparkCities
      val other = Seq(("x", 1)).toDF("other", "cnt")
      val df = src.join(other, lit(true), "inner")
      val lineage = extract(df, "src1" -> src)
      val debug = lineage.debugInfo.get
      val deadEnds = debug.unresolvedColumns.flatMap(_.deadEnds)
      assert(deadEnds.nonEmpty)
      // the type of the plan node creating a column tells which extract logic is missing to trace it back
      assert(deadEnds.forall(_.producedBy.contains("LocalRelation")))
      assert(deadEnds.forall(_.producedByNode.exists(_.contains("LocalRelation"))))
      // the plan is part of the debug output, as it is where the lineage is read from
      assert(debug.plan.exists(_.contains("Join")))
    }
  }

  test("the debug output lists input columns which do not occur in the plan of the output DataFrame") {
    withColumnLineageDebug {
      val src1 = sparkCities
      val src2 = sparkCountries
      val other = Seq(("x", 1)).toDF("other", "cnt")
      // src2 is not read by the output DataFrame at all, while the columns of `other` are unknown
      val df = src1.join(other, lit(true), "inner")
      val lineage = extract(df, "src1" -> src1, "src2" -> src2)
      val debug = lineage.debugInfo.get
      assert(debug.inputs.map(_.dataObjectId.id) == Seq("src1", "src2"))
      assert(debug.inputs.head.columnsNotInPlan.isEmpty)
      assert(debug.inputs.last.columnsNotInPlan == Seq("code", "label", "population"))
    }
  }

  test("a column of a DataObject read twice is traced back although Spark replaced its column ids") {
    val src = sparkCities
    // Sparks analyzer gives the columns of the second occurrence of the same plan new expression ids
    val df = src.as("a").join(src.as("b"), $"a.country" === $"b.country")
      .select($"a.name", $"b.country".as("otherCountry"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
    assert(inputsOf(lineage, "otherCountry") == Seq(("src1", "country", Identity)))
    assert(lineage.unresolvedColumns.isEmpty)
  }

  test("a DataObject read twice in one SQL statement is traced back for both occurrences") {
    val src = sparkCities
    src.createOrReplaceTempView("lineage_cities_twice")
    val df = session.sql(
      "select c.name, b.lastCity from lineage_cities_twice c join " +
        "(select country, max(name) as lastCity from lineage_cities_twice group by country) b on c.country = b.country"
    )
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
    assert(inputsOf(lineage, "lastCity") == Seq(("src1", "name", Transformation)))
    assert(lineage.unresolvedColumns.isEmpty)
  }

  test("a window function does not depend on the columns it is partitioned and ordered by") {
    val src = sparkCountries
    val window = Window.partitionBy($"code").orderBy($"label")
    val df = src.select($"code", sum($"population").over(window).as("totalPopulation"))
    val lineage = extract(df, "src1" -> src)
    // the partition and order by columns influence the value without being part of it, which is INDIRECT lineage
    assert(inputsOf(lineage, "totalPopulation") == Seq(("src1", "population", Transformation)))
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
