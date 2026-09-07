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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.workflow.dataframe.ColumnTransformation.{Identity, Transformation}
import io.smartdatalake.workflow.dataframe.{ColumnLineage, ColumnTransformation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

case class TestLineageCity(name: String, country: String)

/** A geo location computed by a user defined function. */
case class TestLineageGeo(lat: Double, lon: Double)

class SparkColumnLineageUtilTest extends AnyFunSuite {

  implicit val session: SparkSession = SparkTestUtil.session

  import session.implicits._

  private def cities: DataFrame = Seq(("Bern", "CH"), ("Zurich", "CH")).toDF("name", "country")

  private def countries: DataFrame = Seq(("CH", "Switzerland", 9)).toDF("code", "label", "population")

  private def extract(df: DataFrame, inputs: (String, DataFrame)*): ColumnLineage = {
    SparkColumnLineageUtil.extractColumnLineage(df, inputs.map { case (id, df) => (DataObjectId(id), df) })
  }

  /**
   * Get the lineage of a column as tuples of input DataObject, input column and transformation subtype.
   */
  private def inputsOf(lineage: ColumnLineage, column: String): Seq[(String, String, String)] = {
    lineage.get(column).toSeq.flatMap(_.inputFields).map(f => (f.dataObjectId.id, f.column, f.transformation.subtype))
  }

  test("a column selected unchanged is an identity") {
    val src = cities
    val lineage = extract(src.select($"name"), "src1" -> src)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
  }

  test("a renamed column keeps the name of the input column") {
    val src = cities
    val lineage = extract(src.withColumnRenamed("name", "city"), "src1" -> src)
    assert(inputsOf(lineage, "city") == Seq(("src1", "name", Identity)))
    // the rename is applied over multiple projections, the lineage must still be an identity
    val lineage2 = extract(src.withColumnRenamed("name", "city").withColumnRenamed("city", "town"), "src1" -> src)
    assert(inputsOf(lineage2, "town") == Seq(("src1", "name", Identity)))
  }

  test("a calculated column depends on all columns of its expression") {
    val src = cities
    val df = src.withColumn("description", concat(upper($"name"), lit(" / "), $"country"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "description") == Seq(("src1", "country", Transformation), ("src1", "name", Transformation)))
    // the expression creating the column is exported as description of the transformation
    val description = lineage.get("description").get.inputFields.head.transformation.description
    assert(description.exists(_.contains("upper")))
  }

  test("a constant column is reported without input columns") {
    val src = cities
    val lineage = extract(src.withColumn("constant", lit("x")), "src1" -> src)
    // the column has no source, which is different from not being able to trace it back
    assert(lineage.get("constant").exists(_.inputFields.isEmpty))
    assert(lineage.get("constant").flatMap(_.expression).contains("'x'"))
    assert(lineage.unresolvedColumns.isEmpty)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
  }

  test("a column of a DataFrame which is not an input is reported as unresolved") {
    val src = cities
    val other = Seq(("x", 1)).toDF("other", "cnt")
    val df = src.crossJoin(other)
    val lineage = extract(df, "src1" -> src)
    // the columns of the unknown DataFrame can not be traced back, which must not look like having no source
    assert(lineage.unresolvedColumns == Seq("cnt", "other"))
    assert(lineage.get("other").isEmpty)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
  }

  test("a join keeps the lineage of the columns of both inputs") {
    val src1 = cities
    val src2 = countries
    val df = src1.join(src2, src1("country") === src2("code"))
      .select($"name", $"label".as("countryName"), ($"population" * 1000000).as("population"))
    val lineage = extract(df, "src1" -> src1, "src2" -> src2)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
    assert(inputsOf(lineage, "countryName") == Seq(("src2", "label", Identity)))
    assert(inputsOf(lineage, "population") == Seq(("src2", "population", Transformation)))
  }

  test("an aggregated column depends on the aggregated input column") {
    val src = cities
    val df = src.groupBy($"country").agg(count("*").as("cnt"), max($"name").as("lastCity"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "country") == Seq(("src1", "country", Identity)))
    assert(inputsOf(lineage, "lastCity") == Seq(("src1", "name", Transformation)))
    // count(*) does not read any column of the input, it depends on the input dataset as a whole
    assert(lineage.get("cnt").exists(_.inputFields.isEmpty))
    assert(lineage.get("cnt").flatMap(_.expression).exists(_.contains("count")))
    assert(lineage.unresolvedColumns.isEmpty)
  }

  test("a union keeps the lineage of the columns of all inputs") {
    val src1 = cities
    val src2 = Seq(("Paris", "FR")).toDF("name", "country")
    val df = src1.select($"name").union(src2.select(upper($"name").as("name")))
    val lineage = extract(df, "src1" -> src1, "src2" -> src2)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity), ("src2", "name", Transformation)))
  }

  test("a column created by explode depends on the exploded column") {
    val src = Seq(("CH", Seq("Bern", "Zurich"))).toDF("country", "cities")
    val df = src.select($"country", explode($"cities").as("city"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "city") == Seq(("src1", "cities", Transformation)))
  }

  test("a column created by a grouping set depends on the grouped input column") {
    val src = cities
    val df = src.cube($"country", $"name").agg(count("*").as("cnt")).select($"country", $"name")
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "country") == Seq(("src1", "country", Transformation)))
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Transformation)))
  }

  test("a column of a typed Dataset transformation depends on the columns it reads") {
    val src = cities
    val df = src.as[TestLineageCity].map(c => TestLineageCity(c.name.toUpperCase, c.country)).toDF()
    val lineage = extract(df, "src1" -> src)
    // the Scala function is opaque, so every output column depends on every column read by the transformation
    assert(inputsOf(lineage, "name") == Seq(("src1", "country", Transformation), ("src1", "name", Transformation)))
  }

  test("a column created by a user defined function depends on the columns it reads") {
    val src = cities
    val combineUdf = udf((city: String, country: String) => s"$city/$country")
    val df = src.select(combineUdf($"name", $"country").as("combined"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "combined") == Seq(("src1", "country", Transformation), ("src1", "name", Transformation)))
    // the description names the function, so it is visible that the column is created by a UDF
    assert(lineage.get("combined").get.inputFields.head.transformation.description.exists(_.contains("UDF")))
  }

  test("a column created from an attribute of a user defined function result is traced back") {
    val src = cities
    val geoUdf = udf((city: String) => TestLineageGeo(1.0, 2.0))
    val df = src.select(geoUdf($"name").getField("lat").as("lat"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "lat") == Seq(("src1", "name", Transformation)))
  }

  test("the lineage of a user defined function survives adding column comments from its ScalaDoc") {
    val src = cities
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
    val src = countries
    val df = src.withColumn("populationShare", $"population" / sum($"population").over())
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "populationShare") == Seq(("src1", "population", Transformation)))
  }

  test("a column created by a scalar subquery is traced back into the subquery") {
    val src1 = cities
    val src2 = countries
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
    val src1 = cities
    val src2 = countries
    src1.createOrReplaceTempView("lineage_cities")
    src2.createOrReplaceTempView("lineage_countries")
    val df = session.sql(
      "select name, (select max(c.label) from lineage_countries c where c.code = t.country) as countryName from lineage_cities t"
    )
    val lineage = extract(df, "src1" -> src1, "src2" -> src2)
    assert(inputsOf(lineage, "countryName") == Seq(("src2", "label", Transformation)))
    assert(lineage.unresolvedColumns.isEmpty)
  }

  test("no lineage is extracted without input DataFrames") {
    assert(SparkColumnLineageUtil.extractColumnLineage(cities, Seq()) == ColumnLineage.empty)
  }

  test("an input column used twice is reported once as transformation") {
    val src = cities
    val df = src.select(concat($"name", $"name").as("doubled"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "doubled") == Seq(("src1", "name", Transformation)))
  }

  test("the description of a transformation is cut off if it is too long") {
    val src = cities
    val longExpression = (1 to 50).foldLeft(upper($"name"))((c, _) => concat(c, $"country"))
    val df = src.select(longExpression.as("long"))
    val lineage = extract(df, "src1" -> src)
    val description = lineage.get("long").get.inputFields.head.transformation.description
    assert(description.exists(d => d.length <= 200 && d.endsWith("...")))
  }

  test("the transformation type is DIRECT for all detected lineage") {
    val src = cities
    val df = src.select($"name", upper($"country").as("country"))
    val lineage = extract(df, "src1" -> src)
    assert(lineage.fields.flatMap(_.inputFields).forall(_.transformation.tpe == ColumnTransformation.Direct))
    assert(lineage.fields.flatMap(_.inputFields).forall(!_.transformation.masking))
  }
}
