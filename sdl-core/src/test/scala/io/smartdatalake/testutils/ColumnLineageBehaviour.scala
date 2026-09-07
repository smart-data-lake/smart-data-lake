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
package io.smartdatalake.testutils

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.dataframe.ColumnTransformation.{Direct, Identity, Transformation}
import io.smartdatalake.workflow.dataframe.{ColumnLineage, GenericDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion}

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for the column level lineage of a DataFrame, see issue #867 and
 * [[io.smartdatalake.workflow.dataframe.ColumnLineage]].
 *
 * These tests are engine-agnostic so they can be instantiated against any
 * [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation. Every engine analyzing column
 * lineage has to produce the same lineage for the same logical transformation - that contract is what these
 * tests pin down, and it is what the documentation promises. They go through
 * [[GenericDataFrame.getColumnLineage]] instead of an engine's lineage utility for the same reason.
 *
 * Transformations which only one engine can express, e.g. a Spark window function or a typed Dataset
 * transformation, are tested in the engine specific suites. The same holds for the exact format of the
 * `description` of a transformation, which is the engine's own way of writing an expression.
 */
trait ColumnLineageBehaviour {

  def subFeedType: Type
  implicit def context: ActionPipelineContext

  protected lazy val helper: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(subFeedType)

  import helper._
  import helper.implicits._

  protected def cities: GenericDataFrame =
    Seq(("Bern", "CH"), ("Zurich", "CH"), ("Paris", "FR")).toDF("name", "country")

  protected def countries: GenericDataFrame =
    Seq(("CH", "Switzerland", 9)).toDF("code", "label", "population")

  protected def extract(df: GenericDataFrame, inputs: (String, GenericDataFrame)*): ColumnLineage = {
    df.getColumnLineage(inputs.map { case (id, input) => (DataObjectId(id), input) })
      .getOrElse(throw new IllegalStateException(s"Column lineage is not implemented for ${df.subFeedType.typeSymbol.name}"))
  }

  /**
   * Get the lineage of a column as tuples of input DataObject, input column and transformation subtype.
   */
  protected def inputsOf(lineage: ColumnLineage, column: String): Seq[(String, String, String)] = {
    lineage.get(column).toSeq.flatMap(_.inputFields).map(f => (f.dataObjectId.id, f.column, f.transformation.subtype))
  }

  def testAColumnSelectedUnchangedIsAnIdentity(): Unit = {
    val src = cities
    val lineage = extract(src.select(Seq(col("name"))), "src1" -> src)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
  }

  def testARenamedColumnKeepsTheNameOfTheInputColumn(): Unit = {
    val src = cities
    val lineage = extract(src.withColumnRenamed("name", "city"), "src1" -> src)
    assert(inputsOf(lineage, "city") == Seq(("src1", "name", Identity)))
    // the rename is applied over multiple operations, the lineage must still be an identity
    val lineage2 = extract(src.withColumnRenamed("name", "city").withColumnRenamed("city", "town"), "src1" -> src)
    assert(inputsOf(lineage2, "town") == Seq(("src1", "name", Identity)))
  }

  def testACalculatedColumnDependsOnAllColumnsOfItsExpression(): Unit = {
    val src = cities
    val df = src.withColumn("description", concat(col("name"), lit(" / "), col("country")))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "description") == Seq(("src1", "country", Transformation), ("src1", "name", Transformation)))
    // the expression creating the column is exported as description of the transformation
    val description = lineage.get("description").get.inputFields.head.transformation.description
    assert(description.exists(_.contains("concat")))
  }

  def testAConstantColumnIsReportedWithoutInputColumns(): Unit = {
    val src = cities
    val lineage = extract(src.withColumn("constant", lit("x")), "src1" -> src)
    // the column has no source, which is different from not being able to trace it back
    assert(lineage.get("constant").exists(_.inputFields.isEmpty))
    assert(lineage.get("constant").flatMap(_.expression).exists(_.contains("x")))
    assert(lineage.unresolvedColumns.isEmpty)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
  }

  def testAColumnOfADataFrameWhichIsNotAnInputIsReportedAsUnresolved(): Unit = {
    val src = cities
    val other = Seq(("x", 1)).toDF("other", "cnt")
    val df = src.join(other, lit(true), "inner")
    val lineage = extract(df, "src1" -> src)
    // the columns of the unknown DataFrame can not be traced back, which must not look like having no source
    assert(lineage.unresolvedColumns == Seq("cnt", "other"))
    assert(lineage.get("other").isEmpty)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
  }

  def testAJoinKeepsTheLineageOfTheColumnsOfBothInputs(): Unit = {
    val src1 = cities
    val src2 = countries
    val df = src1.as("c").join(src2.as("n"), col("c.country") === col("n.code"), "inner")
      .select(Seq(col("c.name"), col("n.label").as("countryName"), (col("n.population") * lit(1000000)).as("population")))
    val lineage = extract(df, "src1" -> src1, "src2" -> src2)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
    assert(inputsOf(lineage, "countryName") == Seq(("src2", "label", Identity)))
    assert(inputsOf(lineage, "population") == Seq(("src2", "population", Transformation)))
  }

  def testAnAggregatedColumnDependsOnTheAggregatedInputColumn(): Unit = {
    val src = cities
    val df = src.groupBy(Seq(col("country"))).agg(Seq(count(col("*")).as("cnt"), max(col("name")).as("lastCity")))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "country") == Seq(("src1", "country", Identity)))
    assert(inputsOf(lineage, "lastCity") == Seq(("src1", "name", Transformation)))
    // count(*) does not read any column of the input, it depends on the input dataset as a whole
    assert(lineage.get("cnt").exists(_.inputFields.isEmpty))
    assert(lineage.get("cnt").flatMap(_.expression).exists(_.contains("count")))
    assert(lineage.unresolvedColumns.isEmpty)
  }

  def testAUnionKeepsTheLineageOfTheColumnsOfAllInputs(): Unit = {
    val src1 = cities
    val src2 = Seq(("Paris", "FR")).toDF("name", "country")
    // the column of one input is taken over unchanged, the one of the other input is modified
    val df = src1.select(Seq(col("name"))).unionByName(src2.select(Seq(concat(col("name"), lit("!")).as("name"))))
    val lineage = extract(df, "src1" -> src1, "src2" -> src2)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity), ("src2", "name", Transformation)))
  }

  def testFilteringAndDeduplicatingADataFrameKeepsTheLineageOfItsColumns(): Unit = {
    val src = cities
    val df = src.filter(col("country") === lit("CH")).distinct.dropDuplicates(Seq("name"))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
    assert(inputsOf(lineage, "country") == Seq(("src1", "country", Identity)))
  }

  def testAliasingADataFrameKeepsTheLineageOfItsColumns(): Unit = {
    val src = cities
    val lineage = extract(src.as("c"), "src1" -> src)
    assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
  }

  def testACastColumnDependsOnTheColumnItCasts(): Unit = {
    val src = countries
    val lineage = extract(src.withColumn("population", col("population").cast(stringType)), "src1" -> src)
    assert(inputsOf(lineage, "population") == Seq(("src1", "population", Transformation)))
    val description = lineage.get("population").get.inputFields.head.transformation.description
    assert(description.exists(_.toLowerCase.contains("cast")))
  }

  def testNoLineageIsExtractedWithoutInputDataFrames(): Unit = {
    assert(extract(cities) == ColumnLineage.empty)
  }

  def testAnInputColumnUsedTwiceIsReportedOnceAsTransformation(): Unit = {
    val src = cities
    val df = src.select(Seq(concat(col("name"), col("name")).as("doubled")))
    val lineage = extract(df, "src1" -> src)
    assert(inputsOf(lineage, "doubled") == Seq(("src1", "name", Transformation)))
  }

  def testTheDescriptionOfATransformationIsCutOffIfItIsTooLong(): Unit = {
    val src = cities
    val df = src.withColumn("long", concat(col("name"), lit("x" * 250)))
    val lineage = extract(df, "src1" -> src)
    val description = lineage.get("long").get.inputFields.head.transformation.description
    assert(description.exists(d => d.length <= 200 && d.endsWith("...")))
  }

  def testTheTransformationTypeIsDirectForAllDetectedLineage(): Unit = {
    val src = cities
    val df = src.select(Seq(col("name"), concat(col("country"), lit("!")).as("country")))
    val lineage = extract(df, "src1" -> src)
    // the columns of a join, filter, group by or sort condition influence the result without being part of
    // its value, which is INDIRECT lineage in OpenLineage and not detected yet
    assert(lineage.fields.flatMap(_.inputFields).forall(_.transformation.tpe == Direct))
    assert(lineage.fields.flatMap(_.inputFields).forall(!_.transformation.masking))
  }
}
