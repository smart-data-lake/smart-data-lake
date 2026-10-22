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

import io.smartdatalake.testutils.spark.SparkTestUtil
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.typeOf

/**
 * A geo location enriched from an address.
 *
 * @param lat  Latitude in decimal degrees, WGS84.
 * @param lon  Longitude in decimal degrees, WGS84.
 * @param tags Free-form tags attached to the location.
 */
case class TestUdfGeo(lat: Double, lon: Double, tags: Seq[TestUdfTag])

/**
 * A tag attached to a geo location.
 *
 * @param key   The tag key.
 * @param value The tag value.
 */
case class TestUdfTag(key: String, value: String)

/**
 * A flat geo location.
 *
 * @param lat Latitude in decimal degrees, WGS84.
 * @param lon Longitude in decimal degrees, WGS84.
 */
case class TestUdfFlatGeo(lat: Double, lon: Double)

class SparkColumnCommentUtilTest extends AnyFunSuite {

  implicit val session: SparkSession = SparkTestUtil.session
  import session.implicits._

  private val geoUdf: UserDefinedFunction = udf((s: String) => TestUdfGeo(1.0, 2.0, Seq(TestUdfTag("a", "b"))))
  private val tagsUdf: UserDefinedFunction = udf((s: String) => Seq(TestUdfTag("a", "b")))

  private def cities: DataFrame = Seq("Bern", "Zurich").toDF("city")

  private def comments(df: DataFrame): Map[String, String] =
    SparkSchemaUtil.columnsComments(df.schema).map { case (path, comment) => path.mkString(".") -> comment }

  test("enrich comments of a struct column returned by a udf, including nested and array element fields") {
    val df = cities.select(col("city"), geoUdf(col("city")).as("geo"))
    assert(comments(df).isEmpty)

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromUdfs(df)

    assert(comments(enriched) == Map(
      "geo" -> "A geo location enriched from an address.",
      "geo.lat" -> "Latitude in decimal degrees, WGS84.",
      "geo.lon" -> "Longitude in decimal degrees, WGS84.",
      "geo.tags" -> "Free-form tags attached to the location.",
      "geo.tags.key" -> "The tag key.",
      "geo.tags.value" -> "The tag value."
    ))
    // data must be unchanged
    assert(enriched.count() == 2)
    assert(enriched.select("geo.lat").as[Double].collect().toSeq == Seq(1.0, 1.0))
  }

  test("enrich comments of an array-of-struct column returned by a udf") {
    val df = cities.select(col("city"), tagsUdf(col("city")).as("tags"))

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromUdfs(df)

    assert(comments(enriched) == Map(
      "tags" -> "A tag attached to a geo location.",
      "tags.key" -> "The tag key.",
      "tags.value" -> "The tag value."
    ))
  }

  test("enrich comment of a single attribute extracted from a udf result") {
    val df = cities.select(
      geoUdf(col("city")).getField("lat").as("latitude"),
      geoUdf(col("city")).getField("tags").as("theTags")
    )

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromUdfs(df)

    // the comment of the extracted attribute is used, under the new column name
    assert(comments(enriched) == Map(
      "latitude" -> "Latitude in decimal degrees, WGS84.",
      "theTags" -> "Free-form tags attached to the location.",
      "theTags.key" -> "The tag key.",
      "theTags.value" -> "The tag value."
    ))
  }

  test("resolve comments through renaming and intermediate projections") {
    val df = cities.select(col("city"), geoUdf(col("city")).as("geo"))
      .select(col("city"), col("geo"))
      .withColumnRenamed("geo", "location")
      .filter(col("city") === "Bern")

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromUdfs(df)

    assert(comments(enriched).get("location.lat").contains("Latitude in decimal degrees, WGS84."))
    assert(comments(enriched).get("location.tags.key").contains("The tag key."))
  }

  test("never overwrite an existing comment") {
    val df = cities.select(col("city"), geoUdf(col("city")).as("geo"))
    // set an explicit comment on the top level column and on a nested field
    val schemaWithComment = StructType(df.schema.fields.map {
      case f if f.name == "geo" =>
        val geoType = f.dataType.asInstanceOf[StructType]
        val newGeoType = StructType(geoType.fields.map {
          case n if n.name == "lat" => n.withComment("keep me")
          case n => n
        })
        f.copy(dataType = newGeoType).withComment("keep me too")
      case f => f
    })
    val dfWithComment = df.to(schemaWithComment)

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromUdfs(dfWithComment)

    assert(comments(enriched)("geo") == "keep me too")
    assert(comments(enriched)("geo.lat") == "keep me")
    // fields without an existing comment are still enriched
    assert(comments(enriched)("geo.lon") == "Longitude in decimal degrees, WGS84.")
  }

  test("do nothing for a udf created with an explicit return DataType") {
    // A UDF declaring its return type has no outputEncoder, so the case class can not be recovered.
    // Note that the untyped Scala API `udf(f, dataType)` is rejected by Spark, the Java API has to be used.
    val untypedUdf = udf(
      new UDF1[String, Row] { override def call(s: String): Row = Row(1.0, 2.0) },
      StructType(Seq(StructField("lat", DoubleType), StructField("lon", DoubleType)))
    )
    val df = cities.select(col("city"), untypedUdf(col("city")).as("geo"))

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromUdfs(df)

    assert(comments(enriched).isEmpty)
    assert(enriched.count() == 2)
  }

  test("do nothing for a udf returning a simple type") {
    val lengthUdf = udf((s: String) => s.length)
    val df = cities.select(col("city"), lengthUdf(col("city")).as("len"))

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromUdfs(df)

    assert(comments(enriched).isEmpty)
  }

  test("leave a DataFrame without any udf unchanged") {
    val df = cities.select(col("city"), col("city").as("city2"))

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromUdfs(df)

    assert(enriched.schema == df.schema)
    assert(enriched eq df) // early exit must not even create a projection
  }

  test("enrich comments from a case class given by the caller") {
    // df.as[T] leaves no trace in the plan, so the type has to be passed in explicitly
    val df = Seq((1.0, 2.0)).toDF("lat", "lon")
    assert(comments(df).isEmpty)

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromCaseClass(df, typeOf[TestUdfFlatGeo])

    assert(comments(enriched) == Map(
      "lat" -> "Latitude in decimal degrees, WGS84.",
      "lon" -> "Longitude in decimal degrees, WGS84."
    ))
    assert(enriched.head().getDouble(0) == 1.0)
  }

  test("enrich nested comments from a case class given by the caller") {
    val df = Seq(TestUdfGeo(1.0, 2.0, Seq(TestUdfTag("a", "b")))).toDF()

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromCaseClass(df, typeOf[TestUdfGeo])

    assert(comments(enriched) == Map(
      "lat" -> "Latitude in decimal degrees, WGS84.",
      "lon" -> "Longitude in decimal degrees, WGS84.",
      "tags" -> "Free-form tags attached to the location.",
      "tags.key" -> "The tag key.",
      "tags.value" -> "The tag value."
    ))
  }

  test("do nothing for a type that is not a case class") {
    val df = Seq("a").toDF("value")
    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromCaseClass(df, typeOf[String])
    assert(enriched eq df)
  }

  test("leave a DataFrame with duplicate column names unchanged") {
    // Dataframe.to(schema) resolves columns by name, so it must not be used in this case
    val df = cities.select(geoUdf(col("city")).as("geo"), geoUdf(col("city")).as("geo"))
    assert(df.schema.fieldNames.toSeq == Seq("geo", "geo"))

    val enriched = SparkColumnCommentUtil.enrichColumnCommentsFromUdfs(df)

    assert(enriched eq df)
  }
}
