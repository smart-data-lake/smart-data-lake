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

package io.smartdatalake.dataframe

import io.smartdatalake.dataframe.DomainSpecificLanguage.Language
import io.smartdatalake.testutils.DataFrameTestHelper
import io.smartdatalake.testutils.TestUtil.sparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class SparkLanguageImplementationTest extends FlatSpec with Matchers {

  implicit val session: SparkSession = sparkSessionBuilder().getOrCreate
  implicit val language: Language[SparkDataFrame, SparkColumn, SparkStructType, SparkDataType] = SparkLanguageImplementation.language

  "SparkLanguageImplementation" must "return the correct schema" in {
    // Arrange
    val dfNameAndAge: SparkDataFrame = DataFrameTestHelper.createDf(Map(
      "Name" -> "Hans",
      "Age" -> 3
    ), Map(
      "Name" -> "Fritz",
      "Age" -> 5
    ))

    // Act
    val result = language.schema(dfNameAndAge)

    // Assert
    assert(result.fields.map(field => field.name).sameElements(Array("Name", "Age")))
  }

  "SparkLanguageImplementation" must "be able to wrap DataFrames, perform DSL operations on them, and unwrap them" in {
    // Arrange
    val dfNameAndAge: SparkDataFrame = DataFrameTestHelper.createDf(Map(
      "Name" -> "Hans",
      "Age" -> 3
    ), Map(
      "Name" -> "Fritz",
      "Age" -> 5
    ))

    val dfNameAndFavoriteColor: SparkDataFrame = DataFrameTestHelper.createDf(Map(
      "Name" -> "Hans",
      "FavoriteColor" -> "Blue"
    ), Map(
      "Name" -> "Fritz",
      "FavoriteColor" -> "Red"
    ))

    val dfExpected: SparkDataFrame = DataFrameTestHelper.createDf(Map(
      "FavoriteColor" -> "Blue"
    ))

    // This transformation works purely in domain-specific language
    // It does not know which execution engine (e.g. Spark) it will run on
    def businessLogic[DataFrame, Column, Schema, DataType](df1: DataFrame,
                                                 df2: DataFrame)
                                                (implicit language: Language[DataFrame, Column, Schema, DataType]): DataFrame = {
      import language._
      val dfJoined = df1.join(df2, Seq("Name"))
      val dfSelected = dfJoined.select(col("FavoriteColor"))
      val dfFiltered = dfSelected.filter(language.===(col("Name"), lit("Hans")))
      dfFiltered
    }

    // Act
    val dfResult = businessLogic(dfNameAndAge, dfNameAndFavoriteColor)

    // Assert
    DataFrameTestHelper.assertDataFramesEqual(dfResult, dfExpected)
  }
}
