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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.action.generic.transformer.ConvertNullValuesTransformer
import org.scalatest.Assertions

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for [[ConvertNullValuesTransformer]], engine-agnostic so they can be instantiated against any
 * [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation (Spark, plain-Scala, ...).
 *
 * Note: not portable to ScalaSubFeed today: the transformer uses `coalesce`, which is not implemented for ScalaSubFeed.
 */
trait ConvertNullValuesTransformerBehaviour extends Assertions {

  def subFeedType: Type
  implicit def instanceRegistry: InstanceRegistry
  implicit def context: ActionPipelineContext

  def testExclusiveIncludeOrExcludeColumns(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer(includeColumns = Seq("column1"), excludeColumns = Seq("column2"))
    val df = Seq((1, 1), (2, 2)).toDF("column1", "column2")

    val thrown = intercept[IllegalArgumentException] {
      convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())
    }

    assert(thrown.isInstanceOf[IllegalArgumentException])
    assert(thrown.getMessage == "requirement failed: Conflicting parameters. Please use either includeColumns or excludeColumns, as simultaneous application is not supported.")
  }

  def testDefaultValues(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer()
    val initSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Option.empty[Double]), (Option.empty[String], Option.empty[Int], Option.empty[Double]))
    val resultSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Some(-1), Some(3.0)), (Some("na"), Some(2), Some(-1.0)), (Some("na"), Some(-1), Some(-1.0)))
    val df = initSeq.toDF("column1", "column2", "column3")
    val resultDf = resultSeq.toDF("column1", "column2", "column3")

    val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testIncludeColumnsSet(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer(includeColumns = Seq("column1", "column2"))
    val initSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Option.empty[Double]), (Option.empty[String], Option.empty[Int], Option.empty[Double]))
    val resultSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Some(-1), Some(3.0)), (Some("na"), Some(2), Option.empty[Double]), (Some("na"), Some(-1), Option.empty[Double]))
    val df = initSeq.toDF("column1", "column2", "column3")
    val resultDf = resultSeq.toDF("column1", "column2", "column3")

    val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testExcludeColumnsSet(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer(excludeColumns = Seq("column1", "column2"))
    val initSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Option.empty[Double]), (Option.empty[String], Option.empty[Int], Option.empty[Double]))
    val resultSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Some(-1.0)), (Option.empty[String], Option.empty[Int], Some(-1.0)))
    val df = initSeq.toDF("column1", "column2", "column3")
    val resultDf = resultSeq.toDF("column1", "column2", "column3")

    val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testCustomStringValueCheck(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer(valueForString = "n/a")
    val initSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Option.empty[Double]), (Option.empty[String], Option.empty[Int], Option.empty[Double]))
    val resultSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Some(-1), Some(3.0)), (Some("n/a"), Some(2), Some(-1.0)), (Some("n/a"), Some(-1), Some(-1.0)))
    val df = initSeq.toDF("column1", "column2", "column3")
    val resultDf = resultSeq.toDF("column1", "column2", "column3")

    val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testCustomNumberValueCheck(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer(valueForNumber = -7)
    val initSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Option.empty[Double]), (Option.empty[String], Option.empty[Int], Option.empty[Double]))
    val resultSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Some(-7), Some(3.0)), (Some("na"), Some(2), Some(-7.0)), (Some("na"), Some(-7), Some(-7.0)))
    val df = initSeq.toDF("column1", "column2", "column3")
    val resultDf = resultSeq.toDF("column1", "column2", "column3")

    val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testIgnoreOtherThanStringOrNumberTypesColumns(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer()
    val initSeq: Seq[(Option[String], Option[Int], Option[Double], Option[Float], Option[Boolean])] = Seq((Some("1"), Option.empty[Int], Some(3.0), Option.empty[Float], Option.empty[Boolean]), (Option.empty[String], Some(2), Option.empty[Double], Option.empty[Float], Option.empty[Boolean]), (Option.empty[String], Option.empty[Int], Option.empty[Double], Some(9.0f), Some(false)))
    val resultSeq: Seq[(Option[String], Option[Int], Option[Double], Option[Float], Option[Boolean])] = Seq((Some("1"), Some(-1), Some(3.0), Some(-1.0f), Option.empty[Boolean]), (Some("na"), Some(2), Some(-1.0), Some(-1.0f), Option.empty[Boolean]), (Some("na"), Some(-1), Some(-1.0), Some(9.0f), Some(false)))
    val df = initSeq.toDF("column1", "column2", "column3", "column4", "column5")
    val resultDf = resultSeq.toDF("column1", "column2", "column3", "column4", "column5")

    val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testNoErrorForExistingIncludeColumnsCaseInsensitive(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer(includeColumns = Seq("coluMN1", "colUMn2", "COLUMn3"))
    val initSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Option.empty[Double]), (Option.empty[String], Option.empty[Int], Option.empty[Double]))
    val resultSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Some(-1), Some(3.0)), (Some("na"), Some(2), Some(-1.0)), (Some("na"), Some(-1), Some(-1.0)))
    val df = initSeq.toDF("column1", "column2", "column3")
    val resultDf = resultSeq.toDF("column1", "column2", "column3")

    val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testErrorForNonExistingIncludeColumnsCaseInsensitive(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer(includeColumns = Seq("coluMN1", "colUMn2", "column3")) // column3 does not exists
    val df = Seq((1, 1), (2, 2)).toDF("column1", "column2")

    val thrown = intercept[IllegalArgumentException] {
      convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())
    }

    assert(thrown.isInstanceOf[IllegalArgumentException])
  }

  def testNoErrorForExistingIncludeColumnsCaseSensitive(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val previousCaseSensitive = Environment._caseSensitive
    Environment._caseSensitive = Some(true)
    try {
      val convertNullValuesTransformer = ConvertNullValuesTransformer(includeColumns = Seq("coluMN1", "colUMn2", "COLUMn3"))
      val initSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Option.empty[Double]), (Option.empty[String], Option.empty[Int], Option.empty[Double]))
      val resultSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Some(-1), Some(3.0)), (Some("na"), Some(2), Some(-1.0)), (Some("na"), Some(-1), Some(-1.0)))
      val df = initSeq.toDF("coluMN1", "colUMn2", "COLUMn3")
      val resultDf = resultSeq.toDF("coluMN1", "colUMn2", "COLUMn3")

      val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

      assert(resultDf.isEqual(transformedDf))
    } finally {
      Environment._caseSensitive = previousCaseSensitive
    }
  }

  def testErrorForNonExistingIncludeColumnsCaseSensitive(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val previousCaseSensitive = Environment._caseSensitive
    Environment._caseSensitive = Some(true)
    try {
      val convertNullValuesTransformer = ConvertNullValuesTransformer(includeColumns = Seq("coluMN1", "column2"))
      val df = Seq((1, 1), (2, 2)).toDF("column1", "column2")

      val thrown = intercept[IllegalArgumentException] {
        convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())
      }

      assert(thrown.isInstanceOf[IllegalArgumentException])
    } finally {
      Environment._caseSensitive = previousCaseSensitive
    }
  }

  def testNoErrorForExistingExcludeColumnsCaseInsensitive(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer(excludeColumns = Seq("coluMN1", "colUMn2"))
    val initSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Option.empty[Double]), (Option.empty[String], Option.empty[Int], Option.empty[Double]))
    val resultSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Some(-1.0)), (Option.empty[String], Option.empty[Int], Some(-1.0)))
    val df = initSeq.toDF("column1", "column2", "column3")
    val resultDf = resultSeq.toDF("column1", "column2", "column3")

    val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(resultDf.isEqual(transformedDf))
  }

  def testErrorForNonExistingExcludeColumnsCaseInsensitive(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val convertNullValuesTransformer = ConvertNullValuesTransformer(excludeColumns = Seq("coluMN1", "colUMn2", "column3")) // column3 does not exists
    val df = Seq((1, 1), (2, 2)).toDF("column1", "column2")

    val thrown = intercept[IllegalArgumentException] {
      convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())
    }

    assert(thrown.isInstanceOf[IllegalArgumentException])
  }

  def testNoErrorForExistingExcludeColumnsCaseSensitive(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val previousCaseSensitive = Environment._caseSensitive
    Environment._caseSensitive = Some(true)
    try {
      val convertNullValuesTransformer = ConvertNullValuesTransformer(excludeColumns = Seq("colUMN1", "coLUmn2"))
      val initSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Option.empty[Double]), (Option.empty[String], Option.empty[Int], Option.empty[Double]))
      val resultSeq: Seq[(Option[String], Option[Int], Option[Double])] = Seq((Some("1"), Option.empty[Int], Some(3.0)), (Option.empty[String], Some(2), Some(-1.0)), (Option.empty[String], Option.empty[Int], Some(-1.0)))
      val df = initSeq.toDF("colUMN1", "coLUmn2", "column3")
      val resultDf = resultSeq.toDF("colUMN1", "coLUmn2", "column3")

      val transformedDf = convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

      assert(resultDf.isEqual(transformedDf))
    } finally {
      Environment._caseSensitive = previousCaseSensitive
    }
  }

  def testErrorForNonExistingExcludeColumnsCaseSensitive(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val previousCaseSensitive = Environment._caseSensitive
    Environment._caseSensitive = Some(true)
    try {
      val convertNullValuesTransformer = ConvertNullValuesTransformer(excludeColumns = Seq("coluMN1", "column2"))
      val df = Seq((1, 1), (2, 2)).toDF("column1", "column2")

      val thrown = intercept[IllegalArgumentException] {
        convertNullValuesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())
      }

      assert(thrown.isInstanceOf[IllegalArgumentException])
    } finally {
      Environment._caseSensitive = previousCaseSensitive
    }
  }
}
