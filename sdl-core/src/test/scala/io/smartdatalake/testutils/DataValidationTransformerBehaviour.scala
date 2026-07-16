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
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.action.generic.transformer.{DataValidationTransformer, RowLevelValidationRule}

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for [[DataValidationTransformer]], engine-agnostic so they can be instantiated against any
 * [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation (Spark, plain-Scala, ...).
 *
 * Note: not portable to ScalaSubFeed today: the transformer uses `array_construct_compact`, which is not
 * implemented for ScalaSubFeed. `subFeedTypeForValidation` must also be set explicitly, since it defaults to Spark.
 */
trait DataValidationTransformerBehaviour {

  def subFeedType: Type
  implicit def instanceRegistry: InstanceRegistry
  implicit def context: ActionPipelineContext

  def testRowLevelDataValidation(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    val functions = DataFrameSubFeed.getFunctions(subFeedType)
    import functions._
    import helper.implicits._

    val df = Seq(("jonson", "rob", Some(5)), ("doe", "bob", None)).toDF("lastname", "firstname", "rating")
    val validator = DataValidationTransformer(
      subFeedTypeForValidation = subFeedType.toString,
      rules = Seq(
        RowLevelValidationRule("rating is not null", Some("rating should not be empty")),
        RowLevelValidationRule("firstname != 'bob'", None)
      )
    )

    val dfValidated = validator.transform("testAction", Seq(), df, DataObjectId("testDO"), None, Map())

    val errors = dfValidated.filter(col("firstname") === lit("bob")).select(explode(col("errors"))).collect[String].toSet
    assert(errors == Set("rating should not be empty", "validation rule \"firstname != 'bob'\" failed!"))
  }
}
