/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.config

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, DataObjectId}
import io.smartdatalake.definitions.ExecutionMode
import io.smartdatalake.workflow.action.{Action, ActionMetadata}
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, DataObject, TransactionalSparkTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SubFeed}
import org.apache.spark.sql.SparkSession

/**
 * A dummy [[Action]] for unit tests.
 *
 * @param id the unique identifier of this object.
 * @param inputId  input [[DataObject]] id
 * @param outputId output [[DataObject]] id
 * @param arg1  some (optional) dummy argument
 */
case class TestAction(override val id: ActionObjectId,
                      inputId: DataObjectId,
                      outputId: DataObjectId,
                      arg1: Option[String],
                      executionMode: Option[ExecutionMode] = None,
                      override val metricsFailCondition: Option[String] = None,
                      override val metadata: Option[ActionMetadata] = None
                     )(implicit instanceRegistry: InstanceRegistry)
  extends Action {

  override def init(subFeed: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = { /*NOP*/ Seq() }
  override def exec(subFeed: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed] = { /*NOP*/ Seq() }

  private[config] val input = instanceRegistry.get[DataObject with CanCreateDataFrame](inputId)
  private[config] val output = instanceRegistry.get[TransactionalSparkTableDataObject](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[TransactionalSparkTableDataObject] = Seq(output)

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[Action] = TestAction
}

object TestAction extends FromConfigFactory[Action] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): TestAction = {
    import configs.syntax.ConfigOps
    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[TestAction].value
  }
}