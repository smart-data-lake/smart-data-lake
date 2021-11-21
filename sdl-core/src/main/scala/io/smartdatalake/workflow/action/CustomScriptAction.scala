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
package io.smartdatalake.workflow.action

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{Condition, ExecutionMode, SparkStreamingMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.customlogic.CustomDfsTransformerConfig
import io.smartdatalake.workflow.action.sparktransformer.{ParsableDfsTransformer, SQLDfsTransformer}
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanReceiveScriptNotification, CanWriteDataFrame, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.SparkSession

/**
 * [[Action]] execute script after multiple input DataObjects are ready, notifying multiple output DataObjects when script succeeded.
 *
 * @param inputIds               input DataObject's
 * @param outputIds              output DataObject's
 * //TODO @param scripts            scripts to apply...
 * @param mainInputId            optional selection of main inputId used for execution mode and partition values propagation. Only needed if there are multiple input DataObject's.
 * @param mainOutputId           optional selection of main outputId used for execution mode and partition values propagation. Only needed if there are multiple output DataObject's.
 * @param executionMode          optional execution mode for this Action
 * @param executionCondition     optional spark sql expression evaluated against [[SubFeedsExpressionData]]. If true Action is executed, otherwise skipped. Details see [[Condition]].
 * @param metricsFailCondition optional spark sql expression evaluated as where-clause against dataframe of metrics. Available columns are dataObjectId, key, value.
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 */
case class CustomScriptAction(override val id: ActionId,
                              inputIds: Seq[DataObjectId],
                              outputIds: Seq[DataObjectId],
                              override val mainInputId: Option[DataObjectId] = None,
                              override val mainOutputId: Option[DataObjectId] = None,
                              override val executionMode: Option[ExecutionMode] = None,
                              override val executionCondition: Option[Condition] = None,
                              override val metricsFailCondition: Option[String] = None,
                              override val metadata: Option[ActionMetadata] = None
                       )(implicit instanceRegistry: InstanceRegistry) extends ScriptSubFeedsAction {

  // checks
  override val inputs: Seq[DataObject] = inputIds.map(getInputDataObject[DataObject])
  override val outputs: Seq[DataObject with CanReceiveScriptNotification] = outputIds.map(getOutputDataObject[DataObject with CanReceiveScriptNotification])

  override def factory: FromConfigFactory[Action] = CustomScriptAction
}

object CustomScriptAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomScriptAction = {
    extract[CustomScriptAction](config)
  }
}


