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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry, ParsableFromConfig}
import io.smartdatalake.definitions.{Condition, ExecutionMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.script.ParsableScriptDef
import io.smartdatalake.workflow.action.sparktransformer.FilterTransformer.extract
import io.smartdatalake.workflow.action.sparktransformer.{GenericDfsTransformerDef, FilterTransformer, GenericDfTransformer, PartitionValueTransformer}
import io.smartdatalake.workflow.dataobject.{CanReceiveScriptNotification, DataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ScriptSubFeed}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * [[Action]] execute script after multiple input DataObjects are ready, notifying multiple output DataObjects when script succeeded.
 *
 * @param inputIds               input DataObject's
 * @param outputIds              output DataObject's
 * @param scripts                definition of scripts to execute
 * @param executionCondition     optional spark sql expression evaluated against [[SubFeedsExpressionData]]. If true Action is executed, otherwise skipped. Details see [[Condition]].
 *                             If there are any rows passing the where clause, a MetricCheckFailed exception is thrown.
 */
case class CustomScriptAction(override val id: ActionId,
                              inputIds: Seq[DataObjectId],
                              outputIds: Seq[DataObjectId],
                              scripts: Seq[ParsableScriptDef] = Seq(),
                              override val executionCondition: Option[Condition] = None,
                              override val metadata: Option[ActionMetadata] = None
                       )(implicit instanceRegistry: InstanceRegistry) extends ScriptActionImpl {

  override val inputs: Seq[DataObject] = inputIds.map(getInputDataObject[DataObject])
  override val outputs: Seq[DataObject with CanReceiveScriptNotification] = outputIds.map(getOutputDataObject[DataObject with CanReceiveScriptNotification])

  validateConfig()

  override protected def execScript(inputSubFeeds: Seq[ScriptSubFeed], outputSubFeeds: Seq[ScriptSubFeed])(implicit context: ActionPipelineContext): Seq[ScriptSubFeed] = {
    val inputParameters = inputSubFeeds.flatMap(_.parameters).reduceLeftOption(_ ++ _).getOrElse(Map())
    val mainPartitionValues = getMainPartitionValues(inputSubFeeds)
    val outputParameters = scripts.foldLeft(inputParameters) {
      case (p, script) =>
        val stdOut = script.execStdOutString(id, mainPartitionValues, p)
        parseLastLine(stdOut)
    }
    outputSubFeeds.map(_.copy(parameters = Some(outputParameters)))
  }

  private def parseLastLine(stdOut: String): Map[String,String] = {
    val lastLine = stdOut.linesIterator.toIterable.lastOption
    lastLine.map(_.split(' ').map(_.split('=')).filter(_.length==2).map{case Array(k,v) => (k,v)}.toMap).getOrElse(Map())
  }

  override def factory: FromConfigFactory[Action] = CustomScriptAction

}

object CustomScriptAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomScriptAction = {
    extract[CustomScriptAction](config)
  }
}


