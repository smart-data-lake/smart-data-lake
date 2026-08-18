/*
 * Smart Data Lake - Build your data lake the smart way.
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

package io.smartdatalake.util.historization

import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}

import java.sql.Timestamp


/**
 * Strategy how to historize, e.g. how to create the records to insert and update in the output DataObject.
 * The strategy is resolved from the configuration and the input schema, see HistorizeAction.resolveMode.
 */
trait HistorizeMode extends SmartDataLakeLogger {

  /** name of the transformer implementing this strategy */
  def transformerName: String

  /** id of the action */
  def id: ActionId

  /**
   * Optional column holding the timestamp of the last change of the record in the source system.
   * If defined, the validity of a new version starts at this timestamp instead of the runs reference timestamp.
   */
  def sourceTimestampColName: Option[String]

  /** input columns which are needed for historization, but must not be written to the output DataObject */
  def excludedColNames: Seq[String]

  /** create the records to insert and update in the output DataObject */
  def historize(existingDf: Option[GenericDataFrame], newDf: GenericDataFrame, pks: Seq[String], refTimestamp: Timestamp)
               (implicit context: ActionPipelineContext): GenericDataFrame

  /** options for the merge statement writing the result of [[historize]] */
  def saveModeOptions(schema: Option[GenericSchema])(implicit context: ActionPipelineContext): SaveModeOptions

  /** log the resolved strategy, called once the input schema is known */
  def logInfo(): Unit =
    sourceTimestampColName.foreach(c => logger.info(s"($id) validity of new versions starts at column $c"))

  protected def operationCondition(operation: String): String =
    s"${Historization.historizeOperationColName} = '$operation'"

  protected def additionalMergePredicate(predicate: String, predicates: Seq[String]): Option[String] =
    Some((predicate +: predicates).reduce(_ + " and " + _))
}
