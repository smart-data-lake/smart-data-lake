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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.workflow.ActionPipelineContext

/**
 * DataObjects should implement this interface to allow incremental processing
 */
private[smartdatalake] trait CanCreateIncrementalOutput {

  /**
   * To implement incremental processing this function is called to initialize the DataObject with its state from the last increment.
   * The state is just a string. It's semantics is internal to the DataObject.
   * Note that this method is called on initializiation of the SmartDataLakeBuilder job (init Phase) and for streaming execution after every execution of an Action involving this DataObject (postExec).
   * @param state Internal state of last increment. If None then the first increment (may be a full increment) is delivered.
   */
  def setState(state: Option[String])(implicit context: ActionPipelineContext) : Unit

  /**
   * Return the state of the last increment or empty if no increment was processed.
   * After the state is returned to SDLB by getState, it should be reset in the DataObject to avoid side-effects (e.g. calling getSparkDataFrame from a unit test).
   */
  def getState: Option[String]

}
