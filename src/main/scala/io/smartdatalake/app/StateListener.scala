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

package io.smartdatalake.app

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionDAGRunState

case class StateListenerConfig(className: String, options: Option[Map[String,String]] = None) {
  // instantiate listener
  private val clazz = Class.forName(className)
  private val constructor = clazz.getConstructors.head
  private val constructor1 = clazz.getConstructor(classOf[Map[String,String]])
  private[smartdatalake] val listener: StateListener = constructor.newInstance(options.getOrElse(Map())).asInstanceOf[StateListener]
}

trait StateListener extends SmartDataLakeLogger {

  /**
   * Notify State
   */
  def notifyState(state: ActionDAGRunState): Unit
}
