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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionDAGRunState

case class StateListenerConfig(className: String, options: Option[Map[String,String]] = None) {
  // instantiate listener
  private[smartdatalake] val listener: StateListener = try {
    val clazz = Class.forName(className)
    val constructor = clazz.getConstructor(classOf[Map[String,String]])
    constructor.newInstance(options.getOrElse(Map())).asInstanceOf[StateListener]
  } catch {
    case e: NoSuchMethodException => throw ConfigurationException(s"State listener class $className needs constructor with parameter Map[String,String]: ${e.getMessage}", Some("globalConfig.stateListeners"), e)
    case e: Exception => throw ConfigurationException(s"Cannot instantiate state listener class $className: ${e.getMessage}", Some("globalConfig.stateListeners"), e)
  }
}

trait StateListener extends SmartDataLakeLogger {

  /**
   * Notify State
   */
  def notifyState(state: ActionDAGRunState): Unit
}
