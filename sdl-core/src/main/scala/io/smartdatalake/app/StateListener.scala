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
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.secrets.StringOrSecret

/**
 * Configuration to notify interested parties about action results & metric
 *
 * @param className fully qualified class name of class implementing StateListener interface. The class needs a constructor with one parameter `options: Map[String,String]`.
 * @param options Options are passed to StateListener constructor.
 */
case class StateListenerConfig(className: String, options: Option[Map[String,StringOrSecret]] = None) {
  // instantiate listener
  private[smartdatalake] val listener: StateListener = try {
    val clazz = Environment.classLoader.loadClass(className)
    val constructor = clazz.getConstructor(classOf[Map[String,String]])
    constructor.newInstance(options.getOrElse(Map())).asInstanceOf[StateListener]
  } catch {
    case e: NoSuchMethodException => throw ConfigurationException(s"State listener class $className needs constructor with parameter Map[String,String]: ${e.getMessage}", Some("globalConfig.stateListeners"), e)
    case e: Exception => throw ConfigurationException(s"Cannot instantiate state listener class $className: ${e.getMessage}", Some("globalConfig.stateListeners"), e)
  }
}

/**
 * Interface to notify interested parties about action results & metric
 */
trait StateListener {

  /**
   * Called on initialization to check environment
   */
  def init(): Unit = Unit

  /**
   * notifyState is called whenever an action is finished (succeeded or failed) and at the end of the DAG execution (success or failure).
   * It always includes the state of all actions of the DAG.
   * At the end of the DAG execution notifyState is called with the final state. In this case state.isFinal = true and changedActionId is empty.
   *
   * @param state of the currently active part of the DAG
   * @param context information
   * @param changedActionId : if notification is triggered by an action state changes, this is the action corresponding.
   */
  def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId : Option[ActionId]): Unit
}
