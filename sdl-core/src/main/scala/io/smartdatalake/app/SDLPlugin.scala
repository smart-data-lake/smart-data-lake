/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.util.secrets.StringOrSecret
import org.apache.spark.annotation.DeveloperApi

/**
 * SDL Plugin defines an interface to execute custom code on SDL startup and shutdown.
 * Configure it by setting a java system property "sdl.pluginClassName" to a class name implementing SDLPlugin interface.
 * The class needs to have a constructor without any parameters.
 */
@DeveloperApi
trait SDLPlugin {

  /**
   * Startup is called from SDL as early as possible (before config reading).
   * Use cases are dynamic log configuration or setup of credentials
   */
  def startup(): Unit = ()

  /**
   * Configure is called from SDL when GlobalConfig is parsed passing GlobalConfig.pluginOptions as parameter.
   */
  def configure(options: Map[String,StringOrSecret]): Unit = ()

  /**
   * Shutdown is called from SDL as late as possible on ordinary exit of an SDL run.
   */
  def shutdown(): Unit = ()

}
