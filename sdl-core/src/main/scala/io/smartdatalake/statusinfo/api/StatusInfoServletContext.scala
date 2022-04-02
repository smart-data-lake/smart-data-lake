/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.statusinfo.api

import org.eclipse.jetty.server.handler.ContextHandler

import javax.servlet.ServletContext

/**
 * Singleton Object that provides the "glue" between :
 * - the Jetty API used on io.smartdatalake.statusinfo.server.StatusInfoMethods
 * - the class holding the information to display, io.smartdatalake.statusinfo.StatusInfoListener
 */
object StatusInfoServletContext {

  private val attribute = getClass.getCanonicalName

  def setStateListener(contextHandler: ContextHandler, statusInfoListener: SnapshotStatusInfoListener): Unit = {
    contextHandler.setAttribute(attribute, statusInfoListener)
  }

  def getStateListener(context: ServletContext): SnapshotStatusInfoListener = {
    context.getAttribute(attribute).asInstanceOf[SnapshotStatusInfoListener]
  }
}
