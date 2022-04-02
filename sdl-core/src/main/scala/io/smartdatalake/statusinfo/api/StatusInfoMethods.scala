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

import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}

import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.{Context, MediaType}
import javax.ws.rs.{GET, Path, Produces}

/**
 * Definition of the REST-Api of the Status-Info-Server.
 * Example URL with default config running locally :  http://localhost:4440/api/v1/state
 */
@Path("/v1")
@Produces(Array(MediaType.APPLICATION_JSON))
class StatusInfoMethods {

  @GET
  @Path("state")
  def state: Option[ActionDAGRunState] = statelistener.stateVar

  @GET
  @Path("context")
  def context: Option[ActionPipelineContext] = statelistener.contextVar

  @Context
  protected var servletContext: ServletContext = _

  @Context
  protected var httpRequest: HttpServletRequest = _

  def statelistener: SnapshotStatusInfoListener = StatusInfoServletContext.getStateListener(servletContext)
}
