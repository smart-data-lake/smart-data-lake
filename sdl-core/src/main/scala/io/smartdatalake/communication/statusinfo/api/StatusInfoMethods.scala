/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.communication.statusinfo.api

import io.smartdatalake.util.json.SdlJsonUtils
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}
import jakarta.ws.rs.core.{MediaType, Response}
import jakarta.ws.rs.{GET, Path, Produces}
import org.json4s.{Formats, NoTypeHints}


/**
 * Definition of the REST-Api of the Status-Info-Server.
 * Example URL with default config running locally :  http://localhost:4440/api/v1/state
 */
@Path("/v1")
case class StatusInfoMethods(statelistener: SnapshotStatusInfoListener) {

  implicit val formats: Formats = SdlJsonUtils.getFormats(NoTypeHints)

  @GET
  @Path("state")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def state: Response = {
    // reuse our own Json serialization to avoid additional dependency on jersey-media-json-jackson with potential conflicts
    val json = SdlJsonUtils.caseClassToJsonString(getState)
    Response.ok(json).`type`(MediaType.APPLICATION_JSON).build()
  }
  def getState: Option[ActionDAGRunState] = statelistener.stateVar

  @GET
  @Path("context")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def context: Response = {
    // reuse our own Json serialization to avoid additional dependency on jersey-media-json-jackson with potential conflicts
    val json = SdlJsonUtils.caseClassToJsonString(getContext)
    Response.ok(json).`type`(MediaType.APPLICATION_JSON).build()
  }
  def getContext: Option[ActionPipelineContext] = statelistener.contextVar
}
