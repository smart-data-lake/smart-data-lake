package io.smartdatalake.statusinfo

import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}

import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}

@Path("/v1")
@Produces(Array(MediaType.APPLICATION_JSON))
class StatusInfoMethods extends StatusInfoRequestContext {

  @GET
  @Path("state")
  def state: ActionDAGRunState = apiRoot.statusInfoListener.stateVar

  @GET
  @Path("context")
  def context: ActionPipelineContext = apiRoot.statusInfoListener.contextVar
}
