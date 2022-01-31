package io.smartdatalake.jetty

import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext}

import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}
@Path("/v1")
@Produces(Array(MediaType.APPLICATION_JSON))
class RestAPIMethods extends ApiRequestContext {

  @GET
  @Path("state")
  def state: ActionDAGRunState = apiRoot.customListener.stateVar

  @GET
  @Path("context")
  def context: ActionPipelineContext = apiRoot.customListener.contextVar
}
