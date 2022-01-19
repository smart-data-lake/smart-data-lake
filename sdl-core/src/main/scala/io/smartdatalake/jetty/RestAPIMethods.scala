package io.smartdatalake.jetty

import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}
@Path("/v1")
@Produces(Array(MediaType.APPLICATION_JSON))
class RestAPIMethods extends ApiRequestContext {
  @GET
  @Path("isOver")
  def isOver: IsFinished = {
    IsFinished(apiRoot.customListener.isOver)
  }
}
