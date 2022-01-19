package io.smartdatalake.jetty

import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Context

trait ApiRequestContext {
  @Context
  protected var servletContext: ServletContext = _

  @Context
  protected var httpRequest: HttpServletRequest = _

  def apiRoot: APIRoot = ApiRequestServletContext.getApiRoot(servletContext)

}

