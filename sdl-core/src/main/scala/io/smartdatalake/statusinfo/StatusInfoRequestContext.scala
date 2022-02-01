package io.smartdatalake.statusinfo

import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Context

trait StatusInfoRequestContext {
  @Context
  protected var servletContext: ServletContext = _

  @Context
  protected var httpRequest: HttpServletRequest = _

  def apiRoot: StatusInfoProvider = StatusInfoServletContext.getStatusInfoProvider(servletContext)

}

