package io.smartdatalake.statusinfo

import org.eclipse.jetty.server.handler.ContextHandler

import javax.servlet.ServletContext

object StatusInfoServletContext {

  private val attribute = getClass.getCanonicalName

  def setStatusInfoProvider(contextHandler: ContextHandler, provider: StatusInfoProvider): Unit = {
    contextHandler.setAttribute(attribute, provider)
  }

  def getStatusInfoProvider(context: ServletContext): StatusInfoProvider = {
    context.getAttribute(attribute).asInstanceOf[StatusInfoProvider]
  }
}
