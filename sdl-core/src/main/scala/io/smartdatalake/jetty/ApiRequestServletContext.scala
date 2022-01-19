package io.smartdatalake.jetty

import org.eclipse.jetty.server.handler.ContextHandler

import javax.servlet.ServletContext

object ApiRequestServletContext {

  private val attribute = getClass.getCanonicalName

  def setApiRoot(contextHandler: ContextHandler, uiRoot: APIRoot): Unit = {
  contextHandler.setAttribute(attribute, uiRoot)
}

  def getApiRoot(context: ServletContext): APIRoot = {
  context.getAttribute(attribute).asInstanceOf[APIRoot]
}
}
