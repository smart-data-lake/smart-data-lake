package io.smartdatalake.jetty

import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.json4s._
import org.json4s.jackson.Serialization.write

class IsFinished(stateListener: CustomListener) extends HttpServlet{
  implicit val formats = DefaultFormats
case class JsonDummy(isOver: Boolean)


  override protected def doGet(
    request: HttpServletRequest,
    response: HttpServletResponse): Unit = {

    response.setContentType("application/json");
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().println(write(JsonDummy(stateListener.isOver)));
  }
}
