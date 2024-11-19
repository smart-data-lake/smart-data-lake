/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.webservice

import io.smartdatalake.util.misc.SmartDataLakeLogger
import sttp.client3.{Identity, Request, Response, SttpBackend}

import java.io.ByteArrayInputStream
import java.net.URLConnection
import javax.ws.rs.core.MediaType

object SttpUtil extends SmartDataLakeLogger {

  /**
   * Validates if the a provided uri has the scheme/protocol 'http' or 'https'
   */
  def canHandleScheme(uri: String): Boolean = uri.matches("^https?:")

  def sendRequest[T](request: Request[Either[String, T], Any], context: String)(implicit httpBackend: SttpBackend[Identity, Any]): T = {
    logger.info(s"${request.method} ${request.uri}")
    val response = request.send(httpBackend)
    getContent(response, context)
  }

  def getContent[T](response: Response[Either[String, T]], context: String): T = {
    validateResponse(response, context)
    response.body.right.get
  }

  def validateResponse[T](response: Response[Either[String, T]], context: String): Unit = {
    if (response.body.isLeft) {
      throw HttpRequestError(context, response.code.code, response.body.left.get)
    }
    assert(response.isSuccess, throw HttpRequestError(context, response.code.code, "StatusCode is not successfull, but there is no error message!"))
  }

  def guessMimeType(content: Array[Byte]): Option[String] = {
    Option(URLConnection.guessContentTypeFromStream(new ByteArrayInputStream(content)))
      .orElse {
        // manually detect type as guessContentTypeFromStream doesnt work for Json and Text...
        val str = new String(content)
        if (str.take(100).matches("(?:\\P{Cntrl}|\\p{Space})+")) { // is text
          if (str.matches("\\s*[{\\[]")) Some(MediaType.APPLICATION_JSON)
          else Some(MediaType.TEXT_PLAIN)
        } else None
      }
  }

  /**
   * Create an Iterator that query pages Webservices.
   * The Iterator queries the initial URL and extract next URL from response until all pages have been queried.
   *
   * Note: this only works with Response of type String
   */
  def getPagedResponseIterator(url: String, pagingLinkRegex: String, getResponse: String => String): Iterator[String] = {
    val pagingLinkPattern = pagingLinkRegex.r.unanchored
    new Iterator[String]() {
      var nextLink: Option[String] = Some(url)

      override def hasNext: Boolean = nextLink.isDefined

      override def next(): String = {
        assert(nextLink.nonEmpty)
        val response = getResponse(nextLink.get)
        nextLink = {
          response match {
            case pagingLinkPattern(link) =>
              logger.debug(s"next pagingLink found: $link")
              Some(link)
            case _ => None
          }
        }
        response
      }
    }
  }
}


case class HttpRequestError(context: String, code: Int, err: String) extends Exception(s"'$context' failed: StatusCode=$code Error=$err")