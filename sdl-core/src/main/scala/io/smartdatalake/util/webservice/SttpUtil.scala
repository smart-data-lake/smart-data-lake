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
package io.smartdatalake.util.webservice

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.connection.authMode.{AuthMode, HttpHeaderAuth}
import sttp.client3.{Empty, HttpClientSyncBackend, Identity, Request, RequestT, Response, SttpBackend, SttpBackendOptions}
import sttp.model.Uri.unsafeParse
import sttp.model.{MediaType, Uri}

import java.io.ByteArrayInputStream
import java.net.URLConnection
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

object SttpUtil extends SmartDataLakeLogger {

  /**
   * Validates if the a provided uri has the scheme/protocol 'http' or 'https'
   */
  def canHandleScheme(uri: String): Boolean = uri.matches("https?:.*")

  def sendRequest[T](request: Request[Either[String, T], Any], context: String, retries: Int = 0)(implicit sttpBackend: SttpBackend[Identity, Any]): T = {
    logger.info(s"${request.method} ${request.uri}")
    val response = try {
      retry(retries) {
        val r = request.send(sttpBackend)
        logger.debug(s"response received: ${request.method} ${request.uri}")
        r
      }
    } catch {
      case ex: Exception =>
        logger.debug(s"request failed: ${request.method} ${request.uri}")
        throw SttpBackendError(ex)
    }
    getContent(response, context)
  }

  def getContent[T](response: Response[Either[String, T]], context: String): T = {
    validateResponse(response, context)
    response.body.toOption.get
  }

  private[smartdatalake] def validateResponse[T](response: Response[Either[String, T]], context: String): Unit = {
    if (response.body.isLeft) {
      throw HttpRequestError(context, response.code.code, response.body.swap.toOption.get)
    }
    assert(response.isSuccess, throw HttpRequestError(context, response.code.code, "StatusCode is not successfull, but there is no error message!"))
  }

  def guessMimeType(content: Array[Byte]): Option[String] = {
    Option(URLConnection.guessContentTypeFromStream(new ByteArrayInputStream(content)))
      .orElse {
        // manually detect type as guessContentTypeFromStream doesnt work for Json and Text...
        val str = new String(content)
        if (str.take(100).matches("(?:\\P{Cntrl}|\\s)+")) { // is text
          if (str.matches("\\s*[{\\[]")) Some(MediaType.ApplicationJson.toString())
          else Some(MediaType.TextPlain.toString())
        } else None
      }
  }

  def parseUrl(url: String): Uri = {
    try {
      unsafeParse(url)
    } catch {
      case e: Exception =>
        logger.error(s"could not parse url $url")
        throw e
    }
  }

  /**
   * Create an Iterator that query paged Webservices.
   * The Iterator queries the initial URL and extract next URL from response until all pages have been queried.
   */
  def getPagedResponseIterator[R](url: String, pagingLinkExtractor: R => Option[String], getResponse: (String, Int) => R): Iterator[R] = {
    new Iterator[R]() {
      var nextLink: Option[String] = Some(url)
      var idx = 0

      override def hasNext: Boolean = nextLink.isDefined

      override def next(): R = {
        assert(nextLink.nonEmpty)
        val response = getResponse(nextLink.get, idx)
        nextLink = pagingLinkExtractor(response)
        idx = idx + 1
        response
      }
    }
  }

  def createDefaultBackendOptions(proxy: Option[HttpProxyConfig], timeouts: Option[HttpTimeoutConfig]): SttpBackendOptions =
    Seq(proxy, timeouts).flatten.foldLeft(SttpBackendOptions.Default) {
      case (options, config) => config.sttpConfig(options)
    }

  def createDefaultBackend(proxy: Option[HttpProxyConfig] = None, timeouts: Option[HttpTimeoutConfig] = None): SttpBackend[Identity, Any] = {
    HttpClientSyncBackend(createDefaultBackendOptions(proxy, timeouts))
  }

  def retry[T](n: Int)(fn: => T): T = {
    try {
      fn
    } catch {
      case e: Exception if n >= 1 =>
        logger.warn(s"Retry for ${e.getClass.getSimpleName}: ${e.getMessage}")
        retry(n - 1)(fn)
    }
  }

  type SttpRequest[R] = RequestT[Empty, Either[String, R], Any]

  /**
   * Extend functionality of the the RequestT class
   */
  implicit class SttpRequestExtension[R](request: SttpRequest[R]) {
    def optionally[A](config: Option[A], func: (A, SttpRequest[R]) => SttpRequest[R]): SttpRequest[R] = {
      if (config.isDefined) func(config.get, request) else request
    }

    def optionalReadTimeout(timeouts: Option[HttpTimeoutConfig]): SttpRequest[R] = {
      request.optionally(timeouts, (c: HttpTimeoutConfig, request: SttpRequest[R]) => request.readTimeout(c.readTimeout))
    }

    def applyAuthMode(authMode: Option[AuthMode]): SttpRequest[R] = {
      request.optionally(authMode, (v: AuthMode, request: SttpRequest[R]) => {
        v match {
          case headerAuth: HttpHeaderAuth => request.headers(headerAuth.getHeaders)
          case x => throw ConfigurationException(s"authentication mode $x is not supported by SttpWebserviceClient")
        }
      })
    }
  }
}

trait SttpConfigModifier {
  def sttpConfig(options: SttpBackendOptions): SttpBackendOptions
}

/**
 * Proxy configuration used to make HTTP-connection.
 *
 * @param host proxy host
 * @param port proxy port
 */
case class HttpProxyConfig(host: String, port: Int, user: Option[StringOrSecret] = None, password: Option[StringOrSecret] = None) extends SttpConfigModifier {
  def sttpConfig(options: SttpBackendOptions): SttpBackendOptions = {
    if (user.nonEmpty && password.nonEmpty) options.httpProxy(host, port, user.get.resolve(), password.get.resolve())
    else options.httpProxy(host, port)
  }
}

case class HttpTimeoutConfig(connectionTimeoutMs: Int, readTimeoutMs: Int) extends SttpConfigModifier {
  def sttpConfig(options: SttpBackendOptions): SttpBackendOptions = {
    options.connectionTimeout(connectionTimeout)
  }

  def connectionTimeout: FiniteDuration = FiniteDuration(readTimeoutMs, TimeUnit.MILLISECONDS)

  def readTimeout: FiniteDuration = FiniteDuration(readTimeoutMs, TimeUnit.MILLISECONDS)
}


case class HttpRequestError(context: String, code: Int, err: String)
  extends Exception(s"'$context' failed: StatusCode=$code Error=$err")

case class SttpBackendError(ex: Exception)
  extends Exception(s"SttpBackend failed with exception ${ex.getClass.getSimpleName}: ${ex.getMessage}")