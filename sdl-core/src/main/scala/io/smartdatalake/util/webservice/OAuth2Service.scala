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
import io.smartdatalake.util.webservice.OAuth2Service.sendRequest
import io.smartdatalake.util.webservice.SttpUtil.getContent
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import sttp.client3.{HttpClientSyncBackend, Identity, Request, SttpBackend, basicRequest}
import sttp.model.Header.unapply
import sttp.model.{Header, MediaType, Uri}

/**
 * OAuth2 service handles refreshing OAuth2 tokens when they are expired.
 *
 * @param tokenUrl     URL for token handling
 * @param clientId     optional application client id to add to refresh request
 * @param tokenInitFun function to create initial OAuth2 token
 */
case class OAuth2Service(tokenUrl: String, clientId: Option[String], tokenInitFun: () => OAuth2Response) extends SmartDataLakeLogger {

  private var currentToken: Option[OAuth2Response] = None

  def getToken: OAuth2Response = {
    if (currentToken.isEmpty) currentToken = Some(tokenInitFun())
    else if (currentToken.get.isExpired) {
      currentToken = currentToken.map(t => if (t.refresh_token.isDefined) doRefreshToken(t) else tokenInitFun())
    }
    currentToken.get
  }

  /**
   * Refresh given OAuth2 token against a new one.
   */
  private def doRefreshToken(response: OAuth2Response): OAuth2Response = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    logger.info(s"Refresh token using $tokenUrl")
    val request = basicRequest
      .post(Uri.unsafeParse(tokenUrl))
      .header(Header.contentType(MediaType.ApplicationXWwwFormUrlencoded))
      .header(Header.accept(MediaType.ApplicationJson))
      .followRedirects(true)
      .body(Map(
        "grant_type" -> "refresh_token",
        "refresh_token" -> response.refresh_token.getOrElse(throw new IllegalStateException("refresh_token not defined!"))
      ) ++ clientId.map("client_id" -> _))
    parse(sendRequest(request, "refresh token")).extract[OAuth2Response]
  }
}

object OAuth2Service extends SmartDataLakeLogger {
  @transient private lazy val httpBackend: SttpBackend[Identity, Any] = HttpClientSyncBackend()

  def sendRequest(request: Request[Either[String, String], Any], context: String): String = {
    val response = request.send(httpBackend)
    getContent(response, context)
  }
}

/**
 * OAuth2 token endpoint response, see https://www.oauth.com/oauth2-servers/access-tokens/access-token-response
 * Additional id_token possible according to OpenID connect, see also https://auth0.com/blog/id-token-access-token-what-is-the-difference
 */
case class OAuth2Response(access_token: String, refresh_token: Option[String], expires_in: Int, token_type: String, scope: Option[String] = None, id_token: Option[String]) {
  private val expirationTsMillis = System.currentTimeMillis() + (expires_in - 1) * 1000 // 1s tolerance

  def isExpired: Boolean = System.currentTimeMillis() > expirationTsMillis

  def getAuthHeader(useIdToken: Boolean): (String, String) = {
    val tokenToUse = if (useIdToken) this.id_token.getOrElse(throw new IllegalStateException("id_token not defined!"))
    else this.access_token
    unapply(Header.authorization(token_type, tokenToUse)).get
  }
}
